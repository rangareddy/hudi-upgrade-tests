#!/usr/bin/env python3
"""
Generate comparison insights between baseline and upgrade Hudi test results.
Reads all CSVs from the results directory and produces a per-type comparison report.
"""

import csv
import os
from pathlib import Path
from collections import defaultdict

RESULTS_DIR = Path(__file__).parent / "results"


def load_csv(path: Path) -> list[dict]:
    """Load a CSV file and return list of row dicts."""
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def summarize_rows(rows: list[dict]) -> dict:
    """Summarize rows: by spark_version, query_type, status, and counts."""
    by_key = defaultdict(list)
    for r in rows:
        key = (r.get("spark_version", ""), r.get("query_type", ""))
        by_key[key].append(r)
    return dict(by_key)


def compare_counts(baseline_rows: list[dict], upgrade_rows: list[dict]) -> list[tuple]:
    """Compare counts for matching (spark_version, query_type) where both SUCCESS."""
    baseline_ok = {(r["spark_version"], r["query_type"]): int(r.get("count") or 0)
                   for r in baseline_rows if r.get("status") == "SUCCESS"}
    upgrade_ok = {(r["spark_version"], r["query_type"]): int(r.get("count") or 0)
                  for r in upgrade_rows if r.get("status") == "SUCCESS"}
    keys = sorted(set(baseline_ok) | set(upgrade_ok))
    out = []
    for k in keys:
        b = baseline_ok.get(k, None)
        u = upgrade_ok.get(k, None)
        out.append((k[0], k[1], b, u))
    return out


def status_summary(rows: list[dict]) -> dict:
    """Return success/fail counts and list of failed (spark_version, query_type)."""
    success = sum(1 for r in rows if r.get("status") == "SUCCESS")
    failed = [r for r in rows if r.get("status") == "FAILED"]
    errors = {(r["spark_version"], r["query_type"]): r.get("error_message", "") for r in failed}
    return {"total": len(rows), "success": success, "failed": len(failed), "errors": errors}


def find_result_pairs() -> list[tuple[str, Path, Path]]:
    """Find (label, baseline_path, upgrade_path) from results dir."""
    pairs = []
    for f in sorted(RESULTS_DIR.glob("*.csv")):
        name = f.stem
        if name.endswith("_baseline"):
            base = name[: -len("_baseline")]
            upgrade_path = RESULTS_DIR / f"{base}_upgrade.csv"
            if upgrade_path.exists():
                label = base
                if "cdc_hudi" in base:
                    label = "CDC " + base.replace("cdc_", "").replace("_table", " table")
                else:
                    label = base.replace("_table", " table")
                pairs.append((label, f, upgrade_path))
    return pairs


def main():
    print("=" * 80)
    print("HUDI UPGRADE TEST — Baseline vs Upgrade Insights")
    print("=" * 80)

    pairs = find_result_pairs()
    if not pairs:
        print("No baseline/upgrade CSV pairs found in", RESULTS_DIR)
        return

    for label, baseline_path, upgrade_path in pairs:
        baseline_rows = load_csv(baseline_path)
        upgrade_rows = load_csv(upgrade_path)

        print()
        print("-" * 80)
        print(f"  {label}")
        print("-" * 80)

        b_sum = status_summary(baseline_rows)
        u_sum = status_summary(upgrade_rows)

        # Comparison for ALL spark versions: Spark, Query type, Upgrade Status, Any message
        upgrade_by_key = {(r["spark_version"], r["query_type"]): r for r in upgrade_rows}
        all_keys = sorted(upgrade_by_key.keys())
        if all_keys:
            print("\n  Upgrade comparison (all Spark versions):")
            print(f"    {'Spark':<10} {'Query type':<16} {'Upgrade Status':<14} Any message")
            print("    " + "-" * 90)
            for spark_ver, query_type in all_keys:
                row = upgrade_by_key[(spark_ver, query_type)]
                status = row.get("status", "—")
                msg = (row.get("error_message") or "").strip() if status == "FAILED" else ""
                # Truncate long messages for display
                msg_display = (msg[:70] + "…") if len(msg) > 70 else msg
                print(f"    {spark_ver:<10} {query_type:<16} {status:<14} {msg_display}")

        print("\n  Status summary:")
        print(f"    Baseline:  {b_sum['success']}/{b_sum['total']} passed, {b_sum['failed']} failed")
        print(f"    Upgrade:   {u_sum['success']}/{u_sum['total']} passed, {u_sum['failed']} failed")

        # Count comparison where both succeeded
        # Rule: +5 for snapshot/incremental/read_optimized => upgrade has no issues.
        #       timetravel always expected to show 20 (both baseline and upgrade).
        count_comparisons = compare_counts(baseline_rows, upgrade_rows)
        if count_comparisons:
            print("\n  Count comparison (where status=SUCCESS):")
            print("    Upgrade has no issues when: snapshot/incremental/read_optimized show +5; timetravel shows 20.")
            print(f"    {'Spark':<10} {'Query type':<16} {'Baseline':<10} {'Upgrade':<10} {'Diff':<8}  Verdict")
            print("    " + "-" * 72)
            for spark_ver, query_type, b_count, u_count in count_comparisons:
                b_str = str(b_count) if b_count is not None else "—"
                u_str = str(u_count) if u_count is not None else "—"
                if b_count is not None and u_count is not None:
                    diff = u_count - b_count
                    diff_str = f"+{diff}" if diff > 0 else str(diff)
                    # Verdict: +5 for snapshot/incremental/read_optimized => no issues; timetravel 20 => no issues
                    if query_type == "timetravel":
                        verdict = "No issues (timetravel=20)" if u_count == 20 else "Check"
                    elif query_type in ("snapshot", "incremental", "read_optimized", "cdc"):
                        verdict = "No issues (+5)" if diff == 5 else "Check"
                    else:
                        verdict = "—"
                else:
                    diff_str = "—"
                    verdict = "—"
                print(f"    {spark_ver:<10} {query_type:<16} {b_str:<10} {u_str:<10} {diff_str:<8}  {verdict}")

        # Failures in upgrade that were success in baseline (regressions)
        b_ok_keys = {(r["spark_version"], r["query_type"]) for r in baseline_rows if r.get("status") == "SUCCESS"}
        u_fail_keys = {(r["spark_version"], r["query_type"]) for r in upgrade_rows if r.get("status") == "FAILED"}
        regressions = b_ok_keys & u_fail_keys
        if regressions:
            print("\n  Regressions (passed in baseline, failed in upgrade):")
            for (spark_ver, query_type) in sorted(regressions):
                err = u_sum["errors"].get((spark_ver, query_type), "")
                err_short = (err[:60] + "…") if len(err) > 60 else err
                print(f"    {spark_ver} / {query_type}: {err_short}")

        # Improvements: failed in baseline, passed in upgrade
        b_fail_keys = {(r["spark_version"], r["query_type"]) for r in baseline_rows if r.get("status") == "FAILED"}
        u_ok_keys = {(r["spark_version"], r["query_type"]) for r in upgrade_rows if r.get("status") == "SUCCESS"}
        improvements = b_fail_keys & u_ok_keys
        if improvements:
            print("\n  Improvements (failed in baseline, passed in upgrade):")
            for (spark_ver, query_type) in sorted(improvements):
                print(f"    {spark_ver} / {query_type}")

        # Version / table version note
        if baseline_rows:
            r = baseline_rows[0]
            print(f"\n  Baseline table version: {r.get('table_version', '—')}")
        if upgrade_rows:
            r = upgrade_rows[0]
            print(f"  Upgrade table version:  {r.get('table_version', '—')}")

    # Key insights (cross-cutting)
    print()
    print("-" * 80)
    print("  KEY INSIGHTS (summary)")
    print("-" * 80)
    print("""
  • Table version: Baseline uses table_version=5, upgrade uses 6 (post-upgrade).
  • Spark 3.2.4 & 3.3.3: All query types FAIL for both baseline and upgrade with
    IllegalArgumentException ("For input string: \\"null\\"") or NullPointerException.
  • Spark 3.1.3, 3.4.1, 3.5.3: Non-CDC query types pass; CDC query type fails on
    3.4.1 and 3.5.3 with "It isn't a CDC hudi table" (expected if table not created
    with CDC in that run).
  • Upgrade has no issues when:
    - snapshot, incremental, read_optimized (and cdc where applicable): Baseline vs Upgrade
      difference is +5 (upgrade has 5 more rows from post-upgrade writes).
    - timetravel: count is always 20 for both baseline and upgrade (expected).
""")
    print("=" * 80)


if __name__ == "__main__":
    main()
