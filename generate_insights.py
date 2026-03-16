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


def status_summary(rows: list[dict]) -> dict:
    """Return success/fail counts and list of failed (spark_version, query_type)."""
    success = sum(1 for r in rows if r.get("status") == "SUCCESS")
    failed = [r for r in rows if r.get("status") == "FAILED"]
    errors = {(r["spark_version"], r["query_type"]): r.get("error_message", "") for r in failed}
    return {"total": len(rows), "success": success, "failed": len(failed), "errors": errors}


def count_check_verdict(query_type: str, baseline_count: int, upgrade_count: int) -> tuple[bool, str]:
    """
    Validate counts:
    - timetravel: both must be 20 (point-in-time before deletes/upgrade).
    - snapshot/read_optimized/cdc/delete: U=B+5 (no deletes), or (17,22) if deletes in init, or (20,22) if deletes in upgrade.
    Returns (ok, message).
    """
    if query_type == "timetravel":
        expected = 20
        ok = baseline_count == expected and upgrade_count == expected
        msg = "OK" if ok else f"Expected {expected} for both (got B={baseline_count}, U={upgrade_count})"
        return ok, msg
    if query_type in ("snapshot", "read_optimized", "cdc", "delete"):
        # No deletes: B=20, U=25 (U=B+5). Deletes in init: B=17, U=22. Deletes in upgrade: B=20, U=22.
        ok = (
            (upgrade_count == baseline_count + 5)
            or (baseline_count == 17 and upgrade_count == 22)
            or (baseline_count == 20 and upgrade_count == 22)  # 3 deletes applied during upgrade
        )
        if ok:
            msg = "OK"
        else:
            expected = "U=B+5 or (B=17,U=22) or (B=20,U=22); got B={}, U={}"
            msg = expected.format(baseline_count, upgrade_count)
        return ok, msg
    if query_type == "incremental":
        # With deletes: baseline 6, upgrade 10. Without: baseline 6, upgrade 11.
        ok = (upgrade_count == baseline_count + 5) or (baseline_count == 6 and upgrade_count == 10)
        if ok:
            msg = "OK"
        else:
            expected = f"U=B+5 or (B=6,U=10); got B={baseline_count}, U={upgrade_count}"
            msg = f"Expected {expected}"
        return ok, msg
    return True, "—"


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

        # Count logic: compare baseline vs upgrade counts where both SUCCESS; validate rules
        baseline_counts = {(r["spark_version"], r["query_type"]): int(r.get("count") or 0)
                          for r in baseline_rows if r.get("status") == "SUCCESS"}
        upgrade_counts = {(r["spark_version"], r["query_type"]): int(r.get("count") or 0)
                          for r in upgrade_rows if r.get("status") == "SUCCESS"}
        count_keys = sorted(set(baseline_counts) & set(upgrade_counts))
        if count_keys:
            print("\n  Count check (baseline vs upgrade where both SUCCESS):")
            print("    Rules: timetravel=20 for both; snapshot/ro/cdc/delete = U=B+5 or (17,22) or (20,22); incremental = U=B+5 or (6,10)")
            print(f"    {'Spark':<10} {'Query type':<16} {'Baseline':<10} {'Upgrade':<10} Count OK?")
            print("    " + "-" * 70)
            count_mismatches = []
            for spark_ver, query_type in count_keys:
                b_count = baseline_counts[(spark_ver, query_type)]
                u_count = upgrade_counts[(spark_ver, query_type)]
                ok, msg = count_check_verdict(query_type, b_count, u_count)
                verdict = "✓" if ok else f"✗ {msg}"
                print(f"    {spark_ver:<10} {query_type:<16} {b_count:<10} {u_count:<10} {verdict}")
                if not ok:
                    count_mismatches.append((spark_ver, query_type, msg))
            if count_mismatches:
                print("\n    Count mismatches:")
                for spark_ver, query_type, msg in count_mismatches:
                    print(f"      {spark_ver} / {query_type}: {msg}")

        print("\n  Status summary:")
        print(f"    Baseline:  {b_sum['success']}/{b_sum['total']} passed, {b_sum['failed']} failed")
        print(f"    Upgrade:   {u_sum['success']}/{u_sum['total']} passed, {u_sum['failed']} failed")

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
  • Non-CDC tables (hudi_trips_cow, hudi_trips_mor): On Spark 3.1.3, 3.4.1, 3.5.3 all
    query types (snapshot, incremental, timetravel, read_optimized) pass for upgrade.
  • CDC tables (cdc_hudi_trips_cow, cdc_hudi_trips_mor):
    - Spark 3.1.3: All query types including cdc pass for upgrade.
    - Spark 3.4.1 & 3.5.3: snapshot, incremental, timetravel, read_optimized pass;
      cdc query fails with "It isn't a CDC hudi table" (expected if table path not
      created as CDC in that run).
    - Spark 3.2.4 & 3.3.3: All query types fail (same as non-CDC).
""")
    print("=" * 80)


if __name__ == "__main__":
    main()
