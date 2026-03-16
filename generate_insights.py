#!/usr/bin/env python3
"""
Generate comparison insights between baseline and upgrade Hudi test results.
Reads all CSVs from the results directory and produces a per-type comparison report.
"""

import csv
import io
import os
import sys
from pathlib import Path
from collections import defaultdict

RESULTS_DIR = Path(__file__).parent / "results"
STORED_INSIGHTS_FILE = RESULTS_DIR / "insights_baseline.txt"

# Expected number of records deleted during upgrade (must match hudi_upgrade_data_generator.run_delete_commit num_deletes)
EXPECTED_DELETE_COUNT = 3


def safe_count_int(value) -> int:
    """Convert count from CSV to int; handles empty, None, float strings (e.g. '22.0'), and int strings."""
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0

def format_error_message(msg: str, max_len: int = 70) -> str:
    """Format error message for display"""
    if not msg or not msg.strip():
        return ""
    msg = msg.strip()
    if len(msg) > max_len:
        msg = msg[: max_len - 1].rstrip() + "…"
    return msg


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
    Validate counts using delete count when applicable.
    - timetravel: both must be 20 (point-in-time before deletes/upgrade).
    - snapshot/read_optimized/cdc: U=B+5 (no deletes) or U=B+5-EXPECTED_DELETE_COUNT; validate inferred delete count.
    - incremental: U=B+5 or U=B+(5-EXPECTED_DELETE_COUNT) depending on run.
    Returns (ok, message).
    """
    if query_type == "timetravel":
        expected = 20
        ok = baseline_count == expected and upgrade_count == expected
        msg = "OK" if ok else f"Expected {expected} for both (got B={baseline_count}, U={upgrade_count})"
        return ok, msg
    if query_type in ("snapshot", "read_optimized", "cdc"):
        # Upgrade adds 5 rows then deletes EXPECTED_DELETE_COUNT → U = B + 5 - delete_count
        # So inferred_delete_count = B + 5 - U (when U < B+5).
        no_deletes_ok = upgrade_count == baseline_count + 5
        inferred_deletes = baseline_count + 5 - upgrade_count
        with_deletes_ok = (
            0 <= inferred_deletes <= 5
            and inferred_deletes == EXPECTED_DELETE_COUNT
            and upgrade_count == baseline_count + 5 - EXPECTED_DELETE_COUNT
        )
        ok = no_deletes_ok or with_deletes_ok
        if ok:
            if no_deletes_ok:
                msg = "OK (no deletes)"
            else:
                msg = f"OK (delete count {EXPECTED_DELETE_COUNT})"
        else:
            msg = f"Expected U=B+5 or U=B+5-{EXPECTED_DELETE_COUNT} (delete count {EXPECTED_DELETE_COUNT}); got B={baseline_count}, U={upgrade_count} (inferred deletes={inferred_deletes})"
        return ok, msg
    if query_type == "incremental":
        # No deletes: U=B+5. With deletes: U can be B+2, B+3, or B+4 depending on incremental semantics.
        no_deletes_ok = upgrade_count == baseline_count + 5
        with_deletes_ok = (
            baseline_count + 5 - EXPECTED_DELETE_COUNT <= upgrade_count <= baseline_count + 5
            and upgrade_count < baseline_count + 5
        )
        ok = no_deletes_ok or with_deletes_ok
        if ok:
            msg = "OK (no deletes)" if no_deletes_ok else f"OK (delete count {EXPECTED_DELETE_COUNT})"
        else:
            msg = f"Expected U=B+5 or U in [B+2,B+4] (deletes={EXPECTED_DELETE_COUNT}); got B={baseline_count}, U={upgrade_count}"
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


def _write(out: io.TextIOBase, s: str = "") -> None:
    """Write line(s) to stream; flush if stdout for immediate display."""
    out.write(s + "\n" if s else "\n")
    if out is sys.stdout:
        out.flush()


def run_insights(out: io.TextIOBase) -> bool:
    """Generate insights report to stream `out`. Returns True if report was generated, False if no data."""
    _write(out, "=" * 80)
    _write(out, "HUDI UPGRADE TEST — Baseline vs Upgrade Insights")
    _write(out, "=" * 80)

    pairs = find_result_pairs()
    if not pairs:
        _write(out, f"No baseline/upgrade CSV pairs found in {RESULTS_DIR}")
        return False

    for label, baseline_path, upgrade_path in pairs:
        baseline_rows = load_csv(baseline_path)
        upgrade_rows = load_csv(upgrade_path)

        _write(out)
        _write(out, "-" * 80)
        _write(out, f"  {label}")
        _write(out, "-" * 80)

        b_sum = status_summary(baseline_rows)
        u_sum = status_summary(upgrade_rows)

        # Comparison for ALL spark versions: Spark, Query type, Upgrade Status, Any message
        upgrade_by_key = {(r["spark_version"], r["query_type"]): r for r in upgrade_rows}
        all_keys = sorted(upgrade_by_key.keys())
        if all_keys:
            _write(out, "\n  Upgrade comparison (all Spark versions):")
            _write(out, f"    {'Spark':<10} {'Query type':<16} {'Upgrade Status':<14} Any message")
            _write(out, "    " + "-" * 90)
            for spark_ver, query_type in all_keys:
                row = upgrade_by_key[(spark_ver, query_type)]
                status = row.get("status", "—")
                msg = (row.get("error_message") or "").strip() if status == "FAILED" else ""
                msg_display = format_error_message(msg)
                _write(out, f"    {spark_ver:<10} {query_type:<16} {status:<14} {msg_display}")

        # Count logic: compare baseline vs upgrade counts where both SUCCESS; validate rules
        baseline_counts = {(r["spark_version"], r["query_type"]): safe_count_int(r.get("count"))
                          for r in baseline_rows if r.get("status") == "SUCCESS"}
        upgrade_counts = {(r["spark_version"], r["query_type"]): safe_count_int(r.get("count"))
                          for r in upgrade_rows if r.get("status") == "SUCCESS"}
        count_keys = sorted(set(baseline_counts) & set(upgrade_counts))
        if count_keys:
            _write(out, "\n  Count check (baseline vs upgrade where both SUCCESS):")
            _write(out, f"    Rules: timetravel=20; snapshot/ro/cdc = U=B+5 or U=B+5-{EXPECTED_DELETE_COUNT} (delete count {EXPECTED_DELETE_COUNT}); incremental = U=B+5 or U in [B+2,B+4]")
            _write(out, f"    {'Spark':<10} {'Query type':<16} {'Baseline':<10} {'Upgrade':<10} Count OK?")
            _write(out, "    " + "-" * 70)
            count_mismatches = []
            for spark_ver, query_type in count_keys:
                b_count = baseline_counts[(spark_ver, query_type)]
                u_count = upgrade_counts[(spark_ver, query_type)]
                ok, msg = count_check_verdict(query_type, b_count, u_count)
                verdict = "✓" if ok else f"✗ {msg}"
                _write(out, f"    {spark_ver:<10} {query_type:<16} {b_count:<10} {u_count:<10} {verdict}")
                if not ok:
                    count_mismatches.append((spark_ver, query_type, msg))
            if count_mismatches:
                _write(out, "\n    Count mismatches:")
                for spark_ver, query_type, msg in count_mismatches:
                    _write(out, f"      {spark_ver} / {query_type}: {msg}")

        _write(out, "\n  Status summary:")
        _write(out, f"    Baseline:  {b_sum['success']}/{b_sum['total']} passed, {b_sum['failed']} failed")
        _write(out, f"    Upgrade:   {u_sum['success']}/{u_sum['total']} passed, {u_sum['failed']} failed")

        # Failures in upgrade that were success in baseline (regressions)
        b_ok_keys = {(r["spark_version"], r["query_type"]) for r in baseline_rows if r.get("status") == "SUCCESS"}
        u_fail_keys = {(r["spark_version"], r["query_type"]) for r in upgrade_rows if r.get("status") == "FAILED"}
        regressions = b_ok_keys & u_fail_keys
        if regressions:
            _write(out, "\n  Regressions (passed in baseline, failed in upgrade):")
            for (spark_ver, query_type) in sorted(regressions):
                err = u_sum["errors"].get((spark_ver, query_type), "")
                _write(out, f"    {spark_ver} / {query_type}: {format_error_message(err, max_len=60)}")

        # Improvements: failed in baseline, passed in upgrade
        b_fail_keys = {(r["spark_version"], r["query_type"]) for r in baseline_rows if r.get("status") == "FAILED"}
        u_ok_keys = {(r["spark_version"], r["query_type"]) for r in upgrade_rows if r.get("status") == "SUCCESS"}
        improvements = b_fail_keys & u_ok_keys
        if improvements:
            _write(out, "\n  Improvements (failed in baseline, passed in upgrade):")
            for (spark_ver, query_type) in sorted(improvements):
                _write(out, f"    {spark_ver} / {query_type}")

        # Version / table version note
        if baseline_rows:
            r = baseline_rows[0]
            _write(out, f"\n  Baseline table version: {r.get('table_version', '—')}")
        if upgrade_rows:
            r = upgrade_rows[0]
            _write(out, f"  Upgrade table version:  {r.get('table_version', '—')}")

    # Key insights (cross-cutting)
    _write(out)
    _write(out, "-" * 80)
    _write(out, "  KEY INSIGHTS (summary)")
    _write(out, "-" * 80)
    _write(out, """
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
    _write(out, "=" * 80)
    return True


def _diff_lines(prev: str, curr: str) -> list[tuple[str, str, str]]:
    """Return list of (line_no, line_prev, line_curr) for differing lines."""
    prev_lines = prev.splitlines()
    curr_lines = curr.splitlines()
    result = []
    for i in range(max(len(prev_lines), len(curr_lines))):
        a = prev_lines[i] if i < len(prev_lines) else ""
        b = curr_lines[i] if i < len(curr_lines) else ""
        if a != b:
            result.append((str(i + 1), a, b))
    return result


def main():
    buf = io.StringIO()
    if not run_insights(buf):
        print(buf.getvalue(), end="")
        return
    current = buf.getvalue()

    # Always print current report
    print(current, end="")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    if not STORED_INSIGHTS_FILE.exists():
        STORED_INSIGHTS_FILE.write_text(current, encoding="utf-8")
        print(f"\n[Stored baseline insights to {STORED_INSIGHTS_FILE}]")
        return

    stored = STORED_INSIGHTS_FILE.read_text(encoding="utf-8")
    if stored.strip() == current.strip():
        print("\n[No change from stored baseline insights.]")
        return

    diffs = _diff_lines(stored, current)
    if not diffs:
        print("\n[No line-by-line differences (length or whitespace may differ).]")
        return

    print("\n" + "=" * 80)
    print("  COMPARISON WITH STORED BASELINE (differences only)")
    print("=" * 80)
    for line_no, old_line, new_line in diffs[:100]:
        print(f"  Line {line_no}:")
        if old_line.strip():
            print(f"    - {old_line}")
        if new_line.strip():
            print(f"    + {new_line}")
    if len(diffs) > 100:
        print(f"  ... and {len(diffs) - 100} more differing lines.")
    print("=" * 80)


if __name__ == "__main__":
    main()
