# Hudi Upgrade Tests

End-to-end tests for upgrading Apache Hudi tables: create tables with an older Hudi version, run baseline query tests, upgrade the table, then run post-upgrade tests. Results are written to CSV for comparison.

## Prerequisites

- Spark 3.1, 3.2, 3.3, 3.4, and 3.5 installed; set `SPARK31_HOME`, `SPARK32_HOME`, `SPARK33_HOME`, `SPARK34_HOME`, `SPARK35_HOME` (or equivalent) to each Spark installation directory.
- Python 3 with PySpark (used by the test scripts when invoked via `spark-submit`).

## Usage

**Syntax:** `bash hudi-upgrade-test.sh <TABLE_TYPE> [IS_CDC]`

- **TABLE_TYPE:** `COPY_ON_WRITE` or `MERGE_ON_READ`
- **IS_CDC:** `true` or `false` (default: `false`) — when `true`, creates and tests CDC-enabled tables.

### COPY_ON_WRITE

```sh
# Non-CDC COW table
bash hudi-upgrade-test.sh COPY_ON_WRITE
```

```sh
# CDC COW table
bash hudi-upgrade-test.sh COPY_ON_WRITE true
```

### MERGE_ON_READ

```sh
# Non-CDC MOR table
bash hudi-upgrade-test.sh MERGE_ON_READ
```

```sh
# CDC MOR table
bash hudi-upgrade-test.sh MERGE_ON_READ true
```

## What the test does

1. **Generate table** — Creates a Hudi table with the source Hudi version (e.g. 0.12.3).
2. **Baseline tests** — Runs snapshot, incremental, timetravel (and read_optimized for MOR, cdc for CDC tables) across multiple Spark/Hudi version combinations; results are tagged `baseline`.
3. **Upgrade** — Upgrades the table to the target Hudi version (e.g. 0.15.0) and may write additional data.
4. **Post-upgrade tests** — Re-runs the same query types; results are tagged `upgrade`.

Results are written under **`results/`** as CSV files per table type, e.g.:

- `hudi_trips_cow_table_baseline.csv` / `hudi_trips_cow_table_upgrade.csv`
- `hudi_trips_mor_table_baseline.csv` / `hudi_trips_mor_table_upgrade.csv`
- `cdc_hudi_trips_cow_table_baseline.csv` / `cdc_hudi_trips_cow_table_upgrade.csv`
- `cdc_hudi_trips_mor_table_baseline.csv` / `cdc_hudi_trips_mor_table_upgrade.csv`

## Comparing baseline vs upgrade

Run the insights script to compare baseline and upgrade results for each table type:

```sh
python3 generate_insights.py
```

It reads all `*_baseline.csv` and `*_upgrade.csv` files in `results/`, prints pass/fail and row-count comparison, and applies:

- **Snapshot, incremental, read_optimized (and cdc where applicable):** Upgrade has no issues when the difference vs baseline is **+5** (extra rows from post-upgrade writes).
- **Timetravel:** Count is expected to be **20** for both baseline and upgrade.

## Scripts

| Script | Purpose |
|--------|--------|
| `hudi-upgrade-test.sh` | Orchestrates: table generation → baseline tests → upgrade → post-upgrade tests. |
| `hudi_upgrade_data_generator.py` | Creates/upgrades Hudi tables (init or upgrade mode). |
| `hudi_upgrade_test_queries.py` | Runs snapshot, incremental, timetravel, read_optimized, cdc queries and appends CSV results. |
| `generate_insights.py` | Compares baseline vs upgrade CSVs and prints per-type insights and verdicts. |
