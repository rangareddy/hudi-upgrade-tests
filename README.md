# Hudi Upgrade Tests

End-to-end tests for upgrading Apache Hudi tables: create tables with an older Hudi version, run baseline query tests, upgrade the table, then run post-upgrade tests. Results are written to CSV for comparison.

## Prerequisites

- Spark 3.1, 3.2, 3.3, 3.4, and 3.5 installed; set `SPARK31_HOME`, `SPARK32_HOME`, `SPARK33_HOME`, `SPARK34_HOME`, `SPARK35_HOME` (or equivalent) to each Spark installation directory.
- Python 3 with PySpark (used by the test scripts when invoked via `spark-submit`).

## Configuration (versions)

All scripts use the same configuration file **`hudi-upgrade.properties`** for Hudi and Scala versions:

| Property | Description | Default |
|----------|-------------|---------|
| `SOURCE_HUDI_VERSION` | Hudi version used to create the table and run baseline tests | `0.12.3` |
| `TARGET_HUDI_VERSION` | Hudi version to upgrade to and run post-upgrade tests | `0.15.0` |
| `SCALA_VERSION` | Scala version for Spark/Hudi bundles (e.g. `2.12`) | `2.12` |

**To change versions:** edit `hudi-upgrade.properties` in the project root. Both `hudi-upgrade-test.sh` and `hudi-upgrade-test-all.sh` load this file; no need to change any other file. If the file is missing, the scripts fall back to the defaults above.

## Usage

### Run a single test (one table type + CDC flag)

**Syntax:** `bash hudi-upgrade-test.sh <TABLE_TYPE> [IS_CDC]`

- **TABLE_TYPE:** `COPY_ON_WRITE` or `MERGE_ON_READ`
- **IS_CDC:** `true` or `false` (default: `false`) — when `true`, creates and tests CDC-enabled tables.

#### COPY_ON_WRITE

```sh
# Non-CDC COW table
bash hudi-upgrade-test.sh COPY_ON_WRITE
```

```sh
# CDC COW table
bash hudi-upgrade-test.sh COPY_ON_WRITE true
```

#### MERGE_ON_READ

```sh
# Non-CDC MOR table
bash hudi-upgrade-test.sh MERGE_ON_READ
```

```sh
# CDC MOR table
bash hudi-upgrade-test.sh MERGE_ON_READ true
```

### Run all combinations (recommended)

**`hudi-upgrade-test-all.sh`** runs the full matrix: COPY_ON_WRITE and MERGE_ON_READ, each with `IS_CDC` false and true (four runs total). It uses the same **`hudi-upgrade.properties`** as `hudi-upgrade-test.sh`.

```sh
bash hudi-upgrade-test-all.sh
```

This is equivalent to:

```sh
bash hudi-upgrade-test.sh COPY_ON_WRITE false
bash hudi-upgrade-test.sh COPY_ON_WRITE true
bash hudi-upgrade-test.sh MERGE_ON_READ false
bash hudi-upgrade-test.sh MERGE_ON_READ true
```

## What the test does

1. **Generate table** — Creates a Hudi table with the source Hudi version (from `SOURCE_HUDI_VERSION` in `hudi-upgrade.properties`).
2. **Baseline tests** — Runs snapshot, incremental, timetravel, delete (and read_optimized for MOR, cdc for CDC tables) across multiple Spark/Hudi version combinations; results are tagged `baseline`.
3. **Upgrade** — Upgrades the table to the target Hudi version (`TARGET_HUDI_VERSION`), writes 5 more rows, then **hard-deletes 3 records**. Deleted records are not visible in snapshot or timetravel (after the delete instant).
4. **Post-upgrade tests** — Re-runs the same query types; results are tagged `upgrade`. The **delete** query type runs a snapshot count to confirm deletes are applied (e.g. upgrade count 22 = 20 + 5 − 3).

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

It reads all `*_baseline.csv` and `*_upgrade.csv` files in `results/`, prints upgrade status per Spark version and query type, and summarizes:

- **Snapshot, incremental, read_optimized (and cdc where applicable):** Upgrade has no issues when the difference vs baseline is **+5** (extra rows from post-upgrade writes).
- **Timetravel:** Count is expected to be **20** for both baseline and upgrade.

## Scripts

| Script | Purpose |
|--------|--------|
| `hudi-upgrade-test.sh` | Runs one test: table generation → baseline tests → upgrade → post-upgrade tests. Uses `hudi-upgrade.properties`. |
| `hudi-upgrade-test-all.sh` | Runs all four combinations (COW/MOR × CDC false/true) by calling `hudi-upgrade-test.sh`. Uses same `hudi-upgrade.properties`. |
| `hudi_upgrade_data_generator.py` | Creates/upgrades Hudi tables (init or upgrade mode). |
| `hudi_upgrade_test_queries.py` | Runs snapshot, incremental, timetravel, read_optimized, cdc queries and appends CSV results. |
| `generate_insights.py` | Compares baseline vs upgrade CSVs and prints per-type insights and verdicts. |

## Updating Hudi or Scala versions

1. Edit **`hudi-upgrade.properties`** in the project root.
2. Set `SOURCE_HUDI_VERSION`, `TARGET_HUDI_VERSION`, and/or `SCALA_VERSION` as needed.
3. Re-run `hudi-upgrade-test.sh` or `hudi-upgrade-test-all.sh`; no other files need to be changed.
