import json
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import LongType

logger = logging.getLogger("hudi-upgrade-generator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Same partition layout as Hudi QuickstartUtils (stable across runs)
_PARTITIONS = (
    "americas/united_states/san_francisco",
    "americas/brazil/sao_paulo",
    "asia/india/chennai",
)

# Fixed ts bases so precombine ordering is identical every run (no wall-clock randomness)
_TS_INSERT_BASE = 1_000_000_000_000
_TS_UPDATE_BASE = 2_000_000_000_000
_TS_UPGRADE_INSERT_BASE = 3_000_000_000_000


def _stable_uuid(seq: int) -> str:
    """Deterministic UUID-shaped string (hex segments only)."""
    return f"{seq:08x}-0000-4000-8000-{seq:012x}"


def _trip_json(seq: int, ts: int, rider_tag: str) -> str:
    pid = seq % 3
    return json.dumps(
        {
            "ts": ts,
            "uuid": _stable_uuid(seq),
            "rider": f"rider-{rider_tag}",
            "driver": f"driver-{rider_tag}",
            "begin_lat": 37.0 + 0.01 * seq,
            "begin_lon": -122.0 + 0.01 * seq,
            "end_lat": 37.1 + 0.01 * seq,
            "end_lon": -122.1 + 0.01 * seq,
            "fare": 20.0 + seq,
            "partitionpath": _PARTITIONS[pid],
        },
        separators=(",", ":"),
    )


def deterministic_insert_records(count: int, start_seq: int = 1, ts_base: int = _TS_INSERT_BASE) -> list[str]:
    """Fixed trip JSON lines for initial / upgrade inserts."""
    return [_trip_json(start_seq + i, ts_base + i, "ins") for i in range(count)]


def deterministic_update_records(num_updates: int, start_seq: int = 1, ts_base: int = _TS_UPDATE_BASE) -> list[str]:
    """Updates for the first `num_updates` keys (same uuid as inserts seq 1..num_updates)."""
    return [_trip_json(start_seq + i, ts_base + i, "upd") for i in range(num_updates)]


def read_arguments():
    table_type = sys.argv[1] if len(sys.argv) > 1 else "COPY_ON_WRITE"
    is_cdc_enabled = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    mode = sys.argv[3] if len(sys.argv) > 3 else "init"

    logger.info("Table Type : %s", table_type)
    logger.info("CDC Enabled: %s", is_cdc_enabled)
    logger.info("Mode       : %s", mode)
    return table_type, is_cdc_enabled, mode


def create_spark_session():
    logger.info("Starting Spark Session")
    spark = (
        SparkSession.builder.appName("hudi_upgrade_data_generator")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    logger.info("Spark Version   : %s", spark.version)
    return spark


def get_table_details(table_type, is_cdc_enabled):
    table_name = os.environ.get("TABLE_NAME")
    if not table_name:
        raise ValueError("TABLE_NAME environment variable is not set")

    base_path = f"/tmp/{table_name}"
    logger.info("Table Name      : %s", table_name)
    logger.info("Base Path       : %s", base_path)
    return table_name, base_path


def get_hudi_options(table_name, is_cdc_enabled, table_type):
    """Complete CDC configuration + table type"""
    options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "uuid",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "partitionpath",
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
    }

    if is_cdc_enabled:
        options.update(
            {
                "hoodie.table.cdc.enabled": "true",
                "hoodie.table.cdc.supplemental.logging.mode": "DATA_BEFORE_AFTER",
            }
        )
        logger.info("CDC configuration enabled")

    logger.info("Hudi Write Options: %s", options)
    return options


def generate_dataframe(spark, records: list[str]):
    """Build DataFrame from JSON lines; list order is preserved for reproducibility."""
    logger.info("Generating DataFrame with %d records (deterministic)", len(records))
    rdd = spark.sparkContext.parallelize(records, 2)
    df = spark.read.json(rdd)
    return df


def ensure_precombine_long(df, precombine_field: str = "ts"):
    if precombine_field not in df.columns:
        return df
    return df.withColumn(precombine_field, col(precombine_field).cast(LongType()))


def write_hudi(df, marker_value, base_path, hudi_options, mode):
    logger.info("Writing commit with marker=%s mode=%s", marker_value, mode)
    precombine_field = hudi_options.get("hoodie.datasource.write.precombine.field", "ts")
    df = ensure_precombine_long(df, precombine_field)
    df.withColumn("marker_col", lit(marker_value)).write.format("hudi").options(**hudi_options).mode(mode).save(
        base_path
    )
    logger.info("Writing data to hudi table completed")


def run_delete_commit(spark, base_path, hudi_options, num_deletes: int = 3):
    """Hard delete: always pick the same keys (order by uuid) for reproducible results."""
    logger.info("Reading table to select keys for delete")
    df = spark.read.format("hudi").load(base_path)
    cols = df.columns
    if "partitionpath" in cols:
        keys_df = df.select("uuid", "partitionpath").orderBy(col("uuid").asc()).limit(num_deletes)
    elif "_hoodie_partition_path" in cols:
        keys_df = (
            df.select(col("uuid"), col("_hoodie_partition_path").alias("partitionpath"))
            .orderBy(col("uuid").asc())
            .limit(num_deletes)
        )
    else:
        logger.warning("No partition path column found; using uuid only for delete")
        keys_df = df.select("uuid").orderBy(col("uuid").asc()).limit(num_deletes)
    delete_count = keys_df.count()
    if delete_count == 0:
        logger.warning("No rows to delete, skipping delete commit")
        return
    logger.info("Commit 4 → Deleting %d records (stable uuid order)", delete_count)
    delete_options = {**hudi_options, "hoodie.datasource.write.operation": "delete"}
    keys_df.write.format("hudi").options(**delete_options).mode("append").save(base_path)
    logger.info("Delete commit completed")


def run_initial_commits(spark, base_path, hudi_options):
    logger.info("Commit 1 → Inserts (deterministic)")
    records = deterministic_insert_records(20, start_seq=1, ts_base=_TS_INSERT_BASE)
    df = generate_dataframe(spark, records)
    write_hudi(df, "commit1_insert", base_path, hudi_options, "overwrite")

    logger.info("Commit 2 → Updates (deterministic)")
    records = deterministic_update_records(6, start_seq=1, ts_base=_TS_UPDATE_BASE)
    df = generate_dataframe(spark, records)
    write_hudi(df, "commit2_update", base_path, hudi_options, "append")


def run_upgrade_commit(spark, base_path, hudi_options):
    logger.info("Commit 3 → Inserts after upgrade (deterministic)")
    records = deterministic_insert_records(5, start_seq=21, ts_base=_TS_UPGRADE_INSERT_BASE)
    df = generate_dataframe(spark, records)
    write_hudi(df, "commit3_insert_upgrade", base_path, hudi_options, "append")

    # CDC MOR + hard delete can cause Float/DeleteRecord ClassCastException on read; skip deletes for CDC.
    is_cdc = hudi_options.get("hoodie.table.cdc.enabled", "").lower() == "true"
    if is_cdc:
        logger.info("Commit 4 → Skipping deletes (CDC table; avoids read-path ClassCastException)")
    else:
        logger.info("Commit 4 → Deletes (a few records, stable key order)")
        run_delete_commit(spark, base_path, hudi_options, num_deletes=3)


def run_snapshot_query(spark, base_path):
    logger.info("Running snapshot query")
    spark.read.format("hudi").load(base_path).createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql(
        """
    select marker_col, count(*)
    from hudi_trips_snapshot
    group by marker_col
    order by marker_col
    """
    ).show(10, False)


def is_cdc_table_valid(spark, base_path):
    try:
        hoodie_props_path = f"{base_path}/.hoodie/hoodie.properties"
        props = spark.sparkContext.textFile(f"file://{hoodie_props_path}").collect()
        cdc_enabled = any("hoodie.table.cdc.enabled=true" in prop for prop in props)
        logger.info("CDC table check: %s", "ENABLED" if cdc_enabled else "DISABLED")
        return cdc_enabled
    except Exception:
        logger.info("Could not read hoodie.properties - assuming non-CDC table")
        return False


def run_cdc_query(spark, base_path, is_cdc_enabled):
    if not is_cdc_enabled:
        logger.info("Skipping CDC query - CDC not requested")
        return

    logger.info("Running CDC query")

    if not is_cdc_table_valid(spark, base_path):
        logger.info("Skipping CDC query - table not CDC enabled")
        return

    try:
        cdc_df = (
            spark.read.format("hudi")
            .option("hoodie.datasource.query.type", "incremental")
            .option("hoodie.datasource.read.begin.instanttime", "0")
            .option("hoodie.datasource.query.incremental.format", "cdc")
            .load(base_path)
        )
        cdc_count = cdc_df.count()
        logger.info("CDC query success: %d change records", cdc_count)
        cdc_df.show(10, truncate=False)
    except Exception as e:
        logger.error("CDC query failed (non-critical): %s", str(e))


def main():
    table_type, is_cdc_enabled, mode = read_arguments()
    spark = create_spark_session()

    table_name, base_path = get_table_details(table_type, is_cdc_enabled)
    hudi_options = get_hudi_options(table_name, is_cdc_enabled, table_type)

    if mode == "init":
        run_initial_commits(spark, base_path, hudi_options)
    elif mode == "upgrade":
        run_upgrade_commit(spark, base_path, hudi_options)

    logger.info("Data generation completed successfully!")
    logger.info("Validating the data....")
    run_snapshot_query(spark, base_path)
    run_cdc_query(spark, base_path, is_cdc_enabled)
    logger.info("Data validation completed successfully!")
    logger.info("Stopping Spark")
    spark.stop()


if __name__ == "__main__":
    main()
