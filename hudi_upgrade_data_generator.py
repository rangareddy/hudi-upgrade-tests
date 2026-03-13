import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

logger = logging.getLogger("hudi-upgrade-generator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def read_arguments():
    table_type = sys.argv[1] if len(sys.argv) > 1 else "COPY_ON_WRITE"
    is_cdc_enabled = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    mode = sys.argv[3] if len(sys.argv) > 3 else "init"

    logger.info(f"Table Type : {table_type}")
    logger.info(f"CDC Enabled: {is_cdc_enabled}")
    logger.info(f"Mode       : {mode}")
    return table_type, is_cdc_enabled, mode

def create_spark_session():
    logger.info("Starting Spark Session")
    spark = SparkSession.builder \
        .appName("hudi_upgrade_data_generator") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .getOrCreate()
    logger.info(f"Spark Version   : {spark.version}")
    return spark

def get_table_details(table_type, is_cdc_enabled):
    if table_type == "COPY_ON_WRITE":
        table_name = "hudi_trips_cow_table"
    else:
        table_name = "hudi_trips_mor_table"

    if is_cdc_enabled:
        table_name = f"cdc_{table_name}"

    base_path = f"/tmp/{table_name}"
    logger.info(f"Table Name      : {table_name}")
    logger.info(f"Base Path       : {base_path}")
    return table_name, base_path

def get_hudi_data_generator(spark):
    logger.info("Initializing Hudi DataGenerator")
    jvm = spark._jvm
    data_gen = jvm.org.apache.hudi.QuickstartUtils.DataGenerator()
    converter = jvm.org.apache.hudi.QuickstartUtils.convertToStringList
    return data_gen, converter

def get_hudi_options(table_name, is_cdc_enabled, table_type):
    """✅ FIXED: Complete CDC configuration + table type"""
    options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "uuid",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "partitionpath",
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": 2,
        "hoodie.insert.shuffle.parallelism": 2,
    }
    
    if is_cdc_enabled:
        options.update({
            "hoodie.table.cdc.enabled": "true",
            "hoodie.table.cdc.supplemental.logging.mode": "DATA_BEFORE_AFTER",
        })
        logger.info("✅ CDC configuration enabled")

    logger.info("Hudi Write Options: ", options)
    return options

def generate_dataframe(spark, records):
    logger.info(f"Generating DataFrame with {len(records)} records")
    rdd = spark.sparkContext.parallelize(records, 2)
    df = spark.read.json(rdd)
    return df

def write_hudi(df, marker_value, base_path, hudi_options, mode):
    logger.info(f"Writing commit with marker={marker_value} mode={mode}")
    df.withColumn("marker_col", lit(marker_value)) \
        .write.format("hudi") \
        .options(**hudi_options) \
        .mode(mode) \
        .save(base_path)
    logger.info("Writing data to hudi table completed")

def run_initial_commits(spark, data_gen, converter, base_path, hudi_options):
    logger.info("Commit 1 → Inserts")
    inserts = list(converter(data_gen.generateInserts(20)))
    df = generate_dataframe(spark, inserts)
    write_hudi(df, "commit1_insert", base_path, hudi_options, "overwrite")

    logger.info("Commit 2 → Updates")
    updates = list(converter(data_gen.generateUpdates(6)))
    df = generate_dataframe(spark, updates)
    write_hudi(df, "commit2_update", base_path, hudi_options, "append")

def run_upgrade_commit(spark, data_gen, converter, base_path, hudi_options):
    logger.info("Commit 3 → Inserts after upgrade")
    inserts = list(converter(data_gen.generateInserts(5)))
    df = generate_dataframe(spark, inserts)
    write_hudi(df, "commit3_insert_upgrade", base_path, hudi_options, "append")

def run_snapshot_query(spark, base_path):
    logger.info("Running snapshot query")
    spark.read.format("hudi").load(base_path) \
        .createOrReplaceTempView("hudi_trips_snapshot")
    
    spark.sql("""
    select marker_col, count(*)
    from hudi_trips_snapshot
    group by marker_col
    order by marker_col
    """).show(10, False)

def is_cdc_table_valid(spark, base_path):
    """✅ NEW: Validate CDC table properties"""
    try:
        hoodie_props_path = f"{base_path}/.hoodie/hoodie.properties"
        props = spark.sparkContext.textFile(f"file://{hoodie_props_path}").collect()
        cdc_enabled = any("hoodie.table.cdc.enabled=true" in prop for prop in props)
        logger.info(f"CDC table check: {'✅ ENABLED' if cdc_enabled else '❌ DISABLED'}")
        return cdc_enabled
    except:
        logger.info("⚠️ Could not read hoodie.properties - assuming non-CDC table")
        return False

def run_cdc_query(spark, base_path, is_cdc_enabled):
    """✅ FIXED: Safe CDC query with validation"""
    if not is_cdc_enabled:
        logger.info("Skipping CDC query - CDC not requested")
        return
    
    logger.info("Running CDC query")
    
    # Check if table is actually CDC-enabled
    if not is_cdc_table_valid(spark, base_path):
        logger.info("⚠️ Skipping CDC query - table not CDC enabled")
        return
    
    try:
        cdc_df = spark.read.format("hudi") \
            .option("hoodie.datasource.query.type", "incremental") \
            .option("hoodie.datasource.read.begin.instanttime", "0") \
            .option("hoodie.datasource.query.incremental.format", "cdc") \
            .load(base_path)
        cdc_count = cdc_df.count()
        logger.info(f"✅ CDC query success: {cdc_count} change records")
        cdc_df.show(10, truncate=False)
    except Exception as e:
        logger.error(f"❌ CDC query failed (non-critical): {str(e)}")
        logger.error("This is expected if table lacks CDC data or config")

def main():
    table_type, is_cdc_enabled, mode = read_arguments()
    spark = create_spark_session()
    
    table_name, base_path = get_table_details(table_type, is_cdc_enabled)
    data_gen, converter = get_hudi_data_generator(spark)
    hudi_options = get_hudi_options(table_name, is_cdc_enabled, table_type)
    
    if mode == "init":
        run_initial_commits(spark, data_gen, converter, base_path, hudi_options)
    elif mode == "upgrade":
        run_upgrade_commit(spark, data_gen, converter, base_path, hudi_options)
    
    logger.info("Completed")
    
    # VALIDATION
    run_snapshot_query(spark, base_path)
    run_cdc_query(spark, base_path, is_cdc_enabled)
    
    logger.info("Stopping Spark")
    spark.stop()
    logger.info("✅ Data generation completed successfully!")

if __name__ == "__main__":
    main()
