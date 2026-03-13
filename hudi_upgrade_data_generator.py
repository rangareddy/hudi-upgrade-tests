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

############################################
# Utility Logging
############################################

def log(msg):
    logger.info(msg)

############################################
# Read command line arguments
############################################

def read_arguments():

    table_type = sys.argv[1] if len(sys.argv) > 1 else "COPY_ON_WRITE"
    is_cdc_enabled = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    mode = sys.argv[3] if len(sys.argv) > 3 else "init"

    log(f"Table Type : {table_type}")
    log(f"CDC Enabled: {is_cdc_enabled}")
    log(f"Mode       : {mode}")

    return table_type, is_cdc_enabled, mode

############################################
# Create Spark Session
############################################

def create_spark_session():
    log("Starting Spark Session")
    spark = SparkSession.builder \
        .appName("hudi_upgrade_data_generator") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .getOrCreate()
    log(f"Spark Version   : {spark.version}")
    return spark

############################################
# Table Details
############################################

def get_table_details(table_type, is_cdc_enabled):
    if table_type == "COPY_ON_WRITE":
        table_name = "hudi_trips_cow_table"
    else:
        table_name = "hudi_trips_mor_table"

    if is_cdc_enabled:
        table_name = f"cdc_{table_name}"

    base_path = f"/tmp/{table_name}"
    
    log(f"Table Name      : {table_name}")
    log(f"Base Path       : {base_path}")

    return table_name, base_path


############################################
# Access Hudi DataGenerator
############################################

def get_hudi_data_generator(spark):
    log("Initializing Hudi DataGenerator")
    
    jvm = spark._jvm
    data_gen = jvm.org.apache.hudi.QuickstartUtils.DataGenerator()
    converter = jvm.org.apache.hudi.QuickstartUtils.convertToStringList

    return data_gen, converter


############################################
# Build Hudi options
############################################

def get_hudi_options(table_name, is_cdc_enabled):
    options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "uuid",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "partitionpath",
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.upsert.shuffle.parallelism": 2,
        "hoodie.insert.shuffle.parallelism": 2
    }
    if is_cdc_enabled:
        options["hoodie.table.cdc.enabled"] = "true"

    log("Hudi Write Options Loaded")
    return options


############################################
# Generate DataFrame
############################################

def generate_dataframe(spark, records):
    
    log(f"Generating DataFrame with {len(records)} records")

    rdd = spark.sparkContext.parallelize(records, 2)
    df = spark.read.json(rdd)
    return df


############################################
# Write Hudi table
############################################

def write_hudi(df, marker_value, base_path, hudi_options, mode):
    log(f"Writing commit with marker={marker_value} mode={mode}")

    df.withColumn("marker_col", lit(marker_value)) \
        .write.format("hudi") \
        .options(**hudi_options) \
        .mode(mode) \
        .save(base_path)
    log("Commit completed")

############################################
# Initial commits
############################################

def run_initial_commits(spark, data_gen, converter, base_path, hudi_options):

    log("Commit 1 → Inserts")

    inserts = list(converter(data_gen.generateInserts(20)))
    df = generate_dataframe(spark, inserts)

    write_hudi(df, "commit1_insert", base_path, hudi_options, "overwrite")

    log("Commit 2 → Updates")

    updates = list(converter(data_gen.generateUpdates(6)))
    df = generate_dataframe(spark, updates)

    write_hudi(df, "commit2_update", base_path, hudi_options, "append")


############################################
# Upgrade commit
############################################

def run_upgrade_commit(spark, data_gen, converter, base_path, hudi_options):

    log("Commit 3 → Inserts after upgrade")

    inserts = list(converter(data_gen.generateInserts(5)))
    df = generate_dataframe(spark, inserts)

    write_hudi(df, "commit3_insert_upgrade", base_path, hudi_options, "append")


############################################
# Run snapshot query
############################################

def run_snapshot_query(spark, base_path):
    log("Running snapshot query")

    spark.read.format("hudi").load(base_path) \
        .createOrReplaceTempView("hudi_trips_snapshot")
    
    spark.sql("""
    select marker_col, count(*)
    from hudi_trips_snapshot
    group by marker_col
    order by marker_col
    """).show(10, False)

############################################
# Run CDC query
############################################

def run_cdc_query(spark, base_path, is_cdc_enabled):
    if not is_cdc_enabled:
        return
    log("Running CDC query")

    cdc_df = spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "cdc") \
        .load(base_path)

    cdc_df.select(
        "_hoodie_commit_time",
        "_hoodie_operation",
        "uuid",
        "marker_col"
    ).show(truncate=False)


############################################
# Main 
############################################

def main():
    table_type, is_cdc_enabled, mode = read_arguments()

    spark = create_spark_session()

    table_name, base_path = get_table_details(
        table_type,
        is_cdc_enabled
    )

    data_gen, converter = get_hudi_data_generator(spark)

    hudi_options = get_hudi_options(
        table_name,
        is_cdc_enabled
    )

    if mode == "init":
        run_initial_commits(
            spark,
            data_gen,
            converter,
            base_path,
            hudi_options
        )

    elif mode == "upgrade":
        run_upgrade_commit(
            spark,
            data_gen,
            converter,
            base_path,
            hudi_options
        )

    log("Completed")

    ##################################
    # VALIDATION
    ##################################
    run_snapshot_query(spark, base_path)

    run_cdc_query(spark, base_path, is_cdc_enabled)

    log("Stopping Spark")

    spark.stop()
    log("Data generation completed")

############################################

if __name__ == "__main__":
    main()