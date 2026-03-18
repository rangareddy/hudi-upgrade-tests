#!/usr/bin/env python3

"""
Hudi Upgrade Test Queries - Multiple Query Types Validation
Supports: snapshot, read_optimized, incremental, cdc, timetravel
CSV output + Table format with run_mode column
Usage: python hudi_upgrade_test_queries.py COW false baseline
"""

import sys
import time
import re
import csv
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession

logger = logging.getLogger("hudi-upgrade-test-queries")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

working_dir = os.path.dirname(os.path.abspath(__file__))    

############################################
# Read command line arguments
############################################

def read_arguments():
    """Parse CLI arguments: tableType, is_cdc, run_mode"""
    tableType = sys.argv[1] if len(sys.argv) > 1 else "COPY_ON_WRITE"
    is_cdc_table = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    run_mode = sys.argv[3] if len(sys.argv) > 3 else "baseline"
    is_mor_table = tableType == "MERGE_ON_READ"
    logger.info(f"Run mode: {run_mode}, Table: {tableType}, CDC: {is_cdc_table}")
    return tableType, is_cdc_table, is_mor_table, run_mode

############################################
# Table name logic
############################################

def get_table_details(tableType, is_cdc_table):
    """Generate table name and path"""
    tableName = os.environ.get("TABLE_NAME")
    if not tableName:
        raise ValueError("TABLE_NAME environment variable is not set")
    basePath = f"/tmp/{tableName}"
    if not os.path.exists(basePath):
        logger.error(f"Table path does not exist: {basePath}")
        sys.exit(1)
    logger.info(f"Table: {tableName} at {basePath}")
    return tableName, basePath


############################################
# Create Spark Session
############################################

def create_spark_session():
    """Create Spark session with Hudi support"""
    spark = SparkSession.builder \
        .appName("hudi_upgrade_query_validation") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .getOrCreate()
    return spark


############################################
# Fetch Hudi Version
############################################

def get_hudi_version(spark):
    """Extract Hudi version from JARs"""
    hudi_version = "UNKNOWN"
    try:
        jars = spark.sparkContext._jsc.sc().listJars()
        iterator = jars.iterator()
        while iterator.hasNext():
            jar = str(iterator.next())
            if "hudi" in jar and "bundle" in jar:
                jar_name = jar.split("/")[-1]
                hudi_version = jar_name.split("-")[-1].replace(".jar", "")
                break
    except Exception as e:
        logger.error(f"Error extracting Hudi version: {e}")
    logger.info(f"Hudi version: {hudi_version}")
    return hudi_version


############################################
# Fetch Table Version
############################################

def get_table_version(spark, basePath):
    """Get Hudi table version from hoodie.properties"""
    try:
        hoodie_prop_path = os.path.join(basePath, ".hoodie", "hoodie.properties")
        props = spark.sparkContext.textFile(hoodie_prop_path).collect()
        table_version = [
            p.split("=")[1] for p in props
            if p.startswith("hoodie.table.version")
        ]
        version = table_version[0] if table_version else "0"
        logger.info(f"Table version: {version}")
        return version
    except Exception as e:
        logger.warning(f"Failed to fetch table version: {e}")
        return "0"


############################################
# Get Query Types
############################################

def get_query_types(is_mor_table, is_cdc_table):
    """Determine available query types."""
    query_types = [
        "snapshot",
        "incremental",
        "timetravel",
    ]
    if is_mor_table:
        query_types.append("read_optimized")
    if is_cdc_table:
        query_types.append("cdc")
    logger.info(f"Query types: {query_types}")
    return query_types


############################################
# Get begin instant
############################################

def get_begin_instant(spark, basePath):
    """Get earliest commit time for incremental queries"""
    try:
        df = spark.read.format("hudi").load(basePath)
        begin_instant = df.agg({"_hoodie_commit_time": "min"}).collect()[0][0]
        logger.info(f"Begin Instant : {begin_instant}")
        return begin_instant
    except Exception as e:
        logger.error(f"Failed to fetch begin instant: {e}")
        return None


def extract_root_java_exception(error):
    """Extract root Java exception from Spark error"""
    error_str = str(error)

    # Java exceptions
    matches = re.findall(
        r"(java\.[\w\.]+(?:Exception|Error)(?::[^\n]+)?)",
        error_str
    )

    if matches:
        return matches[-1].strip()   # return deepest java exception

    # Spark exceptions
    spark_match = re.search(
        r"(org\.apache\.spark\.[\w\.]+Exception)",
        error_str
    )

    if spark_match:
        return spark_match.group(1)

    # fallback to first line
    return error_str.split("\n")[0]


############################################
# Execute a single query
############################################

def execute_query(spark, basePath, queryType, instantTime):
    """Execute specific Hudi query type"""
    try:
        read_options = {}
        if queryType == "read_optimized":
            read_options.update({
                'hoodie.datasource.query.type': 'read_optimized',
            })
        elif queryType == "incremental":
            read_options.update({
                'hoodie.datasource.query.type': 'incremental',
                'hoodie.datasource.read.begin.instanttime': instantTime,
            })
        elif queryType == "cdc":
            read_options.update({
                'hoodie.datasource.query.incremental.format': 'cdc',
                'hoodie.datasource.query.type': 'incremental',
                'hoodie.datasource.read.begin.instanttime': 0,
            })
        elif queryType == "timetravel":
            read_options.update({
                'as.of.instant': instantTime,
            })
        
        if read_options:
            df = spark.read.format("hudi").options(**read_options).load(basePath)
        else:
            df = spark.read.format("hudi").load(basePath)
        count = df.count()
        logger.info(f"✅ {queryType}: {count} rows")
        return "SUCCESS", "", count
    except Exception as e:
        error_message = extract_root_java_exception(e)
        logger.error(f"❌ {queryType} FAILED: {error_message}")
        return "FAILED", error_message, 0


############################################
# Run Query Validation
############################################

def run_validation(spark, basePath, query_types, beginInstant,
                   spark_version, hudi_version, table_version,
                   tableType, tableName, is_cdc_table, run_mode):
    """Run all query types and collect results"""

    results = []
    for queryType in query_types:
        status, error_message, count = execute_query(
            spark, basePath, queryType, beginInstant
        )

        results.append({
            "run_mode": run_mode,
            "spark_version": spark_version,
            "hudi_version": hudi_version,
            "table_version": table_version,
            "table_type": tableType,
            "table_name": tableName,
            "is_cdc": is_cdc_table,
            "query_type": queryType,
            "status": status,
            "count": count,
            "error_message": error_message,
            "run_mode": run_mode
        })

    return results


############################################
# Print Results
############################################

def print_results(results):
    """Print results in formatted table"""

    print("\n" + "="*180)
    print("🚀 HUDI UPGRADE QUERY VALIDATION RESULTS")
    print("="*180)

    row_format = "| {:<10} | {:<13} | {:<12} | {:<13} | {:<20} | {:<6} | {:<15} | {:<8} | {:<8} | {:<50} |"
    
    header = row_format.format(
        "run_mode", "spark_version", "hudi_version", 
        "table_version", "table_type", "is_cdc", "query_type", 
        "status", "count", "error_message"
    )
    
    separator = "-" * 180
    print(separator)
    print(header)
    print(separator)

    for r in results:
        error_msg = r["error_message"][:47] + "..." if len(r["error_message"]) > 50 else r["error_message"]
        print(row_format.format(
            r["run_mode"],
            r["spark_version"],
            r["hudi_version"],
            r["table_version"],
            r["table_type"],
            str(r["is_cdc"]),
            r["query_type"],
            r["status"],
            str(r["count"]),
            error_msg
        ))
    
    print(separator)
    
    # Summary
    success = sum(1 for r in results if r["status"] == "SUCCESS")
    failed = len(results) - success
    print(f"\n📊 SUMMARY: SUCCESS={success}/{len(results)} | FAILED={failed}")
    print("="*180 + "\n")


############################################
# Write Results to CSV
############################################

def write_results_to_csv(tableName, run_mode, results):
    """Append results to CSV file"""
    results_dir = os.path.join(working_dir, "results")
    os.makedirs(results_dir, exist_ok=True)
    file_name = os.path.join(results_dir, f"{tableName}_{run_mode}.csv")
    file_exists = os.path.isfile(file_name)

    header = results[0].keys()

    with open(file_name, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)

        if not file_exists:
            writer.writeheader()

        writer.writerows(results)

    logger.info(f"✅ Results saved: {file_name}")
    return file_name

############################################
# Main
############################################

def main():
    """Main execution flow"""

    tableType, is_cdc_table, is_mor_table, run_mode = read_arguments()
    tableName, basePath = get_table_details(tableType, is_cdc_table)
    spark = create_spark_session()
    spark_version = spark.version
    hudi_version = get_hudi_version(spark)
    table_version = get_table_version(spark, basePath)
    query_types = get_query_types(is_mor_table, is_cdc_table)
    beginInstant = get_begin_instant(spark, basePath)

    logger.info("="*60)
    logger.info("Hudi Upgrade Query Validation Started")
    logger.info(f"Run Mode: {run_mode}")
    logger.info(f"Spark: {spark_version} | Hudi: {hudi_version} | Table: {table_version}")
    logger.info(f"Path: {basePath} | Queries: {query_types}")
    logger.info("="*60)
    
    results = run_validation(
        spark, basePath, query_types, beginInstant,
        spark_version, hudi_version, table_version,
        tableType, tableName, is_cdc_table, run_mode
    )
    spark.stop()
    print_results(results)
    csv_file = write_results_to_csv(tableName, run_mode, results)
    logger.info(f"✅ Complete! CSV: {csv_file}")
############################################

if __name__ == "__main__":
    main()