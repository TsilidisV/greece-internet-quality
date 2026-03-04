import os
from time import sleep
import logging
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lag, when, sum as _sum, concat, lit, 
    min, max, count, avg, pandas_udf
)
from pyspark.sql.types import TimestampType, FloatType

# 1. Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("GCStoBQ_ETL")

def get_spark_session():
    logger.info("Initializing SparkSession...")
    gcp_key_path = os.environ.get("TRANSFORM_GCP_KEY")
    project_id = os.environ.get("PROJECT_ID")
    
    gcs_jar_url = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar"
    bq_package = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0"

    # Base configuration
    builder = SparkSession.builder \
        .appName("GCS-to-BigQuery-ETL") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars", gcs_jar_url) \
        .config("spark.jars.packages", bq_package) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        
        # --- NEW ADDITIONS EXPLAINED ---
        # .master("local[2]") 
        # Forces Spark to run locally using exactly 2 threads. Matches our container limits.
        #
        # .config("spark.driver.memory", "2g") 
        # Limits Spark's memory usage so the container doesn't hit Out-Of-Memory (OOM) errors.
        #
        # .config("spark.sql.shuffle.partitions", "2") 
        # Default is 200. For <500MB data, 200 partitions creates massive CPU overhead. 
        # Lowering this speeds up the job drastically, saving compute seconds.

    # CONDITIONAL: Only use the JSON key if the environment variable is set (Local mode)
    if gcp_key_path:
        logger.info("Using local GCP Service Account key...")
        builder = builder \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path)
    else:
        logger.info("No local key found. Defaulting to Cloud Run Service Account (ADC)...")

    if project_id:
        logger.debug(f"Setting parentProject to {project_id}")
        builder = builder.config("parentProject", project_id)

    spark = builder.getOrCreate()
    logger.info("SparkSession initialized successfully.")
    return spark

@pandas_udf(FloatType())
def calculate_trend_slope(time_col: pd.Series, speed_col: pd.Series) -> float:
    # If the user has less than 3 tests, we can't establish a reliable trend
    if len(speed_col) < 3:
        return 0.0
    
    # Convert timestamps to numeric values (e.g., hours since first test)
    time_numeric = pd.to_datetime(time_col).astype('int64') // 10**9 
    
    # Use Numpy to calculate the linear regression slope (polyfit degree 1)
    # This tells us how much the Mbps changes per second
    try:
        slope, _ = np.polyfit(time_numeric, speed_col, 1)
        return float(slope)
    except:
        return 0.0

def main():
    logger.info("Starting GCS to BigQuery ETL job.")
    
    try:
        spark = get_spark_session()
        
        bucket_name = os.environ.get("GCS_BUCKET_NAME")
        dataset = os.environ.get("BQ_DATASET")
        
        # Log a warning if environment variables are missing
        if not bucket_name or not dataset:
            logger.warning("One or more environment variables (GCS_BUCKET_NAME, BQ_DATASET) are missing!")
        
        input_path = f"gs://{bucket_name}/hyperion/"
        logger.info(f"Reading from: {input_path}")
        
        
        # -------------------------------------------------------------------
        # ZONE 1: SILVER MEASUREMENTS (Data Cleansing + Sessionization)
        # -------------------------------------------------------------------
        df_bronze = spark.read.parquet(input_path)
        
        # Clean up base data
        df_clean = df_bronze.drop("ISP") \
            .withColumn("measurement_time", col("measurement_time").cast("timestamp")) \
            .dropna(subset=["measured_downstream_mbps"])

        # Define a window partitioned by IP and ordered by time
        window_ip_time = Window.partitionBy("client_ip").orderBy("measurement_time")

        # A. Calculate the time difference (in seconds) between consecutive tests for the same IP
        df_lag = df_clean.withColumn("prev_time", lag("measurement_time").over(window_ip_time))
        df_diff = df_lag.withColumn("time_diff_sec", 
                                    col("measurement_time").cast("long") - col("prev_time").cast("long"))

        # B. Flag as '1' if it's a new session (first record OR gap is > 30 mins / 1800 seconds)
        df_flags = df_diff.withColumn("is_new_session", 
                                    when(col("time_diff_sec").isNull() | (col("time_diff_sec") > 1800), 1).otherwise(0))

        # C. Use a cumulative sum to generate a unique integer for each session per IP
        window_cumulative = Window.partitionBy("client_ip").orderBy("measurement_time") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        df_sessions = df_flags.withColumn("session_int", _sum("is_new_session").over(window_cumulative))

        # D. Combine IP and session integer to create a globally unique 'session_id'
        silver_measurements = df_sessions.withColumn(
            "session_id", concat(col("client_ip"), lit("_SESS_"), col("session_int"))
        ).drop("prev_time", "time_diff_sec", "is_new_session", "session_int")

        # -> WRITE TO BIGQUERY OR GCS: my_project.silver.measurements
        target_table = f"{dataset}.output_table"

        silver_measurements.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .option("table", target_table) \
            .mode("overwrite") \
            .save()
            
        logger.info("Data successfully written to BigQuery.")










    except Exception as e:
        # exc_info=True captures the full stack trace, making debugging much easier
        logger.error(f"ETL job failed with error: {str(e)}", exc_info=True)
        raise e 
        
    finally:
        # Ensure Spark stops cleanly even if the script crashes
        if 'spark' in locals():
            logger.info("Stopping SparkSession...")
            spark.stop()

    logger.info('ETL job completed successfully!')


if __name__ == "__main__":
    main()