import os
from time import sleep
import logging
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, lit, when, current_timestamp, pandas_udf
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
        
        ## 1. Extract from GCS
        #df = spark.read.parquet(input_path)
        #logger.info("Successfully read data from GCS.")
        
        #sleep(5)
        #df = df.drop("year", "month", "day")

        ## 2. Transform (Example: adding a new column)
        #logger.info("Applying transformations...")
        #df = df.withColumn("measurement_time", df["measurement_time"].cast(TimestampType()))
        #transformed_df = df.withColumn("processed_at", current_timestamp())
        
        ## Cast any 'timestamp_ntz' columns to standard 'timestamp'
        #for col_name, dtype in transformed_df.dtypes:
        #    if dtype == "timestamp_ntz":
        #        logger.debug(f"Casting column '{col_name}' from timestamp_ntz to timestamp")
        #        transformed_df = transformed_df.withColumn(col_name, col(col_name).cast("timestamp")


        
                
        ## 3. Load to BigQuery
        #staging_bucket = f"{bucket_name}-temp" 
        #target_table = f"{dataset}.output_table"
        #transformed_df.printSchema()
        #sleep(5)
        #logger.info(f"Writing to BigQuery table: {target_table}")
        #transformed_df.write \
        #    .format("bigquery") \
        #    .option("writeMethod", "direct") \
        #    .option("table", target_table) \
        #    .mode("overwrite") \
        #    .save()
            
        #logger.info("Data successfully written to BigQuery.")


        df_bronze = spark.read.parquet(input_path)
        logger.info("Successfully read data from GCS.")

        df_silver = (
            df_bronze
            # A. Drop completely empty/useless columns
            .drop("ISP")

            .withColumn("processed_at", current_timestamp())
            
            # B. Cast timestamp to a proper datetime object
            .withColumn("measurement_time", to_timestamp(col("measurement_time"), "yyyy-MM-dd HH:mm:ss"))
            
            # C. Create a partitioning column (date) for optimized BigQuery/GCS performance
            .withColumn("measurement_date", to_date(col("measurement_time")))
            
            # D. Handle Nulls in Geographic / Contract data (Filling nulls makes BI tools happier)
            .fillna({
                "connection_municipality": "Unknown",
                "connection_regional_unit": "Unknown",
                "connection_periphery": "Unknown",
                "client_operating_system_version": "Unknown"
            })
            
            # E. Data Quality Rules: Ensure no negative speeds or impossible ping/rtt 
            # (Replaces invalid measurements with Null or filters them out)
            .withColumn("measured_downstream_mbps", 
                        when(col("measured_downstream_mbps") < 0, lit(None))
                        .otherwise(col("measured_downstream_mbps")))
            .withColumn("measured_rtt_msec", 
                        when(col("measured_rtt_msec") < 0, lit(None))
                        .otherwise(col("measured_rtt_msec")))
                        
            # F. Deduplication
            # Drop exact duplicates just in case the app double-sent a payload
            .dropDuplicates(["measurement_id"])
        )

        # 3. Load to BigQuery
        staging_bucket = f"{bucket_name}-temp" 
        target_table = f"{dataset}.output_table"
        df_silver.printSchema()
        sleep(5)
        logger.info(f"Writing to BigQuery table: {target_table}")
        df_silver.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .option("table", target_table) \
            .mode("overwrite") \
            .save()
            
        #logger.info("Data successfully written to BigQuery.")

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