import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, sha2, when, lag, sum as _sum, 
    unix_timestamp, count, covar_pop, var_pop, expr, concat_ws, to_date, date_trunc
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, LongType, DoubleType


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
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
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


def main():
    logger.info("Starting GCS to BigQuery ETL job.")
    
    try:
        spark = get_spark_session()
        
        bucket_name = os.environ.get("GCS_BUCKET_NAME")
        dataset = os.environ.get("BQ_DATASET")

        logger.info("1.....")

        # Log a warning if environment variables are missing
        if not bucket_name or not dataset:
            logger.warning("One or more environment variables (GCS_BUCKET_NAME, BQ_DATASET) are missing!")

        # ---------------------------------------------------------
        # 1. READ BRONZE (Raw CSV)
        # ---------------------------------------------------------
        #silver_schema = hyperion_schema = StructType([
        #    StructField("measurement_time", TimestampType(), True), 
        #    StructField("connection_id", StringType(), True), 
        #    StructField("client_ip", StringType(), True),
        #    StructField("measurement_id", LongType(), True),  # Could also be LongType depending on scale
        #    StructField("measured_downstream_mbps", DoubleType(), True),
        #    StructField("measured_upstream_mbps", DoubleType(), True),
        #    StructField("measured_rtt_msec", DoubleType(), True),
        #    StructField("measured_loss_percentage", DoubleType(), True),
        #    StructField("measured_jitter_msec", DoubleType(), True),
        #    StructField("client_operating_system", StringType(), True),
        #    StructField("client_operating_system_version", StringType(), True), 
        #    StructField("client_operating_system_architecture", StringType(), True),
        #    StructField("ISP", StringType(), True), 
        #    StructField("contract_download_mbps", DoubleType(), True), 
        #    StructField("contract_upload_mbps", DoubleType(), True), 
        #    StructField("connection_postal_code", StringType(), True), 
        #    StructField("connection_municipality_id", StringType(), True), 
        #    StructField("connection_municipality", StringType(), True),
        #    StructField("connection_regional_unit_id", StringType(), True), 
        #    StructField("connection_regional_unit", StringType(), True),
        #    StructField("connection_periphery_id", StringType(), True), 
        #    StructField("connection_periphery", StringType(), True)
        #])

        input_path = f"gs://{bucket_name}/hyperion/"
        logger.info(f"Reading from: {input_path}")

        bronze_df = spark.read.option("mergeSchema", "true").parquet(input_path)
        
        # ---------------------------------------------------------
        # 2. CREATE SILVER (Cleansed & Conformed)
        # ---------------------------------------------------------

        # Type-cast measurement_time and hash user_id
        silver_df = bronze_df \
            .withColumn("measurement_time", to_timestamp(col("measurement_time"))) \
            .withColumn("measurement_date", to_date(col("measurement_time"))) \
            .withColumn("user_id", sha2(col("client_ip"), 256)) \
            .drop("client_ip") # Drop raw IP for GDPR/Privacy compliance

        # Cast metrics to float and handle string "NaN" values
        numeric_cols = ["measured_downstream_mbps", "measured_upstream_mbps", "measured_rtt_msec"]
        for c in numeric_cols:
            silver_df = silver_df.withColumn(c, col(c).cast("float"))

        string_nan_cols = ["connection_municipality", "connection_regional_unit", "connection_periphery"]
        for c in string_nan_cols:
            silver_df = silver_df.withColumn(
                c, 
                when((col(c) == "NaN") | (col(c).isNull()), 'N/A')
                .otherwise(col(c))
            )

        # Basic Data Quality Filter
        silver_df = silver_df.filter(col("measurement_time").isNotNull()) \
                            .filter(col("measured_downstream_mbps") >= 0)
        
        # Dedupe based on measurement_id and measurement_date
        silver_df = silver_df \
            .dropDuplicates(['measurement_date', 'measurement_id'])
        
        #silver_df.cache()

        # -> WRITE TO BIGQUERY OR GCS: my_project.silver.measurements
        target_table = f"{dataset}.silver_measurements"

        silver_df.write \
            .format("bigquery") \
            .option("writeMethod", "indirect") \
            .option("temporaryGcsBucket", bucket_name) \
            .option("table", target_table) \
            .option("partitionField", "measurement_date") \
            .option("partitionType", "DAY") \
            .option("clusteredFields", "user_id,connection_regional_unit") \
            .mode("overwrite") \
            .save()
            
        logger.info(f"{target_table} successfully written to BigQuery.")

        logger.info(f"Reading {target_table} back from BigQuery for Gold transformations...")
        silver_df = spark.read \
            .format("bigquery") \
            .option("table", target_table) \
            .load()
        logger.info(f"{target_table} successfully read from BigQuery.")

        # ---------------------------------------------------------
        # 3A. GOLD: SESSIONIZATION
        # ---------------------------------------------------------
        # Partition by user and order by time
        window_user_time = Window.partitionBy("user_id").orderBy("measurement_time")

        # Calculate time difference from the previous test
        sessions_base = silver_df.withColumn(
            "prev_measurement_time", lag("measurement_time").over(window_user_time)
        ).withColumn(
            "time_gap_seconds", 
            unix_timestamp("measurement_time") - unix_timestamp("prev_measurement_time")
        )

        # Rule: If gap > 30 mins (1800 secs) OR it's the first test (prev_time is null), it's a new session (1)
        sessions_flagged = sessions_base.withColumn(
            "is_new_session",
            when(col("time_gap_seconds") > (2*1800), 1)
            .when(col("prev_measurement_time").isNull(), 1)
            .otherwise(0)
        )

        # Cumulative sum of the new_session flags creates a unique ID per user
        # We concat user_id to make it globally unique across the whole dataset
        sessionized_df = sessions_flagged.withColumn(
            "user_session_seq", _sum("is_new_session").over(window_user_time)
        ).withColumn(
            "session_id", concat_ws("-", col("user_id"), col("user_session_seq"))
        )

        # Aggregate down to the Session Level
        gold_user_sessions_base = sessionized_df.groupBy("session_id", "user_id").agg(
            expr("min(measurement_time)").alias("session_start_time"),
            expr("max(measurement_time)").alias("session_end_time"),
            count("measurement_id").alias("total_tests_in_session"),
            expr("avg(measured_downstream_mbps)").alias("avg_session_downstream_mbps")
        )

        window_session_gap = Window.partitionBy("user_id").orderBy("session_start_time")

        # Add the time_since_last_session column (calculated in seconds for analytical ease)
        gold_user_sessions = gold_user_sessions_base.withColumn(
            "prev_session_start_time", 
            lag("session_start_time").over(window_session_gap)
        ).withColumn(
            # We calculate the difference in seconds. 
            # If you prefer native Spark Intervals (Spark 3.3+), you can simply do: 
            # col("session_start_time") - col("prev_session_start_time")
            "time_since_last_session_seconds",
            unix_timestamp("session_start_time") - unix_timestamp("prev_session_start_time")
        ).drop("prev_session_start_time")

        # -> WRITE TO BIGQUERY OR GCS: my_project.silver.measurements
        target_table = f"{dataset}.gold_sessionization"

        gold_user_sessions.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .option("table", target_table) \
            .mode("overwrite") \
            .save()
            
        logger.info(f"{target_table} successfully written to BigQuery.")

        # ---------------------------------------------------------
        # 3B. GOLD: LINEAR REGRESSION SLOPE (Speeds over Time)
        # ---------------------------------------------------------
        # Convert timestamp to a numeric value (Unix Epoch) to act as our 'X' axis
        #trend_base_df = silver_df.withColumn("time_x", unix_timestamp("measurement_time"))

        #gold_user_speed_trends = trend_base_df.groupBy("user_id").agg(
        #    count("measurement_id").alias("total_tests"),
        #    expr("min(measurement_time)").alias("first_test_time"),
        #    expr("max(measurement_time)").alias("last_test_time"),
        #    covar_pop("measured_downstream_mbps", "time_x").alias("covar_xy"),
        #    var_pop("time_x").alias("var_x")
        #)

        # Filter for users with >1 test and variance > 0 (to avoid division by zero)
        #gold_user_speed_trends = gold_user_speed_trends \
        #    .filter("total_tests > 1 AND var_x > 0") \
        #    .withColumn("download_slope_mbps_per_sec", col("covar_xy") / col("var_x")) \
        #    .drop("covar_xy", "var_x")

        # Note: The slope represents "Change in Mbps per Second". 
        # Multiply by 86400 if you want to view it as "Change in Mbps per Day".
        #gold_user_speed_trends = gold_user_speed_trends.withColumn(
        #    "download_slope_mbps_per_day", col("download_slope_mbps_per_sec") * 86400
        #)

        # -> WRITE TO BIGQUERY OR GCS: my_project.silver.measurements
        #target_table = f"{dataset}.gold_user_speed_trends"

        #gold_user_speed_trends.write \
        #    .format("bigquery") \
        #    .option("writeMethod", "indirect") \
        #    .option("temporaryGcsBucket", bucket_name) \
        #    .option("table", target_table) \
        #    .mode("overwrite") \
        #    .save()
            
        #logger.info(f"{target_table} successfully written to BigQuery.")

        # ---------------------------------------------------------
        # 3C. GOLD: REGIONAL METRICS (Daily)
        # ---------------------------------------------------------
        gold_regional_metrics = silver_df \
            .groupBy("measurement_date", "connection_periphery") \
            .agg(
                count("measurement_id").alias("total_tests"),
                expr("avg(measured_downstream_mbps)").alias("avg_downstream_mbps"),
                #expr("percentile_approx(measured_downstream_mbps, 0.1)").alias("p10_downstream_mbps"),
                expr("percentile_approx(measured_downstream_mbps, 0.5)").alias("p50_downstream_mbps"),
                #expr("percentile_approx(measured_downstream_mbps, 0.9)").alias("p90_downstream_mbps"),
                expr("avg(measured_upstream_mbps)").alias("avg_upstream_mbps"),
                #expr("percentile_approx(measured_upstream_mbps, 0.1)").alias("p10_upstream_mbps"),
                #expr("percentile_approx(measured_upstream_mbps, 0.5)").alias("p50_upstream_mbps"),
                #expr("percentile_approx(measured_upstream_mbps, 0.9)").alias("p90_upstream_mbps")
                expr("avg(measured_rtt_msec)").alias("avg_rtt_msec"),
                expr("avg(measured_loss_percentage)").alias("avg_loss_percentage"),
                expr("avg(measured_jitter_msec)").alias("avg_jitter_msec"),
            )

        # -> WRITE TO BIGQUERY OR GCS: my_project.silver.measurements
        target_table = f"{dataset}.gold_regional_metrics"

        gold_regional_metrics.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .option("table", target_table) \
            .mode("overwrite") \
            .save()
            
        logger.info(f"{target_table} successfully written to BigQuery.")


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