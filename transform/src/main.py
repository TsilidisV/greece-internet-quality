import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    print(">>> 🚀 Starting Spark Smoke Test...")

    # 1. Initialize Spark Session
    # CRITICAL: We explicitly limit memory to ensure we don't crash the container.
    spark = (SparkSession.builder
        .appName("DailyETL_Test")
        .config("spark.driver.memory", "512m")   # Safe limit for Free Tier
        .config("spark.executor.memory", "512m") # Safe limit for Free Tier
        .config("spark.ui.enabled", "false")     # Disable UI to save CPU
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate())

    print(f">>> ✅ Spark Version: {spark.version}")

    # 2. Create Dummy Data
    # We create data manually instead of reading GCS to isolate connection errors.
    data = [
        ("Widget A", 100),
        ("Widget B", 200),
        ("Widget C", 300),
        ("Widget A", 50)
    ]
    columns = ["product", "sales"]
    
    print(">>> 📊 Creating DataFrame...")
    df = spark.createDataFrame(data, columns)

    # 3. Perform a Transformation
    # Group by product and sum sales (The "Hello World" of Spark)
    print(">>> 🔄 Transforming Data...")
    df_transformed = df.groupBy("product").sum("sales").withColumnRenamed("sum(sales)", "total_sales")

    # 4. Show Results to Logs
    print(">>> 📉 Result:")
    df_transformed.show()

    # 5. Stop Spark
    spark.stop()
    print(">>> 🏁 Job Finished Successfully.")

if __name__ == "__main__":
    main()