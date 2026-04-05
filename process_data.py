import os

os.environ['HADOOP_HOME'] = 'C:\\hadoop'

os.environ['PATH'] = os.environ.get('PATH', '') + os.pathsep + 'C:\\hadoop\\bin'


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour


print("Starting Spark Initialization...")

spark = SparkSession.builder \
    .appName("NYCTaxiProcessing") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .getOrCreate()

print("Spark Session Created Successfully!")

raw_path = "s3a://taxi-data/raw/yellow_tripdata_2026-02.parquet"

print(f"Reading data from {raw_path}...")
df = spark.read.parquet(raw_path)

print("Cleaning data...")
cleaned_df = df.filter((col("passenger_count") > 0) & (col("trip_distance") > 0))

print("Writing to Silver layer...")
cleaned_df.write.mode("overwrite").parquet("s3a://taxi-data/silver/cleaned_taxi_data.parquet")

print("Aggregating data for analysis...")
hourly_demand = cleaned_df.withColumn("hour", hour("tpep_pickup_datetime")) \
                          .groupBy("hour") \
                          .count() \
                          .orderBy("hour")

print("Writing to Gold layer...")
hourly_demand.write.mode("overwrite").parquet("s3a://taxi-data/gold/hourly_demand.parquet")


print("==========================================")
print("Processing Complete! Check MinIO dashboard.")
print("==========================================")

spark.stop()