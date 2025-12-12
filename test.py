from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from rtdip_sdk.pipelines.sources.spark.kafka import SparkKafkaSource

# ------------------------------
# 1️⃣ Create Spark Session with MinIO Configurations
# ------------------------------
spark = SparkSession.builder \
    .appName("RTDIP Kafka to MinIO Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.hadoop.fs.s3a.access.key", "your-access-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  # MinIO URL
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")  # Max connections
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  # MinIO S3 compatibility
    .getOrCreate()

# Set log level to show only warnings or higher
spark.sparkContext.setLogLevel("WARN")

# ------------------------------
# 2️⃣ Kafka Options
# ------------------------------
kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",  # Change to your Kafka server
    "subscribe": "OPCUA",  # Change to your Kafka topic
    "startingOffsets": "earliest"
}

# ------------------------------
# 3️⃣ Create RTDIP Kafka Source
# ------------------------------
kafka_source = SparkKafkaSource(spark=spark, options=kafka_options)

# ------------------------------
# 4️⃣ Read Stream from Kafka
# ------------------------------
df_stream = kafka_source.read_stream()

# Kafka messages are in 'key' and 'value' columns as binary
# Convert 'value' to string
df_stream = df_stream.withColumn("value_str", col("value").cast("string"))

# Optional: show timestamp, key, and value
df_stream = df_stream.select(
    col("timestamp"),
    col("key").cast("string").alias("key_str"),
    col("value_str")
)

# ------------------------------
# 5️⃣ Write to MinIO (S3-compatible storage)
# ------------------------------
# Specify MinIO bucket and path
output_path = "s3a://your-bucket-name/path/to/store/data/"

# Write the DataFrame to MinIO in Parquet format (can change to CSV, JSON, etc.)
df_stream.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

# Wait for the query to terminate
df_stream.awaitTermination()
