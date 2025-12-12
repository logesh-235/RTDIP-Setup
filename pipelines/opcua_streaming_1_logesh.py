from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# -------------------------------------------------------------------------
# 1️⃣ Initialize Spark Session (with Delta + MinIO / S3A support)
# -------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("OPCUA-Kafka-Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # 🔹 MinIO (S3-compatible storage) configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------------------
# 2️⃣ Kafka & Delta Configuration
# -------------------------------------------------------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "OPCUA"
DELTA_PATH = "s3a://historian-data/opcua_metrics"
CHECKPOINT_PATH = "s3a://historian-data/checkpoints/opcua-streaming"

# -------------------------------------------------------------------------
# 3️⃣ Define schema matching your OPC UA JSON message
# -------------------------------------------------------------------------
# Your messages look like:
# {
#   "timestamp": "...",
#   "node_id": "...",
#   "display_name": "...",
#   "value": ...,
#   "source_timestamp": "..."
# }

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("node_id", StringType()),
    StructField("display_name", StringType()),
    StructField("value", StringType()),
    StructField("source_timestamp", StringType())
])

# -------------------------------------------------------------------------
# 4️⃣ Read messages from Kafka topic
# -------------------------------------------------------------------------
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")   # change to "earliest" for initial backfill
    .load()
)

# Convert Kafka value (bytes) to string
df_value = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

# Parse JSON and normalize field names
df_json = (
    df_value
    .select(from_json(col("json_data"), schema).alias("data"))
    .select(
        col("data.node_id").alias("nodeId"),
        col("data.display_name").alias("displayName"),
        col("data.value").alias("value"),
        col("data.timestamp").alias("timestamp"),
        col("data.source_timestamp").alias("sourceTimestamp")
    )
    .filter(col("nodeId").isNotNull())
)

# -------------------------------------------------------------------------
# 5️⃣ Write parsed stream to Delta Lake on MinIO
# -------------------------------------------------------------------------
query = (
    df_json.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(DELTA_PATH)
)

query.awaitTermination()
