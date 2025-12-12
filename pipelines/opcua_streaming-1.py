from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# -------------------------------------------------------------------------
# 1️⃣ Initialize Spark Session (with Delta + MinIO / S3A support)
# -------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("OPCUA-Kafka-Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO env
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 🔥 Enable Delta schema auto-merge
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -------------------------------------------------------------------------
# 2️⃣ Kafka & Delta Configuration
# -------------------------------------------------------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "OPCUA"
DELTA_PATH = "s3a://historian-data/opcua_metrics"
CHECKPOINT_PATH = "s3a://historian-data/checkpoints/opcua-streaming"

# -------------------------------------------------------------------------
# 3️⃣ Define incoming JSON schema (now includes quality)
# -------------------------------------------------------------------------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("node_id", StringType()),
    StructField("display_name", StringType()),
    StructField("value", StringType()),
    StructField("source_timestamp", StringType()),
    StructField("quality", StringType())  # NEW
])

# -------------------------------------------------------------------------
# 4️⃣ Read messages from Kafka
# -------------------------------------------------------------------------
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_value = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

df_json = (
    df_value
    .select(from_json(col("json_data"), schema).alias("data"))
    .select(
        col("data.node_id").alias("nodeId"),
        col("data.display_name").alias("displayName"),
        col("data.value").alias("value"),
        col("data.timestamp").alias("timestamp"),
        col("data.source_timestamp").alias("sourceTimestamp"),
        col("data.quality").alias("quality")  # will be NULL if missing
    )
)

# -------------------------------------------------------------------------
# 5️⃣ Write parsed stream to Delta Lake with SCHEMA MERGE
# -------------------------------------------------------------------------
query = (
    df_json.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")          # ⭐ REQUIRED
    .outputMode("append")
    .start(DELTA_PATH)
)

query.awaitTermination()
