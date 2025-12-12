from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ------------------------------------------------------------------------------
# 1️⃣ Initialize Spark Session
# ------------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("OPCUA-Kafka-Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# ------------------------------------------------------------------------------
# 2️⃣ Kafka Configuration
# ------------------------------------------------------------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "OPCUA"
DELTA_PATH = "s3a://historian-data/opcua_metrics"
CHECKPOINT_PATH = "s3a://checkpoints/opcua-streaming"

# ------------------------------------------------------------------------------
# 3️⃣ Define JSON schema for messages
#     (Adjust to match your OPC UA message format)
# ------------------------------------------------------------------------------
schema = StructType([
    StructField("nodeId", StringType()),
    StructField("displayName", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", TimestampType())
])

# ------------------------------------------------------------------------------
# 4️⃣ Read messages from Kafka
# ------------------------------------------------------------------------------
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Kafka message is in the 'value' column as bytes — decode to string
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

# Parse JSON messages
df_json = df_parsed.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# ------------------------------------------------------------------------------
# 5️⃣ Write to Delta Lake (MinIO)
# ------------------------------------------------------------------------------
query = (
    df_json.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(DELTA_PATH)
)

query.awaitTermination()
