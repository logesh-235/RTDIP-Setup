from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from rtdip_sdk import RTDIPClient  # RTDIP SDK import

# Initialize RTDIP Client for MinIO interaction
rtdip_client = RTDIPClient(
    storage_type="minio",
    endpoint="minio:9000",  # MinIO endpoint
    access_key="minioadmin",  # MinIO access key
    secret_key="minioadmin",  # MinIO secret key
    bucket_name="historian-data"  # Bucket in MinIO to save data
)

# Initialize Spark Session with Delta and MinIO
spark = (
    SparkSession.builder.appName("OPCUA-Kafka-Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Kafka & Delta configuration
KAFKA_BROKER = "kafka:9092"
TOPIC = "OPCUA"
DELTA_PATH = "s3a://historian-data/opcua_metrics"
CHECKPOINT_PATH = "s3a://historian-data/checkpoints/opcua-streaming"

# Define incoming JSON schema
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("node_id", StringType()),
    StructField("display_name", StringType()),
    StructField("value", StringType()),
    StructField("source_timestamp", StringType()),
    StructField("quality", StringType())
])

# Read messages from Kafka
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_value = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

# Parse JSON and select required columns
df_json = (
    df_value
    .select(from_json(col("json_data"), schema).alias("data"))
    .select(
        col("data.node_id").alias("nodeId"),
        col("data.display_name").alias("displayName"),
        col("data.value").alias("value"),
        col("data.timestamp").alias("timestamp"),
        col("data.source_timestamp").alias("sourceTimestamp"),
        col("data.quality").alias("quality")
    )
)

# Function to write data to MinIO using RTDIP SDK
def write_to_minio(batch_df, batch_id):
    # Collect the data to a list (it could be optimized for large batches)
    records = batch_df.collect()

    # Convert the records into the format required by RTDIP SDK (e.g., a dictionary or JSON)
    data = []
    for row in records:
        data.append({
            "nodeId": row.nodeId,
            "displayName": row.displayName,
            "value": row.value,
            "timestamp": row.timestamp,
            "sourceTimestamp": row.sourceTimestamp,
            "quality": row.quality
        })
    
    # Use RTDIP SDK to store the processed data to MinIO
    rtdip_client.store_data(data)

# Write the processed data to MinIO
query = (
    df_json.writeStream
    .foreachBatch(write_to_minio)  # Use foreachBatch to write to MinIO after processing
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
