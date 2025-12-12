import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from rtdip_sdk.pipelines.sources.interfaces import SourceInterface
from rtdip_sdk.pipelines.destinations.interfaces import DestinationInterface

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger("rtdip_kafka_delta_pipeline")

# -------------------------------
# Spark setup with Delta + MinIO
# -------------------------------
spark = (
    SparkSession.builder.appName("RTDIP Kafka → Delta")
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
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -------------------------------
# Kafka / Delta configuration
# -------------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "OPCUA"
DELTA_PATH = "s3a://historian-data/opcua_metrics"
CHECKPOINT_PATH = "s3a://historian-data/checkpoints/opcua-streaming"

# -------------------------------
# JSON schema
# -------------------------------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("node_id", StringType()),
    StructField("display_name", StringType()),
    StructField("value", StringType()),
    StructField("source_timestamp", StringType()),
    StructField("quality", StringType())
])

# -------------------------------
# Kafka Source (RTDIP SDK)
# -------------------------------
class KafkaSource(SourceInterface):
    @property
    def libraries(self):
        return ["pyspark", "kafka-python"]

    @property
    def settings(self):
        return {"kafka_broker": KAFKA_BROKER, "kafka_topic": TOPIC}

    @property
    def system_type(self):
        return "Kafka"

    def pre_read_validation(self) -> bool:
        logger.info("KafkaSource: Pre-read validation")
        return True

    def post_read_validation(self) -> bool:
        logger.info("KafkaSource: Post-read validation")
        return True

    def read_stream(self):
        logger.info("KafkaSource: Reading from Kafka stream")
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
                col("data.quality").alias("quality")
            )
        )
        return df_json

    def read_batch(self):
        logger.warning("KafkaSource: Batch read not implemented")
        return None

# -------------------------------
# Delta Destination (RTDIP SDK)
# -------------------------------
class DeltaDestination(DestinationInterface):
    @property
    def libraries(self):
        return ["pyspark", "delta-spark"]

    @property
    def settings(self):
        return {"delta_path": DELTA_PATH}

    @property
    def system_type(self):
        return "Delta"

    def pre_write_validation(self) -> bool:
        logger.info("DeltaDestination: Pre-write validation")
        return True

    def post_write_validation(self) -> bool:
        logger.info("DeltaDestination: Post-write validation")
        return True

    def write_stream(self, df):
        logger.info("DeltaDestination: Writing stream to Delta")
        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .option("mergeSchema", "true")
            .outputMode("append")
            .start(DELTA_PATH)
        )
        return query

    def write_batch(self, df):
        logger.warning("DeltaDestination: Batch write not implemented")
        pass

# -------------------------------
# Run the pipeline
# -------------------------------
def run_pipeline():
    logger.info("Starting RTDIP Kafka → Delta pipeline")

    # Source
    source = KafkaSource()
    source.pre_read_validation()
    df_stream = source.read_stream()
    source.post_read_validation()

    # Destination
    destination = DeltaDestination()
    destination.pre_write_validation()
    query = destination.write_stream(df_stream)
    destination.post_write_validation()

    logger.info("Pipeline running... Press Ctrl+C to stop")
    query.awaitTermination()


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
