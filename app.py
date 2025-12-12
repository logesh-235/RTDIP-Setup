from pyspark.sql import SparkSession
from rtdip_sdk.pipelines.sources.spark.kafka import SparkKafkaSource

# ------------------------------
# 1️⃣ Create Spark Session with Kafka Configurations
# ------------------------------
spark = SparkSession.builder \
    .appName("RTDIP Kafka Read Raw Data") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Set log level to show only warnings or higher
spark.sparkContext.setLogLevel("WARN")

# ------------------------------
# 2️⃣ Kafka Options
# ------------------------------
kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",  # Change to your Kafka server
    "subscribe": "OPCUA",                    # Change to your Kafka topic
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

# ------------------------------
# 5️⃣ Print Raw Kafka Data (Key and Value in Binary)
# ------------------------------
# Directly print raw Kafka messages
# Just select the key, value, and timestamp to print them
df_stream = df_stream.select("timestamp", "key", "value")

# Write to console to see the raw data
query = df_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# Wait for the stream to finish
query.awaitTermination()
