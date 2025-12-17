
# opcua_rtdip_pipeline.py
# Strict Compressor pipeline:
# - Filters ONLY ns=2;s=PLC_S7_200_SMART.Compressor.<TAG> NodeIds (your exact list)
# - Derives Tag from node_id (not display_name), removes spaces, sanitizes folder names
# - Writes Delta to MinIO partitioned by Enterprise/Site/ProductionLine/Equipment/Tag

import os, sys, time, json, threading, logging
from pyspark.sql.functions import (
    col, lit, trim, get_json_object, to_timestamp, current_timestamp, when, expr,
    to_date, regexp_extract, regexp_replace
)
from rtdip_sdk.pipelines.utilities import SparkSessionUtility
from rtdip_sdk.pipelines.sources import SparkKafkaSource
from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer

logging.basicConfig(level=os.getenv("PY_LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("opcua-rtdip-pipeline-strict")

# --- ENV ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "OPCUA")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "100000")  # tuned down to avoid falling behind
SPARK_SHUFFLE = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")

AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
TS_MAX_FUTURE_SEC = int(os.getenv("TS_MAX_FUTURE_SEC", "600"))

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rtdip")
ENTERPRISE = os.getenv("ENTERPRISE", "Utthunga")
SITE = os.getenv("SITE", "Site_name")
PRODUCTION_LINE = os.getenv("PRODUCTION_LINE", "Line-1")
EQUIPMENT = os.getenv("EQUIPMENT_NAME", "Compressor")
HIER_PATH = os.getenv("HIER_PATH", f"s3a://{MINIO_BUCKET}/{ENTERPRISE}/{SITE}")
CHECKPOINT_HIER = os.getenv("CHECKPOINT_HIER", "s3a://checkpoints/opcua-rtdip/hierarchy")

# Compressor prefix and exact tag names (as per your server)
COMPRESSOR_PREFIX = "ns=2;s=PLC_S7_200_SMART.Compressor."
ALLOWED_COMPRESSOR_TAGS = [
    "Comp_Air_run_time",
    "Comp_Fan_Current_A",
    "Comp_Fan_Current_B",
    "Comp_Fan_Current_C",
    "Comp_Freq",
    "Comp_Grease_run_time",
    "Comp_Lube_run_time",
    "Comp_Motor_Current",
    "Comp_Motor_Current_ A",
    "Comp_Motor_Current_ B",
    "Comp_Motor_Current_ C",
    "Comp_Motor_Power",
    "Comp_Motor_Speed",
    "Comp_Motor_this_elec",
    "Comp_Motor_total_elec",
    "Comp_Motor_Voltage",
    "Comp_OA_run_time",
    "Comp_Oil_run_time",
    "Comp_Pf_fan_phase_UI",
    "Comp_Pf_fan_this_elec",
    "Comp_Pf_total_elec",
    "Comp_Pressure",
    "Comp_Supply_Voltage",
]
ALLOWED_NODEIDS = [COMPRESSOR_PREFIX + t for t in ALLOWED_COMPRESSOR_TAGS]
log.info("STRICT whitelist count: %d", len(ALLOWED_NODEIDS))

# --- Spark Session ---
spark = SparkSessionUtility(config={
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # MinIO / S3A
    "spark.hadoop.fs.s3a.endpoint": AWS_S3_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    # Robustness
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "100s",
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.sql.shuffle.partitions": SPARK_SHUFFLE,
}).execute()
spark.sparkContext.setLogLevel("WARN")
log.info("Spark version: %s", spark.version)

# --- Kafka source ---
src_df = SparkKafkaSource(
    spark=spark,
    options={
        "kafka.bootstrap.servers": BOOTSTRAP,
        "subscribe": TOPIC,
        "startingOffsets": STARTING_OFFSETS,
        "failOnDataLoss": "false",
        "includeHeaders": "true",
        "maxOffsetsPerTrigger": MAX_OFFSETS,
    },
).read_stream()

json_df = BinaryToStringTransformer(
    data=src_df, source_column_name="value", target_column_name="json"
).transform()

# --- Parse and filter to whitelist ---
base = json_df.select(
    trim(get_json_object(col("json"), "$.display_name")).alias("DisplayName"),
    trim(get_json_object(col("json"), "$.node_id")).alias("NodeId"),
    get_json_object(col("json"), "$.value").alias("RawData"),
    get_json_object(col("json"), "$.status_code").alias("StatusCode"),
    get_json_object(col("json"), "$.source_timestamp").alias("SourceTimeStr"),
    get_json_object(col("json"), "$.timestamp").alias("IngestTimeStr"),
    get_json_object(col("json"), "$.server_timestamp").alias("ServerTimeStr"),
    get_json_object(col("json"), "$.production_line").alias("ProductionLineJson"),
    get_json_object(col("json"), "$.equipment_name").alias("EquipmentJson"),
)

from pyspark.sql.functions import lit
cond = None
for nid in ALLOWED_NODEIDS:
    c = (col("NodeId") == lit(nid))
    cond = c if cond is None else (cond | c)

filtered = base.filter(cond)

# --- Timestamps ---
source_ts = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")
ingest_ts = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
server_ts = to_timestamp(col("ServerTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")

parsed = filtered.withColumn(
    "EventTime",
    when(source_ts.isNotNull(), source_ts)
     .otherwise(when(ingest_ts.isNotNull(), ingest_ts).otherwise(current_timestamp()))
).withColumn(
    "EventTime",
    when(col("EventTime") <= (current_timestamp() + expr(f"INTERVAL {TS_MAX_FUTURE_SEC} SECONDS")), col("EventTime"))
     .otherwise(current_timestamp())
).withColumn("EventDate", to_date(col("EventTime")))

# --- Derive clean Tag from NodeId (not display_name) ---
TagRaw = regexp_extract(col("NodeId"), r"^ns=\d+;s=PLC_S7_200_SMART\.Compressor\.(.+)$", 1)
TagNoSpaces = regexp_replace(TagRaw, r"\s+", "")
TagClean = regexp_replace(TagNoSpaces, r"[^A-Za-z0-9_]+", "_")

# --- Hierarchy columns ---
hier_df = (
    parsed
    .withColumn("Enterprise", lit(ENTERPRISE))
    .withColumn("Site", lit(SITE))
    .withColumn("ProductionLine", when(col("ProductionLineJson").isNotNull(), col("ProductionLineJson")).otherwise(lit(PRODUCTION_LINE)))
    .withColumn("Equipment", when(col("EquipmentJson").isNotNull(), col("EquipmentJson")).otherwise(lit(EQUIPMENT)))
    .withColumn("Tag", TagClean)
    .select("Enterprise","Site","ProductionLine","Equipment","Tag","RawData","StatusCode","EventTime","EventDate")
)

hier_df = hier_df.withWatermark("EventTime", "10 minutes")

# --- Sink ---
query = (
    hier_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_HIER)
    .outputMode("append")
    .partitionBy("Enterprise","Site","ProductionLine","Equipment","Tag")
    .trigger(processingTime="20 seconds")
    .queryName("OPCUA_STRICT_COMPRESSOR")
    .start(HIER_PATH)
)

# Report any exception explicitly (so you see the root cause in logs)
try:
    query.awaitTermination()
except Exception as e:
    log.exception("Streaming query terminated with error: %s", e)

exc = query.exception()
if exc:
    log.error("Streaming query exception: %s", exc)
