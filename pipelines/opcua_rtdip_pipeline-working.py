
# opcua_rtdip_pipeline.patched_filters.py
# Writes OPC UA data to MinIO with hierarchy: Enterprise / Site / ProductionLine / Equipment / Tag / Raw data
# Adds: Tag aliasing + filtering to avoid Diagnostics and unknown node_id-based folders.
#
# How it decides Tag:
# 1) If display_name like 'PLC_S7_200_SMART.Compressor.<TAG>' -> use <TAG>
# 2) Else if node_id like 'ns=2;s=PLC_S7_200_SMART.Compressor.<TAG>' -> use <TAG>
# 3) Else take the last segment after the final '.' from display_name or node_id (as fallback)
# 4) Sanitizes tag to safe folder name (alphanum + '_')
#
# Filtering:
# - Drops Diagnostics (node_id containing '.Diagnostics.' or tag starting with 'Diagnostics')
# - Optionally restricts to ALLOWED_TAGS list (CSV in env)
#
import os
import sys
import time
import json
import threading
import logging
from pyspark.sql.functions import (
    col, to_timestamp, to_date, when, lit, get_json_object, current_timestamp, expr, trim,
    regexp_extract, coalesce, lower, regexp_replace, split, size
)
from rtdip_sdk.pipelines.utilities import SparkSessionUtility
from rtdip_sdk.pipelines.sources import SparkKafkaSource
from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer

logging.basicConfig(
    level=os.getenv("PY_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("opcua-rtdip-pipeline-filters")

# --- ENV ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "OPCUA")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000")
SPARK_SHUFFLE = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
TS_MAX_FUTURE_SEC = int(os.getenv("TS_MAX_FUTURE_SEC", "600"))

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rtdip")
ENTERPRISE = os.getenv("ENTERPRISE", "Utthunga")
SITE = os.getenv("SITE", "Site_name")
PRODUCTION_LINE_FALLBACK = os.getenv("PRODUCTION_LINE", "Line-1")
EQUIPMENT_FALLBACK = os.getenv("EQUIPMENT_NAME", "Compressor")
HIER_PATH = os.getenv("HIER_PATH", f"s3a://{MINIO_BUCKET}/{ENTERPRISE}/{SITE}")
CHECKPOINT_HIER = os.getenv("CHECKPOINT_HIER", "s3a://checkpoints/opcua-rtdip/hierarchy")
ALLOWED_TAGS = os.getenv("ALLOWED_TAGS", ",".join([
    "Comp_Pressure",
    "Comp_Motor_Power",
    "Comp_Motor_Speed",
    "Comp_Motor_Current_A",
    "Comp_Motor_Current_B",
    "Comp_Motor_Current_C",
    "Comp_Motor_Voltage",
    "Comp_Supply_Voltage",
    "Comp_Air_run_time",
    "Comp_Oil_run_time",
    "Comp_Grease_run_time",
])).strip()

allowed_list = [t.strip() for t in ALLOWED_TAGS.split(',') if t.strip()]

log.info("=== Environment ===")
for k in [
    "KAFKA_BOOTSTRAP_SERVERS","KAFKA_TOPIC","KAFKA_STARTING_OFFSETS",
    "AWS_S3_ENDPOINT","AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY",
    "MINIO_BUCKET","ENTERPRISE","SITE","PRODUCTION_LINE","EQUIPMENT_NAME",
    "HIER_PATH","CHECKPOINT_HIER","ALLOWED_TAGS"
]:
    log.info(f"{k}={os.getenv(k)}")

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
    # Timestamp parsing behavior
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    # Shuffle tuning
    "spark.sql.shuffle.partitions": SPARK_SHUFFLE,
}).execute()
spark.sparkContext.setLogLevel("WARN")
log.info("Spark version: %s", spark.version)

# --- Kafka streaming source ---
log.info("=== Kafka source ===")
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

# --- Binary -> String ---
log.info("=== Binary->String transform ===")
json_df = BinaryToStringTransformer(
    data=src_df, source_column_name="value", target_column_name="json"
).transform()

# --- Parse JSON + Tag aliasing ---
log.info("=== Parse JSON & alias Tag ===")
parsed_base = json_df.select(
    "json",
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

# Derive Tag from DisplayName/NodeId
# Prefer explicit Compressor path
tag_from_dn = regexp_extract(col("DisplayName"), r"^PLC_S7_200_SMART\.Compressor\.(.+)$", 1)
tag_from_id = regexp_extract(col("NodeId"), r"^ns=\d+;s=PLC_S7_200_SMART\.Compressor\.(.+)$", 1)
# Fallback: last token after '.' from display_name or node_id
last_from_dn = regexp_extract(col("DisplayName"), r"([^\.]+)$", 1)
last_from_id = regexp_extract(col("NodeId"), r"([^\.]+)$", 1)

TagRaw = coalesce(tag_from_dn, tag_from_id, last_from_dn, last_from_id)
# Sanitize tag for folder names (replace non-alnum with '_')
TagClean = regexp_replace(TagRaw, r"[^A-Za-z0-9_]+", "_")

# Timestamps
source_ts = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")
ingest_ts = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
server_ts = to_timestamp(col("ServerTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")

parsed = parsed_base.withColumn(
    "Tag", TagClean
).withColumn(
    "EventTime",
    when(source_ts.isNotNull(), source_ts)
    .otherwise(when(ingest_ts.isNotNull(), ingest_ts).otherwise(current_timestamp()))
).withColumn(
    "EventTime",
    when(col("EventTime") <= (current_timestamp() + expr(f"INTERVAL {TS_MAX_FUTURE_SEC} SECONDS")), col("EventTime"))
    .otherwise(current_timestamp())
).withColumn(
    "EventDate", to_date(col("EventTime"))
)

# Drop Diagnostics
not_diag = (~col("NodeId").rlike("\\.Diagnostics\\.") & ~lower(col("Tag")).startswith("diagnostics"))
filtered = parsed.filter(not_diag)

# Restrict to ALLOWED_TAGS if provided
if allowed_list:
    from pyspark.sql.functions import array, lit
    # Create boolean for membership (simple OR chain)
    cond = None
    for t in allowed_list:
        c = (col("Tag") == lit(t))
        cond = c if cond is None else (cond | c)
    filtered = filtered.filter(cond)

# Build hierarchy columns
hier_df = (
    filtered
    .withColumn("Enterprise", lit(ENTERPRISE))
    .withColumn("Site", lit(SITE))
    .withColumn("ProductionLine", when(col("ProductionLineJson").isNotNull(), col("ProductionLineJson")).otherwise(lit(PRODUCTION_LINE_FALLBACK)))
    .withColumn("Equipment", when(col("EquipmentJson").isNotNull(), col("EquipmentJson")).otherwise(lit(EQUIPMENT_FALLBACK)))
    .select("Enterprise","Site","ProductionLine","Equipment","Tag","RawData","StatusCode","EventTime","EventDate")
)

# Watermark for late data metrics
hier_df = hier_df.withWatermark("EventTime", "10 minutes")

# Write to MinIO (Delta) following hierarchy partitions
log.info("=== Start hierarchy sink -> MinIO (Delta) ===")
hier_query = (
    hier_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_HIER)
    .outputMode("append")
    .partitionBy("Enterprise","Site","ProductionLine","Equipment","Tag")
    .trigger(processingTime="10 seconds")
    .queryName("OPCUA_MINIO_HIERARCHY")
    .start(HIER_PATH)
)

# Monitor
log.info("=== Start query monitor ===")

def monitor_queries():
    while True:
        try:
            for q in spark.streams.active:
                try:
                    progress = q.lastProgress
                    if progress:
                        log.info("STREAM PROGRESS (%s): %s", q.name(), json.dumps(progress))
                    else:
                        log.info("STREAM (%s): idle", q.name())
                except Exception as inner:
                    log.debug("Monitor could not read stats: %s", inner)
            time.sleep(10)
        except Exception as outer:
            log.warning("Query monitor loop exception: %s", outer)
            time.sleep(10)

threading.Thread(target=monitor_queries, daemon=True).start()

log.info("=== Await termination ===")
spark.streams.awaitAnyTermination()
