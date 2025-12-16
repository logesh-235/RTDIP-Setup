
# opcua_rtdip_pipeline.py
# Kafka -> RTDIP PCDM (Events + Latest) -> Delta Lake on MinIO/S3A
# Bucket: rtdip
# Tables:
#   s3a://rtdip/pcdm/events_integer   (partitioned by EventYear/Month/Day/Hour; Value = long)
#   s3a://rtdip/pcdm/events_float     (partitioned by EventYear/Month/Day/Hour; Value = double)
#   s3a://rtdip/pcdm/events_string    (partitioned by EventYear/Month/Day/Hour; Value = string)
#   s3a://rtdip/pcdm/latest           (small, unpartitioned; latest value per TagName)

import os
import sys
import time
import json
import threading
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, get_json_object,
    current_timestamp, expr, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, LongType, DoubleType
)

# RTDIP SDK
from rtdip_sdk.pipelines.utilities import SparkSessionUtility
from rtdip_sdk.pipelines.sources import SparkKafkaSource
from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer
from rtdip_sdk.pipelines.destinations import SparkPCDMLatestToDeltaDestination

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=os.getenv("PY_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("opcua-rtdip-pipeline")

# -----------------------
# ENV (edit via docker-compose or env file)
# -----------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "OPCUA")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

# MinIO / S3A
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Bucket and Delta table roots (in rtdip)
DELTA_INTEGER_YMDH = os.getenv("DELTA_PCDM_INTEGER_YMDH", "s3a://rtdip/pcdm/events_integer")
DELTA_FLOAT_YMDH   = os.getenv("DELTA_PCDM_FLOAT_YMDH",   "s3a://rtdip/pcdm/events_float")
DELTA_STRING_YMDH  = os.getenv("DELTA_PCDM_STRING_YMDH",  "s3a://rtdip/pcdm/events_string")
DELTA_LATEST       = os.getenv("DELTA_PCDM_LATEST",       "s3a://rtdip/pcdm/latest")

# Streaming checkpoints (same bucket)
CHECKPOINT_EVENTS = os.getenv("CHECKPOINT_EVENTS", "s3a://rtdip/checkpoints/opcua-rtdip/events")
CHECKPOINT_LATEST = os.getenv("CHECKPOINT_LATEST", "s3a://rtdip/checkpoints/opcua-rtdip/latest")
CHECKPOINT_SUFFIX = os.getenv("CHECKPOINT_SUFFIX", "").strip()

MAX_OFFSETS   = os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000")
SPARK_SHUFFLE = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")

# Force new query paths if suffix present (handy for backfills/tests)
if CHECKPOINT_SUFFIX:
    CHECKPOINT_EVENTS = CHECKPOINT_EVENTS.rstrip("/") + "_" + CHECKPOINT_SUFFIX
    CHECKPOINT_LATEST = CHECKPOINT_LATEST.rstrip("/") + "_" + CHECKPOINT_SUFFIX
    if DELTA_LATEST.endswith("/latest"):
        DELTA_LATEST = DELTA_LATEST + "_" + CHECKPOINT_SUFFIX

# -----------------------
# Spark Session
# -----------------------
log.info("=== Stage 1: Spark Session ===")
spark = SparkSessionUtility(config={
    # Delta
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # MinIO / S3A
    "spark.hadoop.fs.s3a.endpoint": AWS_S3_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    # Performance
    "spark.hadoop.fs.s3a.connection.maximum": "300",
    "spark.hadoop.fs.s3a.threads.max": "96",
    "spark.hadoop.fs.s3a.experimental.input.fadvise": "normal",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",

    # Robustness
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "100s",

    # Shuffle tuning
    "spark.sql.shuffle.partitions": SPARK_SHUFFLE,
}).execute()
spark.sparkContext.setLogLevel("WARN")
log.info("Spark version: %s", spark.version)

# -----------------------
# Ensure Delta tables exist (partitioned schema for events_*; unpartitioned for latest)
# -----------------------
log.info("=== Stage 2: Ensure Delta tables exist ===")

def ensure_delta_table(path: str, schema: StructType, partition_cols=None):
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        log.info("Delta table already exists at: %s", path)
    except Exception as e:
        log.info("Creating Delta table at: %s (reason: %s)", path, str(e))
        empty_df = spark.createDataFrame([], schema)
        writer = empty_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        log.info("Delta table created at: %s", path)

# Latest schema (small, unpartitioned)
latest_schema = StructType([
    StructField("TagName",        StringType(),   False),
    StructField("EventTime",      TimestampType(), True),
    StructField("Status",         StringType(),   True),
    StructField("Value",          StringType(),   True),
    StructField("ValueType",      StringType(),   True),
    StructField("GoodEventTime",  TimestampType(), True),
    StructField("GoodValue",      StringType(),   True),
    StructField("GoodValueType",  StringType(),   True),
])
ensure_delta_table(DELTA_LATEST, latest_schema, None)

# Events base schema (partition columns present; Value stored as string, cast per table on write)
events_partition_schema = StructType([
    StructField("TagName",    StringType(),    False),
    StructField("EventTime",  TimestampType(), True),
    StructField("Status",     StringType(),    True),
    StructField("Value",      StringType(),    True),
    StructField("EventYear",  IntegerType(),   True),
    StructField("EventMonth", IntegerType(),   True),
    StructField("EventDay",   IntegerType(),   True),
    StructField("EventHour",  IntegerType(),   True),
])
ensure_delta_table(DELTA_INTEGER_YMDH, events_partition_schema,
                   ["EventYear","EventMonth","EventDay","EventHour"])
ensure_delta_table(DELTA_FLOAT_YMDH,   events_partition_schema,
                   ["EventYear","EventMonth","EventDay","EventHour"])
ensure_delta_table(DELTA_STRING_YMDH,  events_partition_schema,
                   ["EventYear","EventMonth","EventDay","EventHour"])

# -----------------------
# Kafka streaming source
# -----------------------
log.info("=== Stage 3: Create Kafka streaming source ===")
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
log.info("Kafka source created. Columns=%s", src_df.columns)

# -----------------------
# Binary -> String
# -----------------------
log.info("=== Stage 4: Binary->String transform ===")
json_df = BinaryToStringTransformer(
    data=src_df, source_column_name="value", target_column_name="json"
).transform()

# -----------------------
# Parse JSON -> events_df + robust ValueType + UTC partitions
# -----------------------
log.info("=== Stage 5: Parse JSON + Build events_df (robust time/type) ===")
parsed_base = json_df.select(
    "json",
    get_json_object(col("json"), "$.display_name").alias("TagName"),
    get_json_object(col("json"), "$.value").alias("ValueStr"),
    get_json_object(col("json"), "$.timestamp").alias("IngestTimeStr"),
    get_json_object(col("json"), "$.source_timestamp").alias("SourceTimeStr"),
    get_json_object(col("json"), "$.quality").alias("Quality"),
)

# Timestamp parsing: try source first (with/without offset), then ingest, else current
src_ts_iso_offset = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")
src_ts_iso        = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
src_ts_plain      = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd HH:mm:ss")

ing_ts_iso_offset = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")
ing_ts_iso        = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
ing_ts_plain      = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd HH:mm:ss")

event_local = F.coalesce(src_ts_iso_offset, src_ts_iso, src_ts_plain,
                         ing_ts_iso_offset, ing_ts_iso, ing_ts_plain,
                         current_timestamp())

# Normalize to UTC and use UTC for partition derivation
event_utc = F.to_utc_timestamp(event_local, "UTC")

# Numeric detection on plain string (handles "123", "123.45", with/without spaces)
IS_INT_REGEX   = r'^\s*-?\d+\s*$'
IS_FLOAT_REGEX = r'^\s*-?\d+(\.\d+)?\s*$'
val_trim       = F.trim(F.col("ValueStr"))
is_int         = val_trim.rlike(IS_INT_REGEX)
is_float       = val_trim.rlike(IS_FLOAT_REGEX)

events_df = (
    parsed_base
    .withColumn("EventTime", event_utc)
    .withColumn("Status", when(col("Quality").isNotNull(), col("Quality")).otherwise(lit("None")))
    .withColumn("Value",  val_trim)  # keep original as string; cast in writer per table
    .withColumn("ValueType",
        when(is_int,   lit("integer"))
         .when(is_float, lit("float"))
         .otherwise(lit("string"))
    )
    .withColumn("EventYear",  year(col("EventTime")))
    .withColumn("EventMonth", month(col("EventTime")))
    .withColumn("EventDay",   dayofmonth(col("EventTime")))
    .withColumn("EventHour",  hour(col("EventTime")))
    .select("TagName","EventTime","Status","Value","ValueType",
            "EventYear","EventMonth","EventDay","EventHour")
)
events_df.printSchema()

# -----------------------
# foreachBatch writer (type-correct casts + Y/M/D/H partitions)
# -----------------------
log.info("=== Stage 6: Start events stream -> Delta (Y/M/D/H partitions) ===")

def write_events_batch(batch_df, batch_id):
    batch_df = batch_df.persist()

    # INTEGER: cast Value -> long
    try:
        bi = (batch_df.where(F.col("ValueType") == "integer")
                       .withColumn("Value", F.col("Value").cast(LongType()))
                       .select("TagName","EventTime","Status","Value",
                               "EventYear","EventMonth","EventDay","EventHour"))
        if bi.head(1):
            (bi.write.format("delta")
                .option("mergeSchema","true")
                .mode("append")
                .partitionBy("EventYear","EventMonth","EventDay","EventHour")
                .save(DELTA_INTEGER_YMDH))
    except Exception as e:
        log.error("Error writing INTEGER batch: %s", e)

    # FLOAT: cast Value -> double
    try:
        bf = (batch_df.where(F.col("ValueType") == "float")
                       .withColumn("Value", F.col("Value").cast(DoubleType()))
                       .select("TagName","EventTime","Status","Value",
                               "EventYear","EventMonth","EventDay","EventHour"))
        if bf.head(1):
            (bf.write.format("delta")
                .option("mergeSchema","true")
                .mode("append")
                .partitionBy("EventYear","EventMonth","EventDay","EventHour")
                .save(DELTA_FLOAT_YMDH))
    except Exception as e:
        log.error("Error writing FLOAT batch: %s", e)

    # STRING: keep Value as string
    try:
        bs = (batch_df.where(F.col("ValueType") == "string")
                       .withColumn("Value", F.col("Value").cast(StringType()))
                       .select("TagName","EventTime","Status","Value",
                               "EventYear","EventMonth","EventDay","EventHour"))
        if bs.head(1):
            (bs.write.format("delta")
                .option("mergeSchema","true")
                .mode("append")
                .partitionBy("EventYear","EventMonth","EventDay","EventHour")
                .save(DELTA_STRING_YMDH))
    except Exception as e:
        log.error("Error writing STRING batch: %s", e)

events_query = (
    events_df.writeStream
    .option("checkpointLocation", CHECKPOINT_EVENTS)
    .queryName("OPCUA_PCDM_EVENTS_YMDH")
    .trigger(processingTime="10 seconds")
    .foreachBatch(write_events_batch)
    .start()
)

# -----------------------
# Latest -> Delta (MERGE)
# -----------------------
log.info("=== Stage 7: Start PCDM Latest stream -> Delta (MERGE) ===")
latest_query = SparkPCDMLatestToDeltaDestination(
    spark=spark,
    data=events_df.select("TagName","EventTime","Status","Value","ValueType"),
    options={"checkpointLocation": CHECKPOINT_LATEST},
    destination=DELTA_LATEST,
    mode="append",
    trigger="10 seconds",
    query_name="OPCUA_PCDM_LATEST",
    query_wait_interval=10,
).write_stream()

# -----------------------
# Monitor
# -----------------------
def monitor_queries():
    while True:
        try:
            for q in spark.streams.active:
                try:
                    progress = q.lastProgress
                    status = q.status
                    exc = getattr(q, "exception", None)
                    if progress:
                        log.info("STREAM PROGRESS (%s): %s", q.name, json.dumps(progress))
                    else:
                        log.info("STREAM (%s): idle (no progress yet)", q.name)
                    if exc:
                        log.error("STREAM EXCEPTION (%s): %s", q.name, exc)
                except Exception as inner:
                    log.debug("Monitor could not read stats: %s", inner)
            time.sleep(10)
        except Exception as outer:
            log.warning("Query monitor loop exception: %s", outer)
            time.sleep(10)

log.info("=== Stage 8: Start query monitor ===")
threading.Thread(target=monitor_queries, daemon=True).start()

# -----------------------
# Await termination
# -----------------------
log.info("=== Stage 9: Await termination ===")
spark.streams.awaitAnyTermination()
