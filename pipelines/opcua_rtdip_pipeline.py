
# opcua_rtdip_pipeline.py
# Kafka -> RTDIP PCDM (Events + Latest) -> Delta Lake on MinIO/S3A
# Fixed: EventDate partition column, robust EventTime, ValueType inference, append-only Events, MERGE Latest.

import os
import sys
import time
import json
import threading
import logging
from pyspark.sql.functions import (
    col, to_timestamp, to_date, when, lit, get_json_object, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

# RTDIP SDK
from rtdip_sdk.pipelines.utilities import SparkSessionUtility
from rtdip_sdk.pipelines.sources import SparkKafkaSource
from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer
from rtdip_sdk.pipelines.destinations import (
    SparkPCDMToDeltaDestination,
    SparkPCDMLatestToDeltaDestination,
)

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    level=os.getenv("PY_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("opcua-rtdip-pipeline")

# ----------------------------
# ENV
# ----------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "OPCUA")

# "latest" for live-only, "earliest" for backfill (only honored for a NEW query / checkpoint)
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

CHECKPOINT_EVENTS = os.getenv("CHECKPOINT_EVENTS", "s3a://checkpoints/opcua-rtdip/events")
CHECKPOINT_LATEST = os.getenv("CHECKPOINT_LATEST", "s3a://checkpoints/opcua-rtdip/latest")
CHECKPOINT_SUFFIX = os.getenv("CHECKPOINT_SUFFIX", "").strip()  # e.g., "run6"

# Delta target paths
DELTA_FLOAT   = os.getenv("DELTA_PCDM_FLOAT",   "s3a://historian-data/pcdm/events_float")
DELTA_STRING  = os.getenv("DELTA_PCDM_STRING",  "s3a://historian-data/pcdm/events_string")
DELTA_INTEGER = os.getenv("DELTA_PCDM_INTEGER", "s3a://historian-data/pcdm/events_integer")
DELTA_LATEST  = os.getenv("DELTA_PCDM_LATEST",  "s3a://historian-data/pcdm/latest")

MAX_OFFSETS   = os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000")
SPARK_SHUFFLE = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")

AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Apply suffix to force a new query (new checkpoint paths)
if CHECKPOINT_SUFFIX:
    CHECKPOINT_EVENTS = CHECKPOINT_EVENTS.rstrip("/") + "_" + CHECKPOINT_SUFFIX
    CHECKPOINT_LATEST = CHECKPOINT_LATEST.rstrip("/") + "_" + CHECKPOINT_SUFFIX
    # Optional: write Latest to a new path so you can compare runs cleanly
    if DELTA_LATEST.endswith("/latest"):
        DELTA_LATEST = DELTA_LATEST + "_" + CHECKPOINT_SUFFIX

log.info("=== Stage 0: Environment ===")
log.info(f"KAFKA_BOOTSTRAP_SERVERS={BOOTSTRAP}")
log.info(f"KAFKA_TOPIC={TOPIC}")
log.info(f"KAFKA_STARTING_OFFSETS={STARTING_OFFSETS}")
log.info(f"CHECKPOINT_EVENTS={CHECKPOINT_EVENTS}")
log.info(f"CHECKPOINT_LATEST={CHECKPOINT_LATEST}")
log.info(f"DELTA_STRING={DELTA_STRING}")
log.info(f"DELTA_FLOAT={DELTA_FLOAT}")
log.info(f"DELTA_INTEGER={DELTA_INTEGER}")
log.info(f"DELTA_LATEST={DELTA_LATEST}")

# ----------------------------
# Spark (Delta + S3A/MinIO)
# ----------------------------
log.info("=== Stage 1: Spark Session ===")
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

# ----------------------------
# MinIO preflight (non-fatal)
# ----------------------------
log.info("=== Stage 2: MinIO path preflight ===")
try:
    jvm = spark._jvm
    jsc = spark._jsc
    hconf = jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", AWS_S3_ENDPOINT.replace("http://", "").replace("https://", ""))
    hconf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    Path = jvm.org.apache.hadoop.fs.Path
    for path in [CHECKPOINT_EVENTS, CHECKPOINT_LATEST, DELTA_STRING, DELTA_FLOAT, DELTA_INTEGER, DELTA_LATEST]:
        p = Path(path)
        fs = p.getFileSystem(hconf)
        try:
            exists = fs.exists(p)
            log.info("Path exists? %s -> %s", path, exists)
        except Exception as inner:
            log.warning("FS check failed for %s (non-fatal): %s", path, inner)
    log.info("MinIO preflight completed.")
except Exception as e:
    log.warning("MinIO preflight overall failure (non-fatal): %s", e)

# ----------------------------
# Stage 2.5: Ensure Delta tables exist (partition & schemas)
# ----------------------------
log.info("=== Stage 2.5: Ensure Delta tables exist ===")

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

# Latest schema: let destination evolve, but initialize compatible columns
latest_schema = StructType([
    StructField("TagName",       StringType(),    False),
    StructField("EventTime",     TimestampType(), True),
    StructField("Status",        StringType(),    True),
    StructField("Value",         StringType(),    True),
    StructField("ValueType",     StringType(),    True),
    StructField("GoodEventTime", TimestampType(), True),
    StructField("GoodValue",     StringType(),    True),
    StructField("GoodValueType", StringType(),    True),
])

# Events string schema (partitioned by EventDate)
events_string_schema = StructType([
    StructField("EventDate", DateType(),     True),
    StructField("TagName",   StringType(),   False),
    StructField("EventTime", TimestampType(), True),
    StructField("Status",    StringType(),    True),
    StructField("Value",     StringType(),    True),
])

ensure_delta_table(DELTA_LATEST, latest_schema)
ensure_delta_table(DELTA_STRING, events_string_schema, ["EventDate"])
# (Float/int tables will be created on first write)

# ----------------------------
# Stage 3: Kafka batch peek (non-blocking; visibility only)
# ----------------------------
log.info("=== Stage 3: Kafka batch sample (peek latest) ===")
try:
    sample_df = (
        spark.read
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", "latest")
             .option("endingOffsets", "latest")
             .load()
             .limit(10)
    )
    log.info("Kafka peek rows (up to 10):")
    sample_df.selectExpr("CAST(value AS STRING)").show(10, truncate=False)
except Exception as e:
    log.warning("Kafka batch peek failed (non-blocking): %s", e)

# ----------------------------
# Stage 4: Kafka streaming source
# ----------------------------
log.info("=== Stage 4: Create Kafka streaming source ===")
src_df = SparkKafkaSource(
    spark=spark,
    options={
        "kafka.bootstrap.servers": BOOTSTRAP,
        "subscribe": TOPIC,
        "startingOffsets": STARTING_OFFSETS,   # "latest" or "earliest"
        "failOnDataLoss": "false",
        "includeHeaders": "true",
        "maxOffsetsPerTrigger": MAX_OFFSETS,
    },
).read_stream()
log.info("Kafka source created. Columns=%s", src_df.columns)

# ----------------------------
# Stage 5: Binary -> String
# ----------------------------
log.info("=== Stage 5: Binary->String transform ===")
json_df = BinaryToStringTransformer(
    data=src_df, source_column_name="value", target_column_name="json"
).transform()
log.info("json_df schema:")
json_df.printSchema()

# ----------------------------
# Stage 6: Parse JSON + Build PCDM events_df (with ValueType inference)
# ----------------------------
log.info("=== Stage 6: Parse JSON + Build PCDM events_df ===")

# Keep 'json' for type inference; extract standard fields
parsed_base = json_df.select(
    "json",
    get_json_object(col("json"), "$.display_name").alias("TagName"),
    get_json_object(col("json"), "$.value").alias("ValueStr"),            # arrays/scalars as JSON string
    get_json_object(col("json"), "$.timestamp").alias("IngestTimeStr"),
    get_json_object(col("json"), "$.source_timestamp").alias("SourceTimeStr"),
    get_json_object(col("json"), "$.quality").alias("Quality"),
)

event_time  = to_timestamp(col("SourceTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX")
ingest_time = to_timestamp(col("IngestTimeStr"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")

parsed = parsed_base.withColumn(
    "EventTime",
    when(event_time.isNotNull(), event_time)
    .otherwise(when(ingest_time.isNotNull(), ingest_time).otherwise(current_timestamp()))
)

# ValueType inference for Latest routing (float/integer/string):
is_integer = expr("try_cast(get_json_object(json, '$.value') AS long) IS NOT NULL")
is_float   = expr("try_cast(get_json_object(json, '$.value') AS double) IS NOT NULL")

events_df = (
    parsed
    .withColumn("Status", when(col("Quality").isNotNull(), col("Quality")).otherwise(lit("Good")))
    .withColumn("Value", col("ValueStr"))  # keep as string; numeric analytics can cast when reading
    .withColumn(
        "ValueType",
        when(is_integer, lit("integer"))
        .when(is_float,   lit("float"))
        .otherwise(lit("string"))
    )
    .withColumn("ChangeType", lit("upsert"))         # required by Events sink
    .withColumn("EventDate", to_date(col("EventTime")))  # required partition column for events_string
    .select("EventDate", "TagName", "EventTime", "Status", "Value", "ValueType", "ChangeType")
)

log.info("events_df schema:")
events_df.printSchema()

# ----------------------------
# Stage 7: PCDM Events -> Delta (append-only, no MERGE)
# ----------------------------
log.info("=== Stage 7: Start PCDM Events stream -> Delta (append-only) ===")
events_query = SparkPCDMToDeltaDestination(
    spark=spark,
    data=events_df,
    options={"checkpointLocation": CHECKPOINT_EVENTS},
    destination_float=DELTA_FLOAT,
    destination_string=DELTA_STRING,
    destination_integer=DELTA_INTEGER,
    mode="append",
    trigger="10 seconds",
    query_name="OPCUA_PCDM_EVENTS",
    merge=False,                 # avoid MERGE predicate issues
    remove_nanoseconds=True,
    remove_duplicates=False,     # dedup implies MERGE keys; disable for append-only
).write_stream()

# ----------------------------
# Stage 8: PCDM Latest -> Delta (MERGE)
# ----------------------------
log.info("=== Stage 8: Start PCDM Latest stream -> Delta (MERGE) ===")
latest_query = SparkPCDMLatestToDeltaDestination(
    spark=spark,
    data=events_df.select("TagName", "EventTime", "Status", "Value", "ValueType"),  # columns expected by Latest
    options={"checkpointLocation": CHECKPOINT_LATEST},
    destination=DELTA_LATEST,
    mode="append",
    trigger="10 seconds",
    query_name="OPCUA_PCDM_LATEST",
    query_wait_interval=10,  # emits progress logs periodically
).write_stream()

# ----------------------------
# Stage 9: Monitor queries
# ----------------------------
def monitor_queries():
    while True:
        try:
            for q in spark.streams.active:
                try:
                    progress = q.lastProgress
                    status   = q.status
                    exc      = getattr(q, "exception", None)
                    if progress:
                        log.info("STREAM PROGRESS (%s): %s", q.name(), json.dumps(progress))
                    else:
                        log.info("STREAM (%s): idle (no progress yet)", q.name())
                    log.debug("STREAM STATUS (%s): %s", q.name(), status)
                    if exc:
                        log.error("STREAM EXCEPTION (%s): %s", q.name(), exc)
                except Exception as inner:
                    log.debug("Monitor could not read stats: %s", inner)
            time.sleep(10)
        except Exception as outer:
            log.warning("Query monitor loop exception: %s", outer)
            time.sleep(10)

log.info("=== Stage 9: Start query monitor ===")
threading.Thread(target=monitor_queries, daemon=True).start()

# ----------------------------
# Stage 10: Await termination
# ----------------------------
log.info("=== Stage 10: Await termination ===")
spark.streams.awaitAnyTermination()
