\
from fastapi import FastAPI, Query
from typing import Optional, List
import os, re
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from rtdip_sdk.connectors import SparkConnection
from rtdip_sdk.queries import TimeSeriesQueryBuilder

app = FastAPI(title="RTDIP On-Prem Query API (Latest + Filter)")

S3_ENDPOINT  = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

DELTA_STR = os.getenv("DELTA_PCDM_EVENTS_STRING", "s3a://historian-data/pcdm/events_string")
DELTA_INT = os.getenv("DELTA_PCDM_EVENTS_INTEGER", "s3a://historian-data/pcdm/events_integer")
DELTA_FLT = os.getenv("DELTA_PCDM_EVENTS_FLOAT",   "s3a://historian-data/pcdm/events_float")

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

spark: SparkSession = None
connection: SparkConnection = None
views_ready: bool = False
init_ok: bool = False

# If a user omits timezone, you can set a default (e.g., IST "+05:30" or UTC "+00:00")
DEFAULT_TZ_OFFSET = os.getenv("DEFAULT_TZ_OFFSET", "+00:00")

DATE_ONLY_RE     = re.compile(r'^\d{4}-\d{2}-\d{2}$')
# Accept ISO datetime (with optional fractional seconds) + trailing 'Z' or '+HH:MM'
ISO_DT_ANY_RE    = re.compile(
    r'^(?P<ymd>\d{4}-\d{2}-\d{2})[ T]'
    r'(?P<hms>\d{2}:\d{2}:\d{2})'
    r'(?P<fraction>\.\d+)?'
    r'(?P<tz>Z|[+-]\d{2}:\d{2})?$'
)

def normalize_iso_for_rtdip(ts: Optional[str]) -> Optional[str]:
    """
    Accept user-friendly ISO strings and normalize to RTDIP-accepted formats:
      - 'YYYY-MM-DD'                      (date)
      - 'YYYY-MM-DDTHH:MM:SS+zz:zz'      (datetime with offset)

    Rules:
      * If 'Z' present -> replace with '+00:00'
      * If fractional seconds present -> strip them (RTDIP expects second precision)
      * If no timezone is present -> append DEFAULT_TZ_OFFSET
      * Common typo 'ZISO' -> treat 'ZISO' as 'Z'
    """
    if not ts:
        return ts
    s = ts.strip()

    # Handle common typo: trailing ZISO (treat as Z)
    if s.endswith("ZISO"):
        s = s[:-4] + "Z"

    # Date-only case
    if DATE_ONLY_RE.fullmatch(s):
        return s

    m = ISO_DT_ANY_RE.fullmatch(s)
    if not m:
        # Try to salvage: replace space with T and retry
        s2 = s.replace(' ', 'T')
        m = ISO_DT_ANY_RE.fullmatch(s2)
        if not m:
            # Not an ISO-like string; let the endpoint return a 400
            raise ValueError(f"Timestamp not recognized as ISO: '{ts}'")

    ymd = m.group('ymd')
    hms = m.group('hms')
    tz  = m.group('tz')  # Z, +HH:MM, or None

    # Normalize timezone
    if tz == 'Z':
        tz = '+00:00'
    elif tz is None:
        tz = DEFAULT_TZ_OFFSET

    # Return second-precision (strip fraction)
    return f"{ymd}T{hms}{tz}"


def build_views():
    # Build numeric union view (int + float) if paths are readable
    df_int = spark.read.format("delta").load(DELTA_INT).withColumn("ValueDouble", F.col("Value").cast("double"))
    df_flt = spark.read.format("delta").load(DELTA_FLT).withColumn("ValueDouble", F.col("Value").cast("double"))
    numeric = df_int.select("TagName", "EventTime", "Status", "ValueDouble") \
        .unionByName(df_flt.select("TagName", "EventTime", "Status", "ValueDouble"), allowMissingColumns=True)
    numeric.createOrReplaceTempView("events_numeric")
    # String view (optional for raw inspections)
    spark.read.format("delta").load(DELTA_STR).createOrReplaceTempView("events_string")

def init_spark_once():
    global spark, connection, views_ready, init_ok
    if init_ok and spark is not None and connection is not None:
        return
    try:
        spark = (
            SparkSession.builder
            .appName("rtdip-query-api")
            .master(SPARK_MASTER)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        connection = SparkConnection(spark=spark)  # RTDIP SDK connector (no Spark Connect URI)  # 
        # Attempt to build views; if it fails, we will fallback in endpoints
        try:
            build_views()
            views_ready = True
        except Exception as ve:
            views_ready = False
            print(f"[WARN] Could not build views at startup: {ve}")
        init_ok = True
        print(f"[INIT] Spark master: {SPARK_MASTER}, Spark version: {spark.version}, views_ready={views_ready}")
    except Exception as e:
        init_ok = False
        print(f"[ERROR] Spark init failed: {e}")
        raise

@app.on_event("startup")
def on_startup():
    init_spark_once()

@app.get("/health")
def health():
    return {"status": "ok" if init_ok else "error", "views_ready": views_ready}

# -----------------------------
# 1) Latest (last 10 records)
# -----------------------------
@app.get("/api/latest")
def latest(tag: Optional[str] = Query(None, description="Optional TagName; if omitted, returns last 10 across numeric events.")):
    init_spark_once()
    now = datetime.now(timezone.utc).isoformat()
    start_date = "1970-01-01T00:00:00Z"

    if tag:
        # Pure RTDIP SDK usage; builder returns a DataFrame.  # 
        df = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("events_numeric" if views_ready else DELTA_INT,  # view if ready else int path
                    value_column=("ValueDouble" if views_ready else "Value"),
                    timestamp_column="EventTime",
                    status_column="Status",
                    tagname_column="TagName")
            .raw(tagname_filter=[tag], start_date=start_date, end_date=now,
                 include_bad_data=True, sort=True, limit=None)
        )
        # If views not ready and we only hit the INT path, also try FLOAT path and union
        if not views_ready:
            df_f = (
                TimeSeriesQueryBuilder()
                .connect(connection)
                .source(DELTA_FLT, value_column="Value", timestamp_column="EventTime",
                        status_column="Status", tagname_column="TagName")
                .raw(tagname_filter=[tag], start_date=start_date, end_date=now,
                     include_bad_data=True, sort=True, limit=None)
            )
            df = df.unionByName(df_f, allowMissingColumns=True)
        rows = [r.asDict() for r in df.orderBy(F.col("EventTime").desc()).limit(10).collect()]
        return {"rows": rows}

    # No tag provided: avoid view dependency; read Delta paths directly and union
    df_int = spark.read.format("delta").load(DELTA_INT).select(
        "TagName", "EventTime", "Status", F.col("Value").cast("double").alias("Value"))
    df_flt = spark.read.format("delta").load(DELTA_FLT).select(
        "TagName", "EventTime", "Status", F.col("Value").cast("double").alias("Value"))
    numeric = df_int.unionByName(df_flt, allowMissingColumns=True)
    rows = [r.asDict() for r in numeric.orderBy(F.col("EventTime").desc()).limit(10).collect()]
    return {"rows": rows}

# ------------------------------------------
# 2) Filter (optional: timestamp, value, quality [+ optional tag])
# ------------------------------------------
@app.get("/api/filter")
def filter_api(
    tag: Optional[str] = Query(None, description="Optional TagName"),
    timestamp_from: Optional[str] = Query(None, description="ISO datetime e.g. 2025-12-12T12:31:00Z or +05:30; or 'YYYY-MM-DD HH:MM:SS'"),
    timestamp_to: Optional[str]   = Query(None, description="ISO datetime e.g. 2025-12-12T12:51:00Z or +05:30; or 'YYYY-MM-DD HH:MM:SS'"),
    value_min: Optional[float]    = Query(None, description="Min numeric value"),
    value_max: Optional[float]    = Query(None, description="Max numeric value"),
    quality: Optional[str]        = Query(None, description="e.g. 'Good' or 'Bad'"),
):
    """
    Returns rows filtered by:
      * time window (start/end)
      * numeric value range (min/max)
      * quality ('Good'/'Bad')
    Uses RTDIP SDK TimeSeriesQueryBuilder.raw() and SparkConnection to your cluster.
    """
    # Ensure Spark and RTDIP connection exist
    init_spark_once()

    # Normalize user-friendly timestamps to RTDIP-accepted strings
    try:
        start_date = normalize_iso_for_rtdip(timestamp_from) or "1970-01-01"
        end_date   = normalize_iso_for_rtdip(timestamp_to) if timestamp_to else datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')
        #if not end_date:
        #    # current UTC as '+00:00' offset
        #    end_date = datetime.now(timezone.utc).isoformat().replace('Z', '+00:00')
    except ValueError as e:
        # Return friendly 400 on malformed timestamps
        raise HTTPException(status_code=400, detail=str(e))

    # Decide source for RTDIP builder: use numeric view if available; else INT path
    builder_src = ("events_numeric" if views_ready else DELTA_INT)
    value_col   = ("ValueDouble" if views_ready else "Value")

    # Build the tag list
    tag_list: List[str]
    if tag:
        tag_list = [tag]
    else:
        # If view exists, get distinct tags from view; else from INT path
        try:
            if views_ready:
                tag_list = [r[0] for r in spark.sql("SELECT DISTINCT TagName FROM events_numeric LIMIT 100").collect()]
            else:
                tag_list = [r[0] for r in spark.read.format("delta").load(DELTA_INT).select("TagName").distinct().limit(100).collect()]
        except Exception:
            tag_list = []

    # ---- RTDIP Query Builder (numeric) ----
    try:
        df = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source(builder_src, value_column=value_col, timestamp_column="EventTime",
                    status_column="Status", tagname_column="TagName")
            .raw(
                tagname_filter=tag_list or ([tag] if tag else tag_list),
                start_date=start_date,
                end_date=end_date,
                include_bad_data=True,  # we'll filter quality below
                sort=False,
            )
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Bad timestamp format: {e}")

    # If views not ready, union FLOAT path so numeric coverage is complete
    if not views_ready:
        df_f = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source(DELTA_FLT, value_column="Value", timestamp_column="EventTime",
                    status_column="Status", tagname_column="TagName")
            .raw(
                tagname_filter=tag_list or ([tag] if tag else tag_list),
                start_date=start_date,
                end_date=end_date,
                include_bad_data=True,
                sort=False,
            )
        )
        df = df.unionByName(df_f, allowMissingColumns=True)

    # ---- Apply optional filters: quality & value range ----
    if quality:
        df = df.where(F.col("Status") == quality)

    # cast to double in case source is string
    val_expr = F.col(value_col if views_ready else "Value").cast("double")
    if value_min is not None:
        df = df.where(val_expr >= value_min)
    if value_max is not None:
        df = df.where(val_expr <= value_max)

    # ---- Return most recent first; cap payload ----
    rows = [r.asDict() for r in df.orderBy(F.col("EventTime").desc()).collect()]
    return {
        "window": {"start": start_date, "end": end_date},
        "filters": {"tag": tag, "quality": quality, "value_min": value_min, "value_max": value_max},
        "rows": rows
    }

