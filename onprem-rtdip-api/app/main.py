
from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession, functions as F
import os
from typing import Optional
from datetime import datetime

app = FastAPI(title="Latest 10 API", description="Returns last 10 rows from a Delta table on MinIO", version="1.0.0")

# ---------------------------
# Environment (new structure)
# ---------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# ⚠️ Point this to your NEW MinIO Delta table root (must contain _delta_log/)
DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "s3a://your-bucket/your/new/structure/table")

spark: SparkSession = None


def init_spark_once():
    global spark
    if spark is not None:
        return

    # ---- Spark/Hadoop S3A configuration: taken from your working main(1).py ----
    spark = (
        SparkSession.builder
        .appName("latest-10-api")
        .master(SPARK_MASTER_URL)
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


@app.on_event("startup")
def on_startup():
    init_spark_once()


@app.get("/latest")
def latest():
    """
    Return last 10 rows from the configured Delta table.
    Ordering preference:
      1) EventTime (if present)
      2) Timestamp / timestamp (if present)
      3) ts / time (if present)
      4) Fallback: no ordering, just limit(10)
    """
    init_spark_once()

    try:
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Delta path: {e}")

    cols = [c.lower() for c in df.columns]
    order_col = None
    # choose an appropriate time column from common names
    for candidate in ["eventtime", "timestamp", "time", "ts"]:
        if candidate in cols:
            # pick the actual case-sensitive name
            order_col = df.columns[cols.index(candidate)]
            break

    try:
        if order_col:
            df = df.orderBy(F.col(order_col).desc()).limit(10)
        else:
            # if we don't know a timestamp column, just cap to 10
            df = df.limit(10)

        rows = [r.asDict(recursive=True) for r in df.collect()]
        return {"count": len(rows), "data": rows}
    except Exception as e:
        return {"EEError" : e}


@app.get("/query")
def query_data(
    enterprise: str,
    site: str,
    line: str,
    equipment: str,
    tag: str,
    from_ts: Optional[datetime] = None,
    to_ts: Optional[datetime] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    limit: int = Query(100, le=1000)
):
    init_spark_once()

    try:
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load delta table: {e}")

    # Partition filters (FAST)
    df = df.filter(
        (F.col("Enterprise") == enterprise) &
        (F.col("Site") == site) &
        (F.col("ProductionLine") == line) &
        (F.col("Equipment") == equipment) &
        (F.col("Tag") == tag)
    )

    # Time filters
    if from_ts:
        df = df.filter(F.col("EventTime") >= from_ts)
    if to_ts:
        df = df.filter(F.col("EventTime") <= to_ts)

    # Value filters
    df = df.withColumn("value", F.col("RawData").cast("double"))
    if min_value is not None:
        df = df.filter(F.col("value") >= min_value)
    if max_value is not None:
        df = df.filter(F.col("value") <= max_value)

    df = df.orderBy(F.col("EventTime").desc()).limit(limit)

    rows = [r.asDict(recursive=True) for r in df.collect()]

    return {"count": len(rows), "data": rows}


@app.get("/dump")
def dump_all():
    df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    return {"count": df.count(), "data": df.limit(10000).collect()}
