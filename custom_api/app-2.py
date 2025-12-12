from fastapi import FastAPI, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import uvicorn

app = FastAPI(title="OPC UA Historian API", version="1.0")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("CustomAPI")
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

DATA_PATH = "s3a://historian-data/opcua_metrics/"

# Cache Delta Table at startup
print("Loading and caching Delta table...")
delta_cached = spark.read.format("delta").load(DATA_PATH).cache()
delta_cached.count()
print("Delta table cached in memory.")

@app.get("/api/v1/nodes")
def list_nodes():
    df = delta_cached
    nodes = [r["displayName"] for r in df.select("displayName").distinct().collect()]
    return {"nodes": nodes}

@app.get("/api/v1/query")
def query_data(node: str = Query(...), limit: int = 10):
    df = delta_cached
    filtered = df.filter(col("displayName") == node)
    results = [row.asDict() for row in filtered.orderBy(col("timestamp").desc()).limit(limit).collect()]
    return {"count": len(results), "data": results}

@app.get("/api/v1/aggregate")
def aggregate_data(node: str = Query(...)):
    df = delta_cached
    from pyspark.sql.functions import avg, max, min
    stats = df.filter(col("displayName") == node).agg(
        avg("value").alias("avg_value"),
        max("value").alias("max_value"),
        min("value").alias("min_value")
    ).collect()[0].asDict()
    return {"node": node, "stats": stats}

# Extra filter API
@app.get("/api/v1/filter")
def filter_data(
    node: str = Query(...),
    start_timestamp: str | None = Query(None),
    end_timestamp: str | None = Query(None),
    min_value: float | None = Query(None),
    max_value: float | None = Query(None)
):
    df = delta_cached
    filtered = df.filter(col("displayName") == node)

    if start_timestamp:
        filtered = filtered.filter(col("timestamp") >= start_timestamp)
    if end_timestamp:
        filtered = filtered.filter(col("timestamp") <= end_timestamp)
    if min_value is not None:
        filtered = filtered.filter(col("value") >= min_value)
    if max_value is not None:
        filtered = filtered.filter(col("value") <= max_value)

    results = [row.asDict() for row in filtered.orderBy(col("timestamp").asc()).collect()]
    return {"count": len(results), "data": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
