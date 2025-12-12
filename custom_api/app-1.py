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

@app.get("/api/v1/nodes")
def list_nodes():
    df = spark.read.parquet(DATA_PATH)
    nodes = [r["displayName"] for r in df.select("displayName").distinct().collect()]
    return {"nodes": nodes}

@app.get("/api/v1/query")
def query_data(node: str = Query(...), limit: int = 10):
    df = spark.read.format("delta").load(DATA_PATH)
    filtered = df.filter(col("displayName") == node)
    results = filtered.orderBy(col("timestamp").desc()).limit(limit).toPandas().to_dict(orient="records")
    return {"count": len(results), "data": results}

@app.get("/api/v1/aggregate")
def aggregate_data(node: str = Query(...)):
    df = spark.read.format("delta").load(DATA_PATH)
    from pyspark.sql.functions import avg, max, min
    stats = df.filter(col("displayName") == node).agg(
        avg("value").alias("avg_value"),
        max("value").alias("max_value"),
        min("value").alias("min_value")
    ).toPandas().to_dict(orient="records")
    return {"node": node, "stats": stats}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
