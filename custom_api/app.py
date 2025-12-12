import logging
from fastapi import FastAPI, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
import uvicorn
from typing import Optional
from pyspark.sql.functions import col


# ---------------------------------------------------
# Configure Logging
# ---------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("opcua_api")

logger.info("Starting Spark Session...")

# ---------------------------------------------------
# Initialize Spark Session
# ---------------------------------------------------
spark = (
    SparkSession.builder.appName("CustomAPI")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://rtdip-minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

logger.info("Spark Session initialized successfully.")

# Data Path
DATA_PATH = "s3a://historian-data/opcua_metrics/"

app = FastAPI(title="OPC UA Historian API", version="1.0")

# ---------------------------------------------------
# API 1: List Nodes
# ---------------------------------------------------
@app.get("/api/v1/nodes")
def list_nodes():
    logger.info("API /nodes called")

    try:
        logger.info("Reading parquet data from MinIO")
        df = spark.read.parquet(DATA_PATH)

        logger.info("Extracting distinct displayNames")
        nodes = [r.displayName for r in df.select("displayName").distinct().collect()]

        logger.info(f"Returning {len(nodes)} nodes")
        return {"nodes": nodes}

    except Exception as e:
        logger.error(f"Error in /nodes: {e}")
        return {"error": str(e)}

# ---------------------------------------------------
# API 2: Query Latest Records for a Node
# ---------------------------------------------------
@app.get("/api/v1/query")
def query_data(node: str = Query(...), limit: int = 10):
    logger.info(f"API /query called for node={node} limit={limit}")

    try:
        df = spark.read.parquet(DATA_PATH)

        logger.info("Filtering data by node")
        filtered = (
            df.filter(col("displayName") == node)
            .orderBy(col("timestamp").desc())
            .limit(limit)
        )

        rows = filtered.collect()
        results = [r.asDict() for r in rows]

        logger.info(f"Returning {len(results)} results")
        return {"count": len(results), "data": results}

    except Exception as e:
        logger.error(f"Error in /query: {e}")
        return {"error": str(e)}

# ---------------------------------------------------
# API 3: Aggregation By Node
# ---------------------------------------------------
@app.get("/api/v1/aggregate")
def aggregate_data(node: str = Query(...)):
    logger.info(f"API /aggregate called for node={node}")

    try:
        df = spark.read.parquet(DATA_PATH)

        logger.info("Running aggregate functions")
        stats_row = df.filter(col("displayName") == node).agg(
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value")
        ).collect()[0]

        result = stats_row.asDict()

        logger.info("Aggregation complete")
        return {"node": node, "stats": result}

    except Exception as e:
        logger.error(f"Error in /aggregate: {e}")
        return {"error": str(e)}

# ---------------------------------------------------
# API 4: Query Data by Time Range
# ---------------------------------------------------
@app.get("/api/v1/query_by_time")
def query_by_time(
    start_time: str = Query(..., description="Start timestamp (ISO8601)"),
    end_time: str = Query(..., description="End timestamp (ISO8601)"),
    limit: int = Query(5000, description="Maximum number of rows to return")
):
    logger.info(f"API /query_by_time called start={start_time} end={end_time} limit={limit}")

    try:
        logger.info("Loading Delta/Parquet data")
        df = spark.read.parquet(DATA_PATH)

        logger.info("Filtering by timestamp range")
        filtered = (
            df.filter(
                (col("timestamp") >= start_time) &
                (col("timestamp") <= end_time)
            )
            .orderBy(col("timestamp").asc())
            .limit(limit)
        )

        rows = filtered.collect()
        results = [r.asDict() for r in rows]

        logger.info(f"Returning {len(results)} time-filtered records")
        return {"count": len(results), "data": results}

    except Exception as e:
        logger.error(f"Error in /query_by_time: {e}")
        return {"error": str(e)}


# ---------------------------------------------------
# API 5: Retrieve Data With Optional Filters
# ---------------------------------------------------

@app.get("/api/v1/filter")
def filter_data(
    start_time: Optional[str] = Query(None, description="Start timestamp (ISO8601)"),
    end_time: Optional[str] = Query(None, description="End timestamp (ISO8601)"),
    quality: Optional[str] = Query(None, description="Quality number (use 'null' to filter NULL quality)"),
    value: Optional[float] = Query(None, description="Exact value match"),
    limit: int = Query(100, description="Maximum number of records to return")
):
    logger.info(
        f"API /query_filtered called with: "
        f"start_time={start_time}, end_time={end_time}, "
        f"quality={quality}, value={value}, limit={limit}"
    )

    try:
        logger.info("Loading Delta data...")
        df = spark.read.format("delta").load(DATA_PATH)

        # -------------------------------------------------------
        # Apply filters dynamically
        # -------------------------------------------------------

        # Timestamp filtering
        if start_time:
            logger.info(f"Applying start_time filter: timestamp >= {start_time}")
            df = df.filter(col("timestamp") >= start_time)

        if end_time:
            logger.info(f"Applying end_time filter: timestamp <= {end_time}")
            df = df.filter(col("timestamp") <= end_time)

        # Quality filtering
        if quality == "null":
            logger.info("Filtering where quality IS NULL")
            df = df.filter(col("quality").isNull())

        elif quality:
            logger.info(f"Filtering quality == {quality}")
            df = df.filter(col("quality") == quality)

        # Value filtering
        if value is not None:
            logger.info(f"Filtering value == {value}")
            df = df.filter(col("value") == value)

        # Ordering + limit
        logger.info(f"Ordering by timestamp ASC and limiting to {limit} rows")
        df = df.orderBy(col("timestamp").asc()).limit(limit)

        # Convert to JSON
        logger.info("Converting results to pandas")
        results = df.toPandas().to_dict(orient="records")

        logger.info(f"Returning {len(results)} filtered records")
        return {"count": len(results), "data": results}

    except Exception as e:
        logger.error(f"Error in /query_filtered: {e}")
        return {"error": str(e)}

# ---------------------------------------------------
# Run App
# ---------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting Uvicorn server...")
    uvicorn.run(app, host="0.0.0.0", port=80)
