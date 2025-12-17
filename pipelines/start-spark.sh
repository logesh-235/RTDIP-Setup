
#!/usr/bin/env bash
# Do NOT exit on first error; we want to print spark-submit failure and keep container alive
set -uo pipefail

echo "[start-spark] Using Spark master: ${SPARK_MASTER_URL:-spark://spark-master:7077}"
echo "[start-spark] Running strict compressor pipeline..."

# Build the spark-submit command
SPARK_SUBMIT_CMD="/opt/spark/bin/spark-submit \
  --master \"${SPARK_MASTER_URL:-spark://spark-master:7077}\" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.hadoop.fs.s3a.endpoint=\"${AWS_S3_ENDPOINT:-http://minio:9000}\" \
  --conf spark.hadoop.fs.s3a.access.key=\"${AWS_ACCESS_KEY_ID:-minioadmin}\" \
  --conf spark.hadoop.fs.s3a.secret.key=\"${AWS_SECRET_ACCESS_KEY:-minioadmin}\" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark/pipelines/opcua_rtdip_pipeline.py"

# Run the job and capture the exit code
echo "[start-spark] Submitting job..."
bash -lc "$SPARK_SUBMIT_CMD"
RC=$?

if [ $RC -ne 0 ]; then
  echo "[start-spark] ERROR: spark-submit failed with exit code $RC"
  echo "[start-spark] Streaming failed. Keeping container alive for troubleshooting (check 'docker logs -f rtdip-spark-streaming')..."
fi

# Keep container alive so you can inspect logs
tail -f /dev/null
``
