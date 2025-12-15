
/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.cores.max=6 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.locality.wait=0 \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-200}" \
  --conf spark.hadoop.fs.s3a.endpoint="${AWS_S3_ENDPOINT:-http://minio:9000}" \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark/pipelines/opcua_rtdip_pipeline.py
