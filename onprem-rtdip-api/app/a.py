
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
scconf = spark.sparkContext.getConf()

print("Spark version:", spark.version)
print("spark.jars:", scconf.get("spark.jars", "NOT SET"))   # should list /app/jars/...

hc = spark._jsc.hadoopConfiguration()
print("fs.s3a.impl:", hc.get("fs.s3a.impl"))                 # org.apache.hadoop.fs.s3a.S3AFileSystem
print("fs.s3a.endpoint:", hc.get("fs.s3a.endpoint"))         # http://minio:9000
print("fs.s3a.path.style.access:", hc.get("fs.s3a.path.style.access"))  # true
``
