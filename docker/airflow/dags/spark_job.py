from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("minio_pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read JSON files
df = spark.read.json("s3a://my-bucket/raw/Posicao/*.json")

# Write Parquet
df.write.parquet("s3a://my-bucket/raw/Posicao/parquet/", mode="overwrite")

