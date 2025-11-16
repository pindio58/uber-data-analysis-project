import sys
from pathlib import Path

from shared import settings
from shared.utils.commonUtils import get_logger

from pyspark.sql import SparkSession
from shared import settings

# ---------- Spark Session ----------

def get_spark_session(appname, use_minio=False):
    logger = get_logger(__name__)
    logger.info(f"Building Spark session with app name: {appname}, use_minio: {use_minio}")

    # jars = "file:///app/pysprk/workspace/jars/hadoop-aws-3.3.4.jar,file:///app/pysprk/workspace/jars/aws-java-sdk-bundle-1.12.409.jar"

    builder = (
        SparkSession.builder
        .appName(appname)
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
    )

    if use_minio:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    spark = builder.getOrCreate()
    logger.info("Spark session built successfully")
    return spark


def read_raw_data(spark:SparkSession, source_file:Path):
    """Read raw CSV data."""
    logger = get_logger(__name__)
    path = str(source_file)
    logger.info(f"Reading raw data from {path}")
    df = (
        spark.read.format("csv")
        .option("inferSchema", True)
        .option("header", True)
        .load(path)
    )
    logger.info(f"Data read successfully — {df.count()} rows, {len(df.columns)} columns")
    return df

def write_data( df, bucket_name, folder, partition_by=None):
    """Write data to MinIO in Parquet format."""
    logger = get_logger(__name__)
    target_path = f"s3a://{bucket_name}/{folder}/"
    logger.info(f"Writing data to MinIO with folder name: {target_path}")
    (
        df.write.mode("overwrite")
        .format("parquet")
        .partitionBy(partition_by or [])
        .save(target_path)
    )
    logger.info(f"Data written to {target_path}")

if __name__ == "__main__":
    spark = get_spark_session("test_app", use_minio=True)
