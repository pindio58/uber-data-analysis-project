# imports
from typing import Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from shared.utils.commonUtils import get_logger


def upload_file_to_minio(
    aws_conn_id: str,
    filename: str,
    key: str,
    bucket_name: str,
    replace: bool = True,
) -> Optional[bool]:
    ''' This is to upload a file to minio'''
    hook = S3Hook(aws_conn_id=aws_conn_id)

    return hook.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )

def write_df_to_mino(df, path,bucket_name, partition_by=None):
    logger = get_logger(__name__)
    """Write data to MinIO in Parquet format."""
    logger.info(f"Writing data to MinIO: {path}")
    target_path = f"s3a://{bucket_name}/{path}/"
    (
        df.write.mode("overwrite")
        .format("parquet")
        .partitionBy(partition_by or [])
        .save(target_path)
    )
    logger.info(f"Data written to {target_path}")