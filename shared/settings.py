from dotenv import load_dotenv
from pathlib import Path
import os
# import sys


BASE_DIR = Path(__file__).resolve().parent.parent
# print(BASE_DIR)
env_file = BASE_DIR / ".env"

env_files = [
    env_file
]

for env_file in env_files:
    if env_file.exists():
        load_dotenv(env_file)

# all the variables
MINIO_ACCESS_KEY=os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY=os.getenv('MINIO_SECRET_KEY')
UBERUSER=os.getenv('UBERUSER')
UBERPASS=os.getenv('UBERPASS')
MINIO_ENDPOINT=os.getenv('MINIO_ENDPOINT')
BUCKET_NAME=os.getenv('BUCKET_NAME')
HADOOP_AWS_JAR = "org.apache.hadoop:hadoop-aws:3.3.4"
appname='uber-data-analysis'
URL = 'https://raw.githubusercontent.com/ayushdixit487/Uber-Data-Analysis-Project-in-Pyspark/refs/heads/main/dataset.csv'