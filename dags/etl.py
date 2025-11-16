from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

DATA_DIR = BASE_DIR / "shared" / "data"
file_path = str(DATA_DIR / "uberData.csv")
# print(f)
script_path = str(BASE_DIR / "spark_jobs" / "uber_solution.py")

from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from shared.utils.commonUtils import delete_file, download_file, get_logger
from shared.settings import URL

default_args={
    'owner':'pindio58',
    'depends_on_past':False,
    'retries':0,
    'email_on_failure':False
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025,11,1),
    catchup=False,
    dag_id='uber-data-analysis',
    dag_display_name='uber-data-analysis',
    tags=['uber']
)
def perform_analysis():

    @task
    def get_file(URL,path):
        file = download_file(url=URL,
                             path=str(path))
        return file
    
    download = get_file(URL,str(file_path))

    spark_tasks = []

    for q in range(1,11):
        t = SparkSubmitOperator(
                    task_id=f"question_{q}",
                    application=script_path,
                    application_args=[
                        f"--question-id={q}",
                        f"--path={file_path}"
                    ],
                    conn_id="spark_default",
                    conf={
                    "spark.jars": "file:///opt/spark/jars/hadoop-aws-3.3.4.jar,file:///opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
                     "spark.driver.memory": "2g",
                     "spark.executor.memory": "2g",
                    },
                    verbose=True,
                    env_vars={"JAVA_HOME":"/usr/lib/jvm/java-17-openjdk-arm64/"} 
        )
        spark_tasks.append(t)
    
    # -- this is for sequential chaining --
    
    download >> spark_tasks[0]
    for _ in range(1, len(spark_tasks)):
        spark_tasks[_-1] >> spark_tasks[_]

perform_analysis()