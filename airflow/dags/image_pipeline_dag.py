
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="distributed_image_classification_pipeline",
    default_args=default_args,
    description=(
        "Orchestrates distributed traffic image processing "
        "using Spark Cluster and YOLOv8/Mock, "
        "with results stored in ClickHouse"
    ),
    schedule_interval="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=[
        "traffic_monitoring",
        "spark",
        "yolov8",
        "clickhouse",
    ],
) as dag:

    run_spark_classification = BashOperator(
        task_id="run_spark_classification_job",
        bash_command="""
set -e

"$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
  --conf spark.pyspark.python=/opt/bitnami/python/bin/python \
  --conf spark.pyspark.driver.python=/home/airflow/spark-venv/bin/python \
  /opt/spark-jobs/spark_job.py
""",
    )
