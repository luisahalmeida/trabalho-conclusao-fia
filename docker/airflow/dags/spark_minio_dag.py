from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_minio_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="spark_submit_minio",
        application="/opt/airflow/dags/spark_job.py",
        conn_id="spark_default",
        name="spark_minio_job",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        },
    )

