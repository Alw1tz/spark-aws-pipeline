"""
S3 → Spark → S3 word count pipeline.

--------
* DAG Name:
    lab_01_s3_to_spark_wordcount
* Owner:
    alw1tz
* Description:
    Seeds a sample text file in S3, submits a distributed PySpark job (against
    the standalone Spark cluster) that reads the file via s3a, counts words,
    and writes the result back to S3. Then reads the result back and logs it.
    Demonstrates Airflow orchestrating a Spark job that reads/writes S3 —
    the core "Data Engineering: PySpark + AWS" pattern.
"""
from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.settings import get_default_args, S3_BUCKET, AWS_CONN_ID, SPARK_CONN_ID

# -------------------- Globals --------------------
INPUT_KEY = 'spark-input/sample.txt'
OUTPUT_PREFIX = 'spark-output/{{ ds_nodash }}'

SAMPLE_TEXT = (
    b"spark is fast\n"
    b"spark is fun\n"
    b"practice makes perfect\n"
    b"data engineering with spark and aws\n"
)

# -------------------- DAG --------------------
with DAG(
    dag_id='lab_01_s3_to_spark_wordcount',
    description='S3 -> Spark (s3a) -> S3 distributed word count.',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    default_args=get_default_args('alw1tz'),
    catchup=False,
    tags=['spark', 'aws', 'lab'],
) as dag:

    @task
    def seed_input():
        from utils.commons import get_s3_client, upload_to_s3
        s3 = get_s3_client()
        try:
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"Bucket '{S3_BUCKET}' created.")
        except Exception:
            print(f"Bucket '{S3_BUCKET}' already exists.")
        upload_to_s3(S3_BUCKET, INPUT_KEY, SAMPLE_TEXT)
        print(f"Uploaded sample input to s3://{S3_BUCKET}/{INPUT_KEY}")

    spark_wordcount = SparkSubmitOperator(
        task_id='spark_wordcount',
        conn_id=SPARK_CONN_ID,
        application='/opt/airflow/spark_apps/s3_wordcount.py',
        application_args=[
            f's3a://{S3_BUCKET}/{INPUT_KEY}',
            f's3a://{S3_BUCKET}/{OUTPUT_PREFIX}',
        ],
        packages='org.apache.hadoop:hadoop-aws:3.4.1',
        conf={
            'spark.hadoop.fs.s3a.access.key': os.environ.get('AWS_ACCESS_KEY_ID', ''),
            'spark.hadoop.fs.s3a.secret.key': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.endpoint.region': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
            'spark.jars.ivy': '/opt/airflow/.ivy2',
        },
        verbose=True,
    )

    @task
    def read_result(ds_nodash: str = None):
        from utils.commons import list_s3_objects, download_from_s3
        prefix = f'spark-output/{ds_nodash}/'
        keys = [k for k in list_s3_objects(S3_BUCKET, prefix=prefix) if 'part-' in k]
        if not keys:
            raise FileNotFoundError(f'No Spark output found under s3://{S3_BUCKET}/{prefix}')
        content = download_from_s3(S3_BUCKET, keys[0])
        print(f"Result from s3://{S3_BUCKET}/{keys[0]}:")
        print(content)

    # -------------------- Task Dependencies --------------------
    seed_input() >> spark_wordcount >> read_result()
