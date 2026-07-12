"""Reusable utilities for all DAGs — mirrors commons.py pattern from work repo."""
import boto3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.settings import AWS_CONN_ID


# ── AWS S3 ────────────────────────────────────────────────────────────────────

def upload_to_s3(bucket: str, key: str, data: bytes, conn_id: str = AWS_CONN_ID) -> None:
    """Upload bytes to an S3 object."""
    S3Hook(aws_conn_id=conn_id).load_bytes(data, key=key, bucket_name=bucket, replace=True)


def download_from_s3(bucket: str, key: str, conn_id: str = AWS_CONN_ID) -> str:
    """Download an S3 object and return its content as string."""
    return S3Hook(aws_conn_id=conn_id).read_key(key=key, bucket_name=bucket)


def list_s3_objects(bucket: str, prefix: str = '', conn_id: str = AWS_CONN_ID) -> list:
    """List object keys in an S3 bucket under a prefix."""
    return S3Hook(aws_conn_id=conn_id).list_keys(bucket_name=bucket, prefix=prefix) or []


def get_s3_client(conn_id: str = AWS_CONN_ID):
    """Return a raw boto3 S3 client using the Airflow AWS connection."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    hook = AwsBaseHook(aws_conn_id=conn_id, client_type='s3')
    creds = hook.get_credentials()
    return boto3.client(
        's3',
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        region_name=hook.conn_config.region_name or 'us-east-1',
    )
