"""Global sandbox settings — mirrors production pattern from work repo."""
import os
from copy import deepcopy
from datetime import datetime, timedelta

# ── Environment ──────────────────────────────────────────────────────────────
AIRFLOW_ENVIRONMENT = os.getenv('AIRFLOW_ENVIRONMENT', 'local')
IS_LOCAL      = AIRFLOW_ENVIRONMENT == 'local'
IS_PRODUCTION = AIRFLOW_ENVIRONMENT == 'production'

# ── Airflow connection IDs ────────────────────────────────────────────────────
AWS_CONN_ID   = 'aws_default'
SPARK_CONN_ID = 'spark_default'

# ── S3 ────────────────────────────────────────────────────────────────────────
S3_BUCKET = os.getenv('S3_BUCKET', 'spark-aws-pipeline-sandbox')

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')


def get_default_args(owner: str = 'airflow') -> dict:
    """Default DAG args factory — same pattern as work repo."""
    return deepcopy({
        'owner': owner,
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    })
