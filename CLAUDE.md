# spark-aws-pipeline

Data Engineering sandbox: Airflow orchestrating PySpark jobs against a standalone Spark cluster, reading/writing S3. See [README.md](README.md) for architecture and how to run it.

## Conventions

- **DAG file format** (production-style, mirrors the user's work repo): docstring header with `DAG Name` / `Owner` / `Description`, a `# -------------------- Globals --------------------` section for constants, `with DAG(...)` context manager (not the `@dag` decorator), individual tasks as `@task`-decorated functions (TaskFlow) or standard Operators, and a `# -------------------- Task Dependencies --------------------` section at the bottom wiring the graph. Follow `airflow/dags/spark_s3_pipeline/lab_01_s3_to_spark_wordcount.py` as the reference example.
- **Shared helpers** live in `airflow/dags/utils/`: `settings.py` for connection IDs/env-driven config/`get_default_args()`, `commons.py` for reusable S3 operations (`upload_to_s3`, `download_from_s3`, `list_s3_objects`, `get_s3_client`). Add new shared logic there rather than duplicating it per DAG.
- **PySpark jobs** live in `spark/apps/`, one script per job, taking S3 paths as CLI args (`sys.argv`) rather than hardcoding — keeps them testable with `spark-submit` directly, independent of Airflow.
- **New labs**: number them (`lab_02_...`) and put each in its own subfolder under `airflow/dags/` if it's a distinct scenario, or alongside `lab_01` under `spark_s3_pipeline/` if it's a variation on the same pipeline.

## Credentials

Real values live in `.env` (gitignored). `.env.example` documents the required keys — currently just AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) plus the standard Airflow bootstrap vars (`FERNET_KEY`, `AIRFLOW_UID`, admin user). No Snowflake/Kafka here — those belong to other sandboxes.

## Scope boundary

This repo is scoped to **Spark + AWS + Airflow only**. If a change pulls in Snowflake, Kafka, or dbt, it belongs in a different sandbox under `~/Documents/dev/` — don't grow this one into a monorepo again.
