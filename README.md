# Spark + AWS + Airflow Pipeline

A local, containerized data-engineering sandbox: **Airflow orchestrates a distributed PySpark job that reads and writes data in S3.**

## Why this project

This mirrors a common real-world Data Engineering pattern: raw data lands in object storage (S3), a distributed Spark job transforms it, and Airflow owns the scheduling/orchestration/observability. It's a deliberately small, runnable version of that pattern — a standalone Spark cluster (not a single local process), real `s3a` reads/writes (not mocked), and Airflow driving it through a standard operator (`SparkSubmitOperator`), not a shortcut script.

## Architecture

```
                 ┌──────────────┐
   S3 (AWS)  ───▶│   Airflow    │───▶ spark-submit (client mode)
   sample.txt    │ (scheduler,  │           │
                 │  webserver)  │           ▼
                 └──────────────┘   ┌─────────────────┐
                                     │  Spark Master    │
                                     └────────┬─────────┘
                                     ┌─────────┴─────────┐
                                     ▼                   ▼
                             Spark Worker 1       Spark Worker 2
                                     │                   │
                                     └─────────┬─────────┘
                                               ▼
                                     S3 (AWS) — word count result
```

The Airflow container also acts as the Spark **driver** (client deploy mode): it resolves the `hadoop-aws` package via Ivy the first time it runs (cached afterwards in `airflow/.ivy2/`), submits the job to `spark-master`, and the two workers execute it in parallel.

## Stack

- **Airflow 3.2.2** (SequentialExecutor + SQLite — single-node, enough for a sandbox)
- **Apache Spark 4.0.3** standalone cluster (1 master + 2 workers, official `apache/spark` image)
- **AWS S3** as the data lake (via `boto3`/`S3Hook` in Airflow, `s3a://` + `hadoop-aws` in Spark)

## Running it

```bash
cp .env.example .env   # fill in your AWS_* credentials
docker compose up airflow-init
docker compose up -d
```

- Airflow UI → http://localhost:8080 (airflow / airflow)
- Spark Master UI → http://localhost:8090
- Spark Worker UIs → http://localhost:8091, http://localhost:8092

Trigger the pipeline: open the Airflow UI and unpause/trigger `lab_01_s3_to_spark_wordcount`, or:

```bash
docker compose exec airflow-apiserver airflow dags trigger lab_01_s3_to_spark_wordcount
```

It will: seed a sample text file in S3 → submit a distributed word-count job to the Spark cluster reading that file via `s3a://` → write the result back to S3 → read the result back and log it.

## Project layout

```
airflow/dags/spark_s3_pipeline/   # the DAG (Airflow -> Spark orchestration)
airflow/dags/utils/               # shared settings + S3 helpers
spark/apps/                       # PySpark job(s) submitted to the cluster
spark/data/                       # local scratch data (mounted into the cluster)
```
