

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


BUCKET_NAME = "project-bda-data-123"
AWS_CONN_ID = "aws_default"


SCRIPTS_DIR = "/opt/airflow/dags/scripts"
# ============================================================

default_args = {
    "owner": "onkar",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_aws_env_prefix() -> str:
    """Builds a shell env-var prefix (e.g. 'AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...')
    from the aws_default Airflow Connection, so a BashOperator subprocess actually
    has credentials available to it."""
    conn = BaseHook.get_connection(AWS_CONN_ID)
    access_key = conn.login
    secret_key = conn.password
    region = "us-east-1"
    if conn.extra_dejson:
        region = conn.extra_dejson.get("region_name", region)
    return (
        f"AWS_ACCESS_KEY_ID={access_key} "
        f"AWS_SECRET_ACCESS_KEY={secret_key} "
        f"AWS_DEFAULT_REGION={region}"
    )


def set_aws_env_from_connection():
    """Sets AWS_* environment variables in THIS Python process from the
    aws_default connection — used by PythonOperator tasks (boto3 reads
    these automatically once set)."""
    conn = BaseHook.get_connection(AWS_CONN_ID)
    os.environ["AWS_ACCESS_KEY_ID"] = conn.login
    os.environ["AWS_SECRET_ACCESS_KEY"] = conn.password
    region = "us-east-1"
    if conn.extra_dejson:
        region = conn.extra_dejson.get("region_name", region)
    os.environ["AWS_DEFAULT_REGION"] = region


def check_daily_funnel_not_empty():
    """Basic data-quality gate: fail the DAG run if daily_funnel came out empty,
    rather than silently feeding an empty table into the forecast step."""
    import io
    import boto3
    import pandas as pd

    set_aws_env_from_connection()
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="processed/daily_funnel/")
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

    if not keys:
        raise ValueError("No parquet files found under processed/daily_funnel/ — Spark job may have failed.")

    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    df = pd.concat(frames, ignore_index=True)

    if df.empty:
        raise ValueError("daily_funnel table is empty — failing DAG so it doesn't feed a stale forecast.")

    print(f"Data quality check passed: {len(df)} rows in daily_funnel.")


def build_spark_bash_command() -> str:
    return f"{get_aws_env_prefix()} python3 {SCRIPTS_DIR}/spark_job.py"


def build_forecast_bash_command() -> str:
    return f"{get_aws_env_prefix()} python3 {SCRIPTS_DIR}/forecast_job.py"


with DAG(
    dag_id="retailrocket_clickstream_pipeline",
    default_args=default_args,
    description="RetailRocket clickstream: S3 sensing -> Spark processing -> data quality -> Prophet forecast",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["clickstream", "retailrocket"],
) as dag:

    wait_for_raw_file = S3KeySensor(
        task_id="check_events_csv_exists",
        bucket_name=BUCKET_NAME,
        bucket_key="raw/events.csv",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",  
    )

    run_spark_job = BashOperator(
        task_id="run_spark_processing",
        bash_command=build_spark_bash_command(),
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_daily_funnel_not_empty,
    )

    run_forecast = BashOperator(
        task_id="run_prophet_forecast",
        bash_command=build_forecast_bash_command(),
    )

    wait_for_raw_file >> run_spark_job >> data_quality_check >> run_forecast
