import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from scripts.load_bronze_to_silver import process_entity

PROJECT_ID = os.getenv("GCP_PROJECT", "chitti-data-dev-1795012096")
BRONZE_BUCKET = f"{PROJECT_ID}-bronze"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="credit_risk_bronze_to_silver",
    default_args=default_args,
    description="Orchestrate Bronze GCS CSV ingestion into BigQuery Silver tables",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["medallion", "silver", "credit-risk"],
) as dag:

    wait_for_bronze_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_bronze_files",
        bucket=BRONZE_BUCKET,
        prefix="",
        poke_interval=60,
        timeout=3600,
    )

    dim_tasks = [
        PythonOperator(
            task_id=f"load_silver_{entity}",
            python_callable=process_entity,
            op_kwargs={"entity_name": entity},
        )
        for entity in ["customers", "loans", "collateral", "macro_indicators"]
    ]

    fact_tasks = [
        PythonOperator(
            task_id=f"load_silver_{entity}",
            python_callable=process_entity,
            op_kwargs={"entity_name": entity},
        )
        for entity in ["repayments", "defaults_recoveries"]
    ]

    wait_for_bronze_files >> dim_tasks
    for d_task in dim_tasks:
        d_task >> fact_tasks
