import logging
import os

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
RAW_BUCKET = (
    os.environ.get("GCP_GCS_RAW_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
LANDING_BUCKET = (
    os.environ.get("GCP_GCS_STAGING_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
BQ_DATASET = os.environ.get("GCP_BQ_DATASET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
LOCAL_SCRIPT_PATH = f"{path_to_local_home}/scripts"
LOCAL_SPARK_PATH = f"{path_to_local_home}/spark-jobs"
CSV_PATH = f"gs://{RAW_BUCKET}/csv/stock_list.csv"

logging.info(f"RAW_BUCKET: {RAW_BUCKET}")
logging.info(f"LANDING_BUCKET: {LANDING_BUCKET}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "Transform_Tabular_Data",
    default_args=default_args,
    description="A pipeline to process financial metrics and load into BigQuery",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    logging.info("DAG initialized")

    # Step 1: Upload scripts to GCS
    upload_script_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_script_to_gcs",
        src=f"{LOCAL_SPARK_PATH}/transform_job_tabular_entitities.py",
        dst="scripts/transform_job_tabular_entitities.py",
        bucket=f"{RAW_BUCKET}",
    )
    logging.info("Task upload_script_to_gcs created")

    # Step 2: Submit PySpark job to Dataproc
    dataproc_job_config = {
        "reference": {"project_id": f"{PROJECT_ID}"},
        "placement": {"cluster_name": "finance-spark-cluster"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{RAW_BUCKET}/scripts/transform_job_tabular_entitities.py",
            "args": [
                "--input-bucket",
                RAW_BUCKET,
                "--input-file-path",
                f"raw/api",
                "--output-bucket",
                LANDING_BUCKET,
                "--csv-path",
                CSV_PATH,
            ],
        },
    }

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id="submit_dataproc_job",
        job=dataproc_job_config,
        region="us-east1",
        project_id=f"{PROJECT_ID}",
    )
    logging.info("Task submit_dataproc_job created")

    # Step 3. Run BigQuery Jobs
    args = ["balance_sheet", "cash_flow", "income_statement", "quarterly"]

    # Group the BigQuery load jobs into a TaskGroup
    with TaskGroup(
        "bq_load_tasks_group", tooltip="BigQuery Load Tasks"
    ) as bq_load_tasks_group:
        for arg in args:
            bq_load_job = BigQueryInsertJobOperator(
                task_id=f"bq_load_job_{arg}",
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{LANDING_BUCKET}/{arg}/*"],
                        "destinationTable": {
                            "projectId": f"{PROJECT_ID}",
                            "datasetId": f"{BQ_DATASET}",
                            "tableId": f"{arg}",
                        },
                        "sourceFormat": "PARQUET",
                        "autodetect": True,
                    }
                },
            )
            logging.info(f"Task bq_load_job_{arg} created")

    # Set dependencies: upload -> submit -> bq_load_tasks_group
    upload_script_to_gcs >> submit_dataproc_job >> bq_load_tasks_group

    logging.info("Task dependencies set")
