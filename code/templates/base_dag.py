import logging
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
RAW_BUCKET = (
    os.environ.get("GCP_GCS_RAW_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
LANDING_BUCKET = (
    os.environ.get("GCP_GCS_STAGING_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
BQ_DATASET = os.environ.get("GCP_BQ_DATASET")

logging.info(f"RAW_BUCKET: {RAW_BUCKET}")
logging.info(f"LANDING_BUCKET: {LANDING_BUCKET}")


# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Initialize DAG
with DAG(
    "Transform_{{ arg }}_data",
    default_args=default_args,
    description="A pipeline to process {{ arg }} data and load into BigQuery",
    schedule_interval=None,  # Can be set to a cron schedule if needed
    start_date=days_ago(1),
    catchup=False,
) as dag:
    logging.info("DAG initialized")

    # Step 3: Submit PySpark job to Dataproc
    dataproc_job_config = {
        "reference": {"project_id": f"{PROJECT_ID}"},
        "placement": {"cluster_name": "finance-spark-cluster"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{RAW_BUCKET}/scripts/raw_to_landing_tabular.py",
            "args": [
                # //TO-DO
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

    # Step 4: Run BigQuery Load Job
    bq_load_job = BigQueryInsertJobOperator(
        task_id="bq_load_job",
        configuration={
            "load": {
                "sourceUris": [f"gs://{LANDING_BUCKET}/{{ arg }}/*"],
                "destinationTable": {
                    "projectId": f"{PROJECT_ID}",
                    "datasetId": f"{BQ_DATASET}",
                    "tableId": "{{ arg }}",
                },
                "sourceFormat": "PARQUET",
                "autodetect": True,
            }
        },
    )
    logging.info("Task bq_load_job_{{arg}} created")

    # Define task dependencies
    submit_dataproc_job >> bq_load_job
    logging.info("Task dependencies set")
