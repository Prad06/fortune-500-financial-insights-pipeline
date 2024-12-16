import os
import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Initialize logging
logging.basicConfig(level=logging.INFO)
logging.info(f"BUCKET: {BUCKET}")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
LANDING_BUCKET = os.environ.get("GCP_GCS_STAGING_BUCKET") + os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("GCP_BQ_DATASET")
LOCAL_CSV_PATH = os.environ.get("LOCAL_CSV_PATH")
LOCAL_SPARK_PATH = os.environ.get("LOCAL_SPARK_PATH")

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
    "Transform_Open_Close_Data",
    default_args=default_args,
    description="A pipeline to process financial data and load into BigQuery",
    schedule_interval=None,  # Can be set to a cron schedule if needed
    start_date=days_ago(1),
    catchup=False,
) as dag:
    logger.info("DAG initialized")

    # Step 1: Upload stock_list.csv to GCS
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=f"{LOCAL_CSV_PATH}/stock_list.csv",
        dst="csv/stock_list.csv",
        bucket=f"{RAW_BUCKET}",
    )
    logger.info("Task upload_csv_to_gcs created")

    # Step 2: Upload PySpark script to GCS
    upload_script_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_script_to_gcs",
        src=f"{LOCAL_SPARK_PATH}/raw_to_landing_sparkjob.py",
        dst="scripts/raw_to_landing_sparkjob.py",
        bucket=f"{LANDING_BUCKET}",
    )
    logger.info("Task upload_script_to_gcs created")

    # Step 3: Submit PySpark job to Dataproc
    dataproc_job_config = {
        "reference": {"project_id": f"{PROJECT_ID}"},
        "placement": {"cluster_name": "finance-spark-cluster"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{LANDING_BUCKET}/scripts/raw_to_landing_sparkjob.py",
        },
    }

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id="submit_dataproc_job",
        job=dataproc_job_config,
        region="us-east1",
        project_id=f"{PROJECT_ID}",
    )
    logger.info("Task submit_dataproc_job created")

    # Step 4: Run BigQuery Load Job
    bq_load_job = BigQueryInsertJobOperator(
        task_id="bq_load_job",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{LANDING_BUCKET}/stock_data/year=*"
                ],
                "destinationTable": {
                    "projectId": f"{PROJECT_ID}",
                    "datasetId": f"{BQ_DATASET}",
                    "tableId": "open_close",
                },
                "sourceFormat": "PARQUET",
                "autodetect": True,
            }
        },
    )
    logger.info("Task bq_load_job created")

    # Define task dependencies
    [upload_csv_to_gcs, upload_script_to_gcs] >> submit_dataproc_job >> bq_load_job
    logger.info("Task dependencies set")
