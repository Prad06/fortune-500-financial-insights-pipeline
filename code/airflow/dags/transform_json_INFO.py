import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
RAW_BUCKET = (
    os.environ.get("GCP_GCS_RAW_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
LANDING_BUCKET = (
    os.environ.get("GCP_GCS_STAGING_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")
)
BQ_DATASET = os.environ.get("GCP_BQ_DATASET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "Transform_JSON_Data_Info",
    default_args=default_args,
    description="A pipeline to process financial company data and load into BigQuery",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    logger.info("DAG initialized")

    # Step 1: List files in the bucket
    list_files_in_info_folder = GCSListObjectsOperator(
        task_id="list_files_in_info_folder",
        bucket=RAW_BUCKET,
        prefix="raw/api/info/",
        delimiter="/",
    )

    # Step 2: Process each file using Dataproc
    def process_files(**kwargs):
        ti = kwargs["ti"]
        files = ti.xcom_pull(task_ids="list_files_in_info_folder")
        logger.info(f"Files retrieved from XCom: {files}")

        if not files:
            logger.info("No files found in the bucket!")
            return

        for file_object in files:
            file_name = file_object.split("/")[-1]
            logger.info(f"Processing file: {file_name}")

            # Define the Dataproc job configuration for each file
            dataproc_job_config = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": "finance-spark-cluster"},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{RAW_BUCKET}/scripts/transform_and_copy_to_landing_info.py",
                    "args": [
                        "--input-bucket",
                        RAW_BUCKET,
                        "--input-file",
                        f"raw/api/info/{file_name}",
                        "--output-bucket",
                        LANDING_BUCKET,
                        "--output-file",
                        f"info/{file_name}",
                    ],
                },
            }

            # Submit PySpark Job for each file
            try:
                job = DataprocSubmitJobOperator(
                    task_id=f"submit_dataproc_job_{file_name}",
                    job=dataproc_job_config,
                    region="us-east1",
                    project_id=PROJECT_ID,
                ).execute(context=kwargs)
                logger.info(f"Successfully submitted Dataproc job for {file_name}")
            except Exception as e:
                logger.error(f"Failed to submit Dataproc job for {file_name}: {str(e)}")
                raise

    process_files_task = PythonOperator(
        task_id="process_files",
        python_callable=process_files,
        provide_context=True,
    )

    # Step 3: Load all processed files into BigQuery
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://{LANDING_BUCKET}/info/*"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": "info",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
            }
        },
    )

    # Set task dependencies
    list_files_in_info_folder >> process_files_task >> load_to_bigquery
    logger.info("Task dependencies set")
