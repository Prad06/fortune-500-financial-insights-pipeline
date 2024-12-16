import datetime as dt
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RawToStagingSpark") \
    .getOrCreate()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")

logging.info(f"BUCKET: {BUCKET}")


def process_raw_data_with_spark(raw_dir, staging_dir):
    logging.info(f"Processing raw data with Spark from directory: {raw_dir}")
    
    # Read all CSV and JSON files from raw_dir
    csv_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".csv")]
    json_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".json")]
    
    all_files = csv_files + json_files
    
    # Read data with Spark
    df = spark.read.option("header", "true").csv(csv_files) if csv_files else None
    if json_files:
        df_json = spark.read.json(json_files)
        if df is not None:
            df = df.union(df_json)
        else:
            df = df_json
    
    # Perform transformations if needed, for example adding a timestamp
    df = df.withColumn("timestamp", spark.sql.functions.current_timestamp())

    # Write the processed data to staging directory in Parquet format
    df.write.mode("overwrite").parquet(staging_dir)
    logging.info(f"Processed data written to staging directory: {staging_dir}")


def upload_processed_data_to_gcs(bucket, src_dir, target_dir):
    logging.info(f"Uploading processed data from {src_dir} to GCS bucket {bucket}")
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    if os.path.exists(src_dir):
        for root, _, files in os.walk(src_dir):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_file_path, src_dir)
                subfolder = os.path.dirname(relative_path)
                gcs_path = os.path.join(target_dir, subfolder, file_name)

                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file_path)

                logging.info(
                    f"Uploaded {local_file_path} to gs://{bucket.name}/{gcs_path}"
                )


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="spark_raw_to_staging_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "financial-data", "yfinance"],
) as dag:

    raw_dir = f"{path_to_local_home}/data_raw"
    staging_dir = f"{path_to_local_home}/data_staging"

    # Task to process the raw data using Spark
    process_raw_data_task = PythonOperator(
        task_id="process_raw_data_with_spark",
        python_callable=process_raw_data_with_spark,
        op_kwargs={"raw_dir": raw_dir, "staging_dir": staging_dir},
    )

    # Task to upload processed data to GCS
    upload_task = PythonOperator(
        task_id="upload_processed_data_to_gcs",
        python_callable=upload_processed_data_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "src_dir": staging_dir,
            "target_dir": "staging/api/",
        },
    )

    # Task dependencies
    process_raw_data_task >> upload_task
