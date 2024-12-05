import logging
import os
import zipfile

import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

print(f"BUCKET: {BUCKET}")

def get_file_names_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    tickers = df["Ticker"].tolist()  # Extract tickers
    return [f"{ticker.lower()}.us.txt" for ticker in tickers]


dataset_file = "historical_stock_prices-kaggle.zip"
dataset_url = "https://www.kaggle.com/api/v1/datasets/download/borismarjanovic/price-volume-data-for-all-us-stocks-etfs"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
unzipped_dir = f"{path_to_local_home}/unzipped"
target_dir = "raw/kaggle"
selected_files = get_file_names_from_csv(f"{path_to_local_home}/stock_list.csv")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, selected_files, src_dir, target_dir):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for file_name in selected_files:
        local_file_path = os.path.join(src_dir, file_name)
        if os.path.exists(local_file_path):
            object_name = f"{target_dir}/{file_name}"
            blob = bucket.blob(object_name)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {file_name} to gs://{bucket.name}/{object_name}")


def unzip_file(zip_path, extract_to):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logging.info(f"Extracted files to {extract_to}")


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_part2",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["historical", "gcs", "data-load"],
) as dag:

    # Task 1: Download the dataset
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file} && ls -l {path_to_local_home}/{dataset_file}",
    )

    # Task 2: Unzip the dataset
    unzip_task = PythonOperator(
        task_id="unzip_task",
        python_callable=unzip_file,
        op_kwargs={
            "zip_path": f"{path_to_local_home}/{dataset_file}",
            "extract_to": unzipped_dir,
        },
    )

    # Task 3: Echo Task
    echo_task = BashOperator(
        task_id="echo_task",
        bash_command="echo 'File Downloaded and Unzipped!'",
    )

    # Task 4: Upload the dataset to GCS
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{target_dir}/{dataset_file}",
            "selected_files": selected_files,
            "src_dir": f"{unzipped_dir}/Data/Stocks",
            "target_dir": f"{target_dir}",
        },
    )

    (download_dataset_task >> unzip_task >> echo_task >> local_to_gcs_task)
