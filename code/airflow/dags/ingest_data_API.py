import datetime as dt
import json
import logging
import os
import zipfile

import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import yfinance as yf
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET") + "_" + os.environ.get("GCP_PROJECT_ID")


logging.info(f"BUCKET: {BUCKET}")


def get_file_names_from_csv(csv_path):
    logging.info(f"Reading CSV file from path: {csv_path}")
    df = pd.read_csv(csv_path)
    tickers = df["Ticker"].tolist()  # Extract tickers
    logging.info(f"Tickers extracted: {tickers}")
    return tickers


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
target_dir = "raw/kaggle"
tickers = get_file_names_from_csv(f"{path_to_local_home}/stock_list.csv")
logging.info(f"Tickers to process: {tickers}")


def download_stock_data(ticker, **kwargs):
    logging.info(f"Starting download for ticker: {ticker}")
    # Configuration
    last_date_from_historical_load = dt.date(2017, 11, 10)
    end_date_for_historical_load_from_api = dt.date(2024, 12, 1)

    # Initialize Ticker
    stock = yf.Ticker(ticker)

    # Create necessary directories
    data_dirs = [
        "info",
        "open_close",
        "balance_sheet",
        "cash_flow",
        "income_statement",
        "quarterly",
        "analyst_recommendations",
        "earnings_estimates",
        "dividend_history",
        "sustainability",
        "options",
    ]
    for dir_name in data_dirs:
        os.makedirs(f"{path_to_local_home}/data_api/{dir_name}", exist_ok=True)

    # Metadata (JSON)
    stock_metadata_info = stock.info
    with open(f"{path_to_local_home}/data_api/info/{ticker}.json", "w") as json_file:
        json.dump(stock_metadata_info, json_file)
    logging.info(f"Downloaded metadata for {ticker}")

    # Historical Open/Close Data
    stock_open_close_data = yf.download(
        ticker,
        start=last_date_from_historical_load,
        end=end_date_for_historical_load_from_api,
    )
    stock_open_close_data.to_csv(
        f"{path_to_local_home}/data_api/open_close/{ticker}.csv"
    )
    logging.info(f"Downloaded historical open/close data for {ticker}")

    # Financial Statements
    stock.balance_sheet.to_csv(
        f"{path_to_local_home}/data_api/balance_sheet/{ticker}.csv"
    )
    stock.cashflow.to_csv(f"{path_to_local_home}/data_api/cash_flow/{ticker}.csv")
    stock.income_stmt.to_csv(
        f"{path_to_local_home}/data_api/income_statement/{ticker}.csv"
    )
    logging.info(f"Downloaded financial statements for {ticker}")

    # Quarterly Data
    stock.quarterly_income_stmt.to_csv(
        f"{path_to_local_home}/data_api/quarterly/{ticker}.csv"
    )
    logging.info(f"Downloaded quarterly data for {ticker}")

    # Analyst Data
    stock.recommendations.to_csv(
        f"{path_to_local_home}/data_api/analyst_recommendations/{ticker}.csv"
    )
    stock.earnings_estimate.to_csv(
        f"{path_to_local_home}/data_api/earnings_estimates/{ticker}.csv"
    )
    logging.info(f"Downloaded analyst data for {ticker}")

    # Dividend History
    stock.dividends.to_csv(
        f"{path_to_local_home}/data_api/dividend_history/{ticker}.csv"
    )
    logging.info(f"Downloaded dividend history for {ticker}")

    # Sustainability Data
    sustainability_data = stock.sustainability.to_dict()
    with open(
        f"{path_to_local_home}/data_api/sustainability/{ticker}.json", "w"
    ) as json_file:
        json.dump(sustainability_data, json_file)
    logging.info(f"Downloaded sustainability data for {ticker}")

    # Options Chain
    options_chain = stock.option_chain()
    options_chain.calls.to_csv(
        f"{path_to_local_home}/data_api/options/calls_{ticker}.csv"
    )
    options_chain.puts.to_csv(
        f"{path_to_local_home}/data_api/options/puts_{ticker}.csv"
    )
    logging.info(f"Downloaded options chain for {ticker}")

    return ticker


def upload_single_file_to_gcs(bucket, src_dir):
    logging.info(f"Uploading files from {src_dir} to GCS bucket {bucket}")
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)


def upload_to_gcs(bucket, src_dir, target_dir):
    logging.info(
        f"Uploading files from {src_dir} to GCS bucket {bucket} in directory {target_dir}"
    )
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
    "retries": 10,
}

with DAG(
    dag_id="Data_API_Ingestion",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "financial-data", "yfinance"],
) as dag:

    download_tasks = []

    for ticker in tickers:
        download_task = PythonOperator(
            task_id=f"download_stock_data_{ticker}",
            python_callable=download_stock_data,
            op_kwargs={"ticker": ticker},
            provide_context=True,
        )
        download_tasks.append(download_task)

    upload_single_file_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "src_dir": f"{path_to_local_home}/data_api",
            "target_dir": "raw/api/",
        },
    )

    download_tasks >> upload_single_file_task
