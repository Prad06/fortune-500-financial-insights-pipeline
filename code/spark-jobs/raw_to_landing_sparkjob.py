from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import lit, col, to_date, year, regexp_replace, row
from pyspark.sql.window import Window
import pandas as pd

# Standardize Kaggle Data
def standardize_kaggle_data(spark, local_path):
    """
    Standardize Kaggle data with consistent schema.
    """
    kaggle_schema = StructType([
        StructField("Date", StringType(), True),  # Read as string first, then convert
        StructField("Open", StringType(), True),  # Read as string to handle any formatting issues
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("Volume", StringType(), True),  # Volume as string due to commas
        StructField("OpenInt", StringType(), True)  # Assuming OpenInt may be unnecessary
    ])
    
    # Read the Kaggle data with the defined schema
    kaggle_df = spark.read.csv(
        local_path,
        schema=kaggle_schema,
        header=True
    )

    # Convert columns to proper data types
    kaggle_df = kaggle_df.withColumn(
        "Date", 
        to_date(col("Date"), "yyyy-MM-dd")
    ).filter(col("Date").isNotNull())  # Remove rows with invalid dates

    # Cast numeric columns
    kaggle_df = kaggle_df.withColumn(
        "Open", col("Open").cast("double")
    ).withColumn(
        "High", col("High").cast("double")
    ).withColumn(
        "Low", col("Low").cast("double")
    ).withColumn(
        "Close", col("Close").cast("double")
    ).withColumn(
        "Volume", regexp_replace(col("Volume"), ",", "").cast("double")  # Remove commas
    ).drop("OpenInt")  # Drop unnecessary columns

    # Add a source column to mark the source of the data
    kaggle_df = kaggle_df.withColumn("Source", lit("Kaggle"))

    return kaggle_df

# Standardize API Data
def standardize_api_data(spark, local_path):
    """
    Standardize API CSV data with consistent schema.
    """
    api_schema = StructType([
        StructField("Date", StringType(), True),  # Read as string first, then convert
        StructField("AdjClose", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Open", StringType(), True),
        StructField("Volume", StringType(), True)  # Volume as string due to commas
    ])

    # Read the raw API data with the schema
    raw_df = spark.read.csv(
        local_path,
        schema=api_schema,
        header=False  # The file contains metadata in the first few rows
    )

    # Add a row number column to filter out the first few rows with metadata
    window_spec = Window.orderBy(lit(1))
    indexed_df = raw_df.withColumn("row_index", row_number().over(window_spec))
    
    # Filter out the first 4 rows, which may contain metadata
    filtered_df = indexed_df.filter(col("row_index") > 4).drop("row_index")

    # Clean the Date column and ensure it's in the correct format
    api_standardized = filtered_df.select(
        to_date(col("Date"), "yyyy-MM-dd").alias("Date"),
        col("Open").cast("double").alias("Open"),
        col("High").cast("double").alias("High"),
        col("Low").cast("double").alias("Low"),
        col("Close").cast("double").alias("Close"),
        col("AdjClose").cast("double").alias("AdjClose"),
        regexp_replace(col("Volume"), ",", "").cast("double").alias("Volume")
    ).withColumn("Source", lit("API")).filter(col("Date").isNotNull())  # Remove rows with invalid dates

    return api_standardized

# Combine Kaggle and API data
def move_raw_to_staging(spark, kaggle_path, api_path, TICKER):
    """
    Combine and process stock data from Kaggle and API sources.
    """
    try:
        # Standardize Kaggle data
        kaggle_df = standardize_kaggle_data(spark, kaggle_path)
        print("Kaggle data standardized")
    except Exception as e:
        print(f"Error reading Kaggle data for {TICKER}: {e}")
        return None  # Skip this ticker if Kaggle data is missing

    try:
        # Standardize API data
        api_df = standardize_api_data(spark, api_path)
        print("API data standardized")
    except Exception as e:
        print(f"Error reading API data for {TICKER}: {e}")
        return None  # Skip this ticker if API data is missing
    
    # Combine data and add year and ticker
    api_df = api_df.drop("AdjClose")
    kaggle_df = kaggle_df.drop("OpenInt")
    
    combined_df = kaggle_df.unionByName(api_df)
    final_df = combined_df.withColumn("year", year(combined_df.Date)) \
                          .withColumn("Ticker", lit(f"{TICKER}"))
    
    return final_df

def get_file_names_from_csv(csv_path):
    print(f"Reading CSV file from path: {csv_path}")
    df = pd.read_csv(csv_path)
    tickers = df["Ticker"].tolist()
    print(f"Tickers extracted: {tickers}")
    return tickers

# Main logic
spark = SparkSession.builder \
    .appName("Fortune500") \
    .getOrCreate()

temp_bucket = "dataproc-temp-us-east1-327585098176-6nrezoqb"
spark.conf.set('temporaryGcsBucket', temp_bucket)

GCP_RAW_BUCKET = "finance-raw-ingest_data-engineering-finance"
GCP_LANDING_BUCKET = "finance-spark-staging_data-engineering-finance"

CSV_PATH = f"gs://{GCP_RAW_BUCKET}/csv/stock_list.csv"
tickers = get_file_names_from_csv(CSV_PATH)

final_df_list = []  # List to collect DataFrames for each ticker

# Loop through each ticker, process the data, and append to final_df_list
for t in tickers:
    TICKER = t.upper()  # Ensure the ticker is in uppercase
    TICKER_LOWER = TICKER.lower()
    api_path = f"gs://{GCP_RAW_BUCKET}/raw/api/open_close/{TICKER}.csv"
    kaggle_path = f"gs://{GCP_RAW_BUCKET}/raw/kaggle/{TICKER_LOWER}.us.txt"

    try:
        final_df = move_raw_to_staging(spark, kaggle_path, api_path, TICKER)
        if final_df is not None:
            final_df_list.append(final_df)
        print(f"Processed {TICKER}")
    except Exception as e:
        print(f"An error occurred while processing {TICKER}: {e}")
        continue

# Combine all the dataframes for different tickers
if final_df_list:
    combined_final_df = final_df_list[0]
    for df in final_df_list[1:]:
        combined_final_df = combined_final_df.unionByName(df)

    # Write combined data to the landing bucket using INSERT OVERWRITE
    combined_final_df.write \
        .partitionBy("Ticker", "year") \
        .mode("overwrite") \
        .parquet(f"gs://{GCP_LANDING_BUCKET}/stock_data/")
    print("Final data written successfully")
else:
    print("No data to write")

spark.stop()
