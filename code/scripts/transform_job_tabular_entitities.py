import argparse
import logging
import os

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_tabular_data_cleaned(file_path, columns_interested_in, ticker):
    try:
        df = pd.read_csv(file_path)

        if "Unnamed: 0" in df.columns:
            df = df.rename(columns={"Unnamed: 0": "Date"})

        df = df[df["Date"].isin(columns_interested_in)]

        df = df.T.reset_index()

        df.columns = df.iloc[0]
        df = df[1:].copy()

        df["Ticker"] = ticker

        return df
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        raise


def get_file_names_from_csv(csv_path):
    try:
        logger.info(f"Reading CSV file from path: {csv_path}")
        df = pd.read_csv(csv_path)

        if "Ticker" not in df.columns:
            raise ValueError("CSV file must contain a 'Ticker' column")

        tickers = df["Ticker"].tolist()
        logger.info(f"Extracted {len(tickers)} tickers")
        return tickers
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {str(e)}")
        raise


def main(
    input_bucket,
    input_file_path,
    output_bucket,
    columns_interested_in,
    tickers,
):
    sheets = ["balance_sheet", "cash_flow", "income_statement", "quarterly"]

    for sheet in sheets:
        if sheet not in columns_interested_in:
            logger.warning(f"No columns specified for sheet {sheet}, skipping")
            continue

        base_path = f"gs://{input_bucket}/{input_file_path}/{sheet}"
        dump_path = f"gs://{output_bucket}/{sheet}"
        df_list = []

        for ticker in tickers:
            filename = f"{ticker}.csv"
            file_path = f"{base_path}/{filename}"

            try:
                logger.info(f"Processing {filename} for sheet {sheet}")
                df_returned = get_tabular_data_cleaned(
                    file_path, columns_interested_in[sheet], ticker
                )
                df_list.append(df_returned)
            except Exception as e:
                logger.error(f"Failed to process {filename}: {str(e)}")
                continue

        if not df_list:
            logger.warning(f"No data processed for sheet {sheet}")
            continue

        try:
            df_all_combined = pd.concat(df_list, ignore_index=True)

            combined_parquet_path = f"{dump_path}/{sheet}.parquet"
            df_all_combined.to_parquet(combined_parquet_path)
            logger.info(f"Successfully processed and saved sheet {sheet}")
        except Exception as e:
            logger.error(f"Error combining/saving data for sheet {sheet}: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process financial data files")
    parser.add_argument(
        "--input-bucket", type=str, required=True, help="GCS input bucket name"
    )
    parser.add_argument(
        "--input-file-path", type=str, required=True, help="Path within input bucket"
    )
    parser.add_argument(
        "--output-bucket", type=str, required=True, help="GCS output bucket name"
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        required=True,
        help="Path to CSV file containing ticker symbols",
    )

    args = parser.parse_args()
    logger.info(f"Starting transformation with arguments: {args}")

    columns_interested_in = {
        "balance_sheet": [
            "Treasury Shares Number",
            "Ordinary Shares Number",
            "Share Issued",
            "Net Debt",
            "Total Debt",
            "Tangible Book Value",
            "Invested Capital",
            "Working Capital",
            "Net Tangible Assets",
            "Capital Lease Obligations",
            "Common Stock Equity",
            "Total Capitalization",
            "Total Equity Gross Minority Interest",
            "Stockholders Equity",
            "Gains Losses Not Affecting Retained Earnings",
            "Other Equity Adjustments",
            "Retained Earnings",
            "Capital Stock",
            "Common Stock",
            "Total Liabilities Net Minority Interest",
            "Total Non Current Liabilities Net Minority Interest",
            "Other Non Current Liabilities",
            "Tradeand Other Payables Non Current",
            "Long Term Debt And Capital Lease Obligation",
            "Long Term Capital Lease Obligation",
            "Long Term Debt",
            "Current Liabilities",
            "Other Current Liabilities",
            "Current Deferred Liabilities",
            "Current Deferred Revenue",
            "Current Debt And Capital Lease Obligation",
            "Current Capital Lease Obligation",
            "Current Debt",
            "Other Current Borrowings",
            "Commercial Paper",
            "Payables And Accrued Expenses",
            "Payables",
            "Total Tax Payable",
            "Income Tax Payable",
            "Accounts Payable",
            "Total Assets",
            "Total Non Current Assets",
            "Other Non Current Assets",
            "Non Current Deferred Assets",
            "Non Current Deferred Taxes Assets",
            "Investments And Advances",
            "Other Investments",
            "Investmentin Financial Assets",
            "Available For Sale Securities",
            "Net PPE",
            "Accumulated Depreciation",
            "Gross PPE",
            "Leases",
            "Other Properties",
            "Machinery Furniture Equipment",
            "Land And Improvements",
            "Properties",
            "Current Assets",
            "Other Current Assets",
            "Inventory",
        ],
        "cash_flow": [
            "Free Cash Flow",
            "Repurchase Of Capital Stock",
            "Repayment Of Debt",
            "Issuance Of Debt",
            "Issuance Of Capital Stock",
            "Capital Expenditure",
            "Interest Paid Supplemental Data",
            "Income Tax Paid Supplemental Data",
            "End Cash Position",
            "Beginning Cash Position",
            "Changes In Cash",
            "Financing Cash Flow",
            "Cash Flow From Continuing Financing Activities",
            "Net Other Financing Charges",
            "Cash Dividends Paid",
            "Common Stock Dividend Paid",
            "Net Common Stock Issuance",
            "Common Stock Payments",
            "Common Stock Issuance",
            "Net Issuance Payments Of Debt",
            "Net Short Term Debt Issuance",
            "Net Long Term Debt Issuance",
            "Long Term Debt Payments",
            "Long Term Debt Issuance",
            "Investing Cash Flow",
            "Cash Flow From Continuing Investing Activities",
            "Net Other Investing Changes",
            "Net Investment Purchase And Sale",
            "Sale Of Investment",
            "Purchase Of Investment",
            "Net Business Purchase And Sale",
            "Purchase Of Business",
            "Net PPE Purchase And Sale",
            "Purchase Of PPE",
            "Operating Cash Flow",
            "Cash Flow From Continuing Operating Activities",
            "Change In Working Capital",
            "Change In Other Working Capital",
            "Change In Other Current Liabilities",
            "Change In Other Current Assets",
            "Change In Payables And Accrued Expense",
            "Change In Payable",
            "Change In Account Payable",
            "Change In Inventory",
            "Change In Receivables",
            "Changes In Account Receivables",
            "Other Non Cash Items",
            "Stock Based Compensation",
            "Deferred Tax",
            "Deferred Income Tax",
            "Depreciation Amortization Depletion",
            "Depreciation And Amortization",
            "Net Income From Continuing Operations",
        ],
        "income_statement": [
            "Tax Effect Of Unusual Items",
            "Tax Rate For Calcs",
            "Normalized EBITDA",
            "Net Income From Continuing Operation Net Minority Interest",
            "Reconciled Depreciation",
            "Reconciled Cost Of Revenue",
            "EBITDA",
            "EBIT",
            "Net Interest Income",
            "Interest Expense",
            "Interest Income",
            "Normalized Income",
            "Net Income From Continuing And Discontinued Operation",
            "Total Expenses",
            "Total Operating Income As Reported",
            "Diluted Average Shares",
            "Basic Average Shares",
            "Diluted EPS",
            "Basic EPS",
            "Diluted NI Availto Com Stockholders",
            "Net Income Common Stockholders",
            "Net Income",
            "Net Income Including Noncontrolling Interests",
            "Net Income Continuous Operations",
            "Tax Provision",
            "Pretax Income",
            "Other Income Expense",
            "Other Non Operating Income Expenses",
            "Net Non Operating Interest Income Expense",
            "Interest Expense Non Operating",
            "Interest Income Non Operating",
            "Operating Income",
            "Operating Expense",
            "Research And Development",
            "Selling General And Administration",
            "Gross Profit",
            "Cost Of Revenue",
            "Total Revenue",
            "Operating Revenue",
        ],
        "quarterly": [
            "Tax Effect Of Unusual Items",
            "Tax Rate For Calcs",
            "Normalized EBITDA",
            "Net Income From Continuing Operation Net Minority Interest",
            "Reconciled Depreciation",
            "Reconciled Cost Of Revenue",
            "EBITDA",
            "EBIT",
            "Net Interest Income",
            "Interest Expense",
            "Interest Income",
            "Normalized Income",
            "Net Income From Continuing And Discontinued Operation",
            "Total Expenses",
            "Total Operating Income As Reported",
            "Diluted Average Shares",
            "Basic Average Shares",
            "Diluted EPS",
            "Basic EPS",
            "Diluted NI Availto Com Stockholders",
            "Net Income Common Stockholders",
            "Net Income",
            "Net Income Including Noncontrolling Interests",
            "Net Income Continuous Operations",
            "Tax Provision",
            "Pretax Income",
            "Other Income Expense",
            "Other Non Operating Income Expenses",
            "Net Non Operating Interest Income Expense",
            "Interest Expense Non Operating",
            "Interest Income Non Operating",
            "Operating Income",
            "Operating Expense",
            "Research And Development",
            "Selling General And Administration",
            "Gross Profit",
            "Cost Of Revenue",
            "Total Revenue",
            "Operating Revenue",
        ],
    }

    try:
        tickers = get_file_names_from_csv(args.csv_path)
        main(
            args.input_bucket,
            args.input_file_path,
            args.output_bucket,
            columns_interested_in,
            tickers,
        )
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        exit(1)
