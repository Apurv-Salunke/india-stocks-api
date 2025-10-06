# database/master_contract_db.py

import os
import pandas as pd
import httpx
from typing import List, Tuple, Optional
from india_stocks_api.utils.logging import get_logger
from india_stocks_api.utils.httpx_client import get_http_client
from india_stocks_api.config import (
    get_broker_config,
    get_cache_directory,
)
from india_stocks_api.database import (
    initialize_broker_database,
    store_broker_instruments,
)

logger = get_logger(__name__)


# Define the headers as provided
headers = [
    "Fytoken",
    "Symbol Details",
    "Exchange Instrument type",
    "Minimum lot size",
    "Tick size",
    "ISIN",
    "Trading Session",
    "Last update date",
    "Expiry date",
    "Symbol ticker",
    "Exchange",
    "Segment",
    "Scrip code",
    "Underlying symbol",
    "Underlying scrip code",
    "Strike price",
    "Option type",
    "Underlying FyToken",
    "Reserved column1",
    "Reserved column2",
    "Reserved column3",
]

# Data types for each header
data_types = {
    "Fytoken": str,
    "Symbol Details": str,
    "Exchange Instrument type": int,
    "Minimum lot size": int,
    "Tick size": float,
    "ISIN": str,
    "Trading Session": str,
    "Last update date": str,
    "Expiry date": str,
    "Symbol ticker": str,
    "Exchange": int,
    "Segment": int,
    "Scrip code": int,
    "Underlying symbol": str,
    "Underlying scrip code": pd.Int64Dtype(),
    "Strike price": float,
    "Option type": str,
    "Underlying FyToken": str,
    "Reserved column1": str,
    "Reserved column2": str,
    "Reserved column3": str,
}



def download_csv_fyers_data(output_path: str) -> Tuple[bool, List[str], Optional[str]]:
    """
    Download Fyers master contract CSV files using a shared HTTPX client with connection pooling.

    Args:
        output_path (str): Directory path where the CSV files will be saved

    Returns:
        Tuple[bool, List[str], Optional[str]]:
            - bool: True if all downloads were successful, False otherwise
            - List[str]: List of paths to downloaded files
            - Optional[str]: Error message if any error occurred, None otherwise
    """
    logger.info("Downloading Master Contract CSV Files")

    # Get Fyers URLs from config
    fyers_config = get_broker_config("fyers")
    csv_urls = fyers_config["master_contract_urls"]

    downloaded_files = []
    errors = []

    # Get the shared HTTPX client with connection pooling
    client = get_http_client()

    try:
        for key, url in csv_urls.items():
            try:
                response = client.get(url, timeout=30.0)
                response.raise_for_status()  # Raises an exception for 4XX/5XX responses

                file_path = os.path.join(output_path, f"{key}.csv")
                with open(file_path, "wb") as file:
                    file.write(response.content)
                downloaded_files.append(file_path)
                logger.info(f"Successfully downloaded {key} to {file_path}")

            except httpx.HTTPStatusError as e:
                error_msg = f"HTTP error occurred while downloading {key} from {url}: {e.response.status_code} {e.response.reason_phrase}"
                logger.error(error_msg)
                errors.append(error_msg)
            except httpx.RequestError as e:
                error_msg = (
                    f"Request error occurred while downloading {key} from {url}: {e}"
                )
                logger.error(error_msg)
                errors.append(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error downloading {key}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
    finally:
        # Don't close the client as it's shared
        pass

    # Determine success/failure based on whether we got all files
    success = len(downloaded_files) == len(csv_urls)
    error_msg = "; ".join(errors) if errors else None

    return success, downloaded_files, error_msg


def reformat_symbol_detail(s):
    parts = s.split()  # Split the string into parts
    # Reorder and format the parts to match the desired output
    # Assuming the format is consistent and always "Name DD Mon YY FUT"
    return f"{parts[0]}{parts[3]}{parts[2].upper()}{parts[1]}{parts[4]}"


def process_fyers_nse_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers NSE CSV Data")
    file_path = f"{path}/NSE_CM.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    # Assigning headers to the DataFrame
    df.columns = headers

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]
    df["expiry"] = df["Expiry date"]
    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]

    # Filtering the DataFrame based on 'Exchange Instrument type' and assigning values to 'exchange'
    df.loc[df["Exchange Instrument type"].isin([0, 9]), "exchange"] = "NSE"
    df.loc[df["Exchange Instrument type"].isin([0, 9]), "instrumenttype"] = "EQ"
    df.loc[df["Exchange Instrument type"] == 10, "exchange"] = "NSE_INDEX"
    df.loc[df["Exchange Instrument type"] == 10, "instrumenttype"] = "INDEX"

    # Keeping only rows where 'exchange' column has been filled ('NSE' or 'NSE_INDEX')
    df_filtered = df[df["Exchange Instrument type"].isin([0, 9, 10])].copy()

    df_filtered.loc[:, "symbol"] = df_filtered["Underlying symbol"]
    df_filtered["brexchange"] = "NSE"

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df_filtered.drop(columns=columns_to_remove)

    return token_df


def process_fyers_bse_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers BSE CSV Data")
    file_path = f"{path}/BSE_CM.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    # Assigning headers to the DataFrame
    df.columns = headers

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]
    df["expiry"] = df["Expiry date"]
    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]

    # Filtering the DataFrame based on 'Exchange Instrument type' and assigning values to 'exchange'
    df.loc[df["Exchange Instrument type"].isin([0, 4, 50]), "exchange"] = "BSE"
    df.loc[df["Exchange Instrument type"].isin([0, 4, 50]), "instrumenttype"] = "EQ"
    df.loc[df["Exchange Instrument type"] == 10, "exchange"] = "BSE_INDEX"
    df.loc[df["Exchange Instrument type"] == 10, "instrumenttype"] = "INDEX"

    # Keeping only rows where 'exchange' column has been filled ('BSE' or 'BSE_INDEX')
    df_filtered = df[df["Exchange Instrument type"].isin([0, 4, 10, 50])].copy()

    df_filtered.loc[:, "symbol"] = df_filtered["Underlying symbol"]

    df_filtered["brexchange"] = "BSE"

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df_filtered.drop(columns=columns_to_remove)

    return token_df


def process_fyers_nfo_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers NFO CSV Data")
    file_path = f"{path}/NSE_FO.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]

    # Convert 'Expiry date' from Unix timestamp to datetime
    # First convert string to numeric to avoid FutureWarning
    df["expiry"] = pd.to_datetime(
        pd.to_numeric(df["Expiry date"], errors="coerce"), unit="s"
    )

    # Format the datetime object to the desired format '15-APR-24'
    df["expiry"] = df["expiry"].dt.strftime("%d-%b-%y").str.upper()

    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]
    df["brexchange"] = "NFO"
    df["exchange"] = "NFO"
    df["instrumenttype"] = df["Option type"].str.replace("XX", "FUT")

    # Apply the function to rows where 'Option type' is 'XX'
    df.loc[df["Option type"] == "XX", "symbol"] = df["Symbol Details"].apply(
        lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
    )
    df.loc[df["Option type"] == "CE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "CE"
    )
    df.loc[df["Option type"] == "PE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "PE"
    )

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df.drop(columns=columns_to_remove)

    return token_df


def process_fyers_cds_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers CDS CSV Data")
    file_path = f"{path}/NSE_CD.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]

    # Convert 'Expiry date' from Unix timestamp to datetime
    # First convert string to numeric to avoid FutureWarning
    df["expiry"] = pd.to_datetime(
        pd.to_numeric(df["Expiry date"], errors="coerce"), unit="s"
    )

    # Format the datetime object to the desired format '15-APR-24'
    df["expiry"] = df["expiry"].dt.strftime("%d-%b-%y").str.upper()

    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]
    df["brexchange"] = "CDS"
    df["exchange"] = "CDS"
    df["instrumenttype"] = df["Option type"].str.replace("XX", "FUT")

    # Apply the function to rows where 'Option type' is 'XX'
    df.loc[df["Option type"] == "XX", "symbol"] = df["Symbol Details"].apply(
        lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
    )
    df.loc[df["Option type"] == "CE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "CE"
    )
    df.loc[df["Option type"] == "PE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "PE"
    )

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df.drop(columns=columns_to_remove)

    return token_df


def process_fyers_bfo_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers BFO CSV Data")
    file_path = f"{path}/BSE_FO.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]

    # Convert 'Expiry date' from Unix timestamp to datetime
    # First convert string to numeric to avoid FutureWarning
    df["expiry"] = pd.to_datetime(
        pd.to_numeric(df["Expiry date"], errors="coerce"), unit="s"
    )

    # Format the datetime object to the desired format '15-APR-24'
    df["expiry"] = df["expiry"].dt.strftime("%d-%b-%y").str.upper()

    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]
    df["brexchange"] = "BFO"
    df["exchange"] = "BFO"
    df["instrumenttype"] = df["Option type"].fillna("FUT").str.replace("XX", "FUT")

    # Apply the function to rows where 'Option type' is 'XX'
    df.loc[(df["Option type"] == "XX") | df["Option type"].isna(), "symbol"] = df[
        "Symbol Details"
    ].apply(lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x)
    df.loc[df["Option type"] == "CE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "CE"
    )
    df.loc[df["Option type"] == "PE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "PE"
    )

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df.drop(columns=columns_to_remove)

    return token_df


def process_fyers_mcx_csv(path):
    """
    Processes the Fyers CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Fyers MCX CSV Data")
    file_path = f"{path}/MCX_COM.csv"

    df = pd.read_csv(file_path, names=headers, dtype=data_types)

    df["token"] = df["Fytoken"]
    df["name"] = df["Symbol Details"]

    # Convert 'Expiry date' from Unix timestamp to datetime
    # First convert string to numeric to avoid FutureWarning
    df["expiry"] = pd.to_datetime(
        pd.to_numeric(df["Expiry date"], errors="coerce"), unit="s"
    )

    # Format the datetime object to the desired format '15-APR-24'
    df["expiry"] = df["expiry"].dt.strftime("%d-%b-%y").str.upper()

    df["strike"] = df["Strike price"]
    df["lotsize"] = df["Minimum lot size"]
    df["tick_size"] = df["Tick size"]
    df["brsymbol"] = df["Symbol ticker"]
    df["brexchange"] = "MCX"
    df["exchange"] = "MCX"
    df["instrumenttype"] = df["Option type"].str.replace("XX", "FUT")

    # Apply the function to rows where 'Option type' is 'XX'
    df.loc[df["Option type"] == "XX", "symbol"] = df["Symbol Details"].apply(
        lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
    )
    df.loc[df["Option type"] == "CE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "CE"
    )
    df.loc[df["Option type"] == "PE", "symbol"] = (
        df["Symbol Details"].apply(
            lambda x: reformat_symbol_detail(x) if pd.notnull(x) else x
        )
        + "PE"
    )

    # List of columns to remove
    columns_to_remove = [
        "Fytoken",
        "Symbol Details",
        "Exchange Instrument type",
        "Minimum lot size",
        "Tick size",
        "ISIN",
        "Trading Session",
        "Last update date",
        "Expiry date",
        "Symbol ticker",
        "Exchange",
        "Segment",
        "Scrip code",
        "Underlying symbol",
        "Underlying scrip code",
        "Strike price",
        "Option type",
        "Underlying FyToken",
        "Reserved column1",
        "Reserved column2",
        "Reserved column3",
    ]

    # Removing the specified columns
    token_df = df.drop(columns=columns_to_remove)

    return token_df


def delete_fyers_temp_data(output_path):
    # Check each file in the directory
    for filename in os.listdir(output_path):
        # Construct the full file path
        file_path = os.path.join(output_path, filename)
        # If the file is a CSV, delete it
        if filename.endswith(".csv") and os.path.isfile(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted {file_path}")
            except OSError as e:
                logger.warning(f"Error deleting file {file_path}: {e}")


def master_contract_download():
    logger.info("Downloading Master Contract")

    # Use config for cache directory
    cache_dir = get_cache_directory()
    output_path = cache_dir / "fyers"
    output_path.mkdir(exist_ok=True)
    
    try:
        download_csv_fyers_data(output_path)
        
        # Initialize our database
        initialize_broker_database()
        
        # Process exchange data and store in our database
        all_instruments = []
        
        # Process each exchange and collect all instruments
        token_df = process_fyers_nse_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))
        
        token_df = process_fyers_bse_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))
        
        token_df = process_fyers_bfo_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))
        
        token_df = process_fyers_nfo_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))
        
        token_df = process_fyers_cds_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))
        
        token_df = process_fyers_mcx_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        # Store all instruments in our database
        store_broker_instruments(all_instruments, "fyers")

        delete_fyers_temp_data(output_path)

        logger.info("Successfully Downloaded Fyers symbols")
        return {"status": "success", "message": "Successfully Downloaded"}

    except Exception as e:
        logger.exception(f"{e}")
        logger.error(f"Failed to download Fyers symbols: {str(e)}")
        return {"status": "error", "message": str(e)}


