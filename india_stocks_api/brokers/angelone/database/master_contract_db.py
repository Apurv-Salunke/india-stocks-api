# database/master_contract_db.py

import os
import pandas as pd
import requests
from datetime import datetime


# from extensions import socketio  # Import SocketIO - Not needed for our use case
from india_stocks_api.utils.logging import get_logger
from india_stocks_api.config import (
    get_broker_config,
    get_cache_directory,
)
from india_stocks_api.database import (
    initialize_broker_database,
    store_broker_instruments,
)

logger = get_logger(__name__)


def download_json_angel_data(url, output_path):
    """
    Downloads a JSON file from the specified URL and saves it to the specified path.
    """
    logger.info("Downloading JSON data")
    from india_stocks_api.config import get_http_settings

    http_settings = get_http_settings()
    timeout = http_settings["timeout"]
    response = requests.get(url, timeout=timeout)
    if response.status_code == 200:  # Successful download
        with open(output_path, "wb") as f:
            f.write(response.content)
        logger.info("Download complete")
    else:
        logger.error(f"Failed to download data. Status code: {response.status_code}")


def reformat_symbol(row):
    symbol = row["symbol"]
    instrument_type = row["instrumenttype"]

    if instrument_type == "FUT":
        # For FUT, remove the spaces and append 'FUT' at the end
        parts = symbol.split(" ")
        if len(parts) == 5:  # Make sure the symbol has the correct format
            symbol = parts[0] + parts[2] + parts[3] + parts[4] + parts[1]
    elif instrument_type in ["CE", "PE"]:
        # For CE/PE, rearrange the parts and remove spaces
        parts = symbol.split(" ")
        if len(parts) == 6:  # Make sure the symbol has the correct format
            symbol = parts[0] + parts[3] + parts[4] + parts[5] + parts[1] + parts[2]
    else:
        symbol = symbol  # No change for other instrument types

    return symbol


def convert_date(date_str):
    # Convert from '19MAR2024' to '19-MAR-24'
    try:
        return datetime.strptime(date_str, "%d%b%Y").strftime("%d-%b-%y")
    except ValueError:
        # Return the original date if it doesn't match the format
        return date_str


def process_angel_json(path):
    """
    Processes the Angel JSON file to fit the existing database schema.
    Args:
    path (str): The file path of the downloaded JSON data.

    Returns:
    DataFrame: The processed DataFrame ready to be inserted into the database.
    """
    # Read JSON data into a DataFrame
    df = pd.read_json(path)

    # Rename the columns based on the database schema
    # Assuming that the JSON structure matches the sample response provided
    df = df.rename(
        columns={
            "exch_seg": "exchange",
            "instrumenttype": "instrumenttype",
            "lotsize": "lotsize",
            "strike": "strike",
            "symbol": "symbol",
            "token": "token",
            "name": "name",
            "tick_size": "tick_size",
        }
    )

    # Reformat 'symbol' column if needed (based on the given reformat_symbol function)
    # df['symbol'] = df.apply(lambda row: reformat_symbol(row), axis=1)

    # Assuming 'brsymbol' and 'brexchange' are not present in the JSON and are the same as 'symbol' and 'exchange'
    df["brsymbol"] = df["symbol"]
    df["brexchange"] = df["exchange"]

    # Update exchange names based on the instrument type
    df.loc[
        (df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "NSE"), "exchange"
    ] = "NSE_INDEX"
    df.loc[
        (df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "BSE"), "exchange"
    ] = "BSE_INDEX"
    df.loc[
        (df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "MCX"), "exchange"
    ] = "MCX_INDEX"

    # Reformat 'symbol' based on 'brsymbol'
    df["symbol"] = df["symbol"].str.replace("-EQ|-BE|-MF|-SG", "", regex=True)

    # Assuming the 'expiry' field in the JSON is in the format '19MAR2024'
    df["expiry"] = df["expiry"].apply(lambda x: convert_date(x) if pd.notnull(x) else x)
    df["expiry"] = df["expiry"].str.upper()

    # Convert 'strike' to float, 'lotsize' to int, and 'tick_size' to float as per the database schema
    df["strike"] = df["strike"].astype(float) / 100
    df.loc[(df["instrumenttype"] == "OPTCUR") & (df["exchange"] == "CDS"), "strike"] = (
        df["strike"].astype(float) / 100000
    )
    df.loc[(df["instrumenttype"] == "OPTIRC") & (df["exchange"] == "CDS"), "strike"] = (
        df["strike"].astype(float) / 100000
    )

    df["lotsize"] = df["lotsize"].astype(int)
    df["tick_size"] = df["tick_size"].astype(float) / 100  # Divide tick_size by 100

    # Futures Symbol Update in CDS and MCX Exchanges
    df.loc[(df["instrumenttype"] == "FUTCUR") & (df["exchange"] == "CDS"), "symbol"] = (
        df["name"] + df["expiry"].str.replace("-", "", regex=False) + "FUT"
    )
    df.loc[(df["instrumenttype"] == "FUTIRC") & (df["exchange"] == "CDS"), "symbol"] = (
        df["name"] + df["expiry"].str.replace("-", "", regex=False) + "FUT"
    )
    df.loc[(df["instrumenttype"] == "FUTCOM") & (df["exchange"] == "MCX"), "symbol"] = (
        df["name"] + df["expiry"].str.replace("-", "", regex=False) + "FUT"
    )
    # Options Symbol Update in CDS and MCX Exchanges
    df.loc[(df["instrumenttype"] == "OPTCUR") & (df["exchange"] == "CDS"), "symbol"] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + df["symbol"].str[-2:]
    )
    df.loc[(df["instrumenttype"] == "OPTIRC") & (df["exchange"] == "CDS"), "symbol"] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + df["symbol"].str[-2:]
    )
    df.loc[(df["instrumenttype"] == "OPTFUT") & (df["exchange"] == "MCX"), "symbol"] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + df["symbol"].str[-2:]
    )
    # Common Index Symbol Formats

    df["symbol"] = df["symbol"].replace(
        {
            "Nifty 50": "NIFTY",
            "Nifty Next 50": "NIFTYNXT50",
            "Nifty Fin Service": "FINNIFTY",
            "Nifty Bank": "BANKNIFTY",
            "NIFTY MID SELECT": "MIDCPNIFTY",
            "India VIX": "INDIAVIX",
            "SNSX50": "SENSEX50",
        }
    )

    # Return the processed DataFrame
    return df


def delete_angel_temp_data(output_path):
    try:
        # Check if the file exists
        if os.path.exists(output_path):
            # Delete the file
            os.remove(output_path)
            logger.info(f"The temporary file {output_path} has been deleted.")
        else:
            logger.info(f"The temporary file {output_path} does not exist.")
    except Exception as e:
        logger.error(f"An error occurred while deleting the file: {e}")


def master_contract_download():
    logger.info("Downloading Master Contract")

    # Get AngelOne config
    angelone_config = get_broker_config("angelone")
    url = angelone_config["master_contract_url"]

    # Use config for cache directory
    cache_dir = get_cache_directory()
    output_path = cache_dir / "angel.json"
    try:
        download_json_angel_data(url, output_path)
        token_df = process_angel_json(output_path)
        delete_angel_temp_data(output_path)
        # token_df['token'] = pd.to_numeric(token_df['token'], errors='coerce').fillna(-1).astype(int)

        # token_df = token_df.drop_duplicates(subset='symbol', keep='first')

        # Initialize our database and store the data
        initialize_broker_database()

        # Convert DataFrame to list of dictionaries for our database function
        instruments_list = token_df.to_dict("records")
        store_broker_instruments(instruments_list, "angelone")

        logger.info("Successfully Downloaded AngelOne symbols")
        return {"status": "success", "message": "Successfully Downloaded"}

    except Exception as e:
        logger.info(f"{str(e)}")
        logger.error(f"Failed to download AngelOne symbols: {str(e)}")
        return {"status": "error", "message": str(e)}


