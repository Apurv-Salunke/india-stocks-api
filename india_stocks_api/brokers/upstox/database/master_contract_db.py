# database/master_contract_db.py

import os
import pandas as pd
import requests
import gzip
import shutil
from india_stocks_api.utils.logging import get_logger
from india_stocks_api.config import (
    get_broker_config,
    get_database_url,
    get_cache_directory,
)
from india_stocks_api.database import (
    initialize_broker_database,
    store_broker_instruments,
)

logger = get_logger(__name__)


def download_and_unzip_upstox_data(url, input_path, output_path):
    """
    Downloads the compressed JSON from Upstox, unzips it, and saves it to the specified path.
    """
    logger.info("Downloading Upstox Master Contract")
    response = requests.get(url, timeout=10)  # timeout after 10 seconds
    with open(input_path, "wb") as f:
        f.write(response.content)
    logger.info("Decompressing the JSON file")
    with gzip.open(input_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


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


def process_upstox_json(path):
    """
    Processes the Upstox JSON file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Upstox Data")
    df = pd.read_json(path)

    # Filter out NSE_COM instruments
    df = df[df["segment"] != "NSE_COM"]

    # return df

    # Assume your JSON structure requires some transformations to match your schema
    # For the sake of this example, let's assume 'df' now represents your transformed DataFrame
    # Map exchange names
    exchange_map = {
        "NSE_EQ": "NSE",
        "NSE_FO": "NFO",
        "NCD_FO": "CDS",
        "NSE_INDEX": "NSE_INDEX",
        "BSE_INDEX": "BSE_INDEX",
        "BSE_EQ": "BSE",
        "BSE_FO": "BFO",
        "BCD_FO": "BCD",
        "MCX_FO": "MCX",
    }
    segment_copy = df["segment"].copy()
    df["segment"] = df["segment"].map(exchange_map)
    df["expiry"] = (
        pd.to_datetime(df["expiry"], unit="ms").dt.strftime("%d-%b-%y").str.upper()
    )

    df = df[
        [
            "instrument_key",
            "trading_symbol",
            "name",
            "expiry",
            "strike_price",
            "lot_size",
            "instrument_type",
            "segment",
            "tick_size",
        ]
    ].rename(
        columns={
            "instrument_key": "token",
            "trading_symbol": "symbol",
            "name": "name",
            "expiry": "expiry",
            "strike_price": "strike",
            "lot_size": "lotsize",
            "instrument_type": "instrumenttype",
            "segment": "exchange",
            "tick_size": "tick_size",
        }
    )

    df["brsymbol"] = df["symbol"]
    df["symbol"] = df.apply(reformat_symbol, axis=1)
    df["brexchange"] = segment_copy

    df["symbol"] = df["symbol"].replace({"INDIA VIX": "INDIAVIX"})

    return df


def delete_upstox_temp_data(input_path, output_path):
    try:
        # Check if the file exists
        if os.path.exists(input_path) and os.path.exists(output_path):
            # Delete the file
            os.remove(input_path)
            os.remove(output_path)
            logger.info(
                f"The temporary file {input_path} and {output_path} has been deleted."
            )
        else:
            logger.info(
                f"The temporary file {input_path} and {output_path} does not exist."
            )
    except Exception as e:
        logger.error(f"An error occurred while deleting the file: {e}")


def master_contract_download():
    logger.info("Downloading Master Contract")
    
    # Get Upstox config
    upstox_config = get_broker_config("upstox")
    url = upstox_config["master_contract_url"]
    
    # Use config for cache directory
    cache_dir = get_cache_directory()
    input_path = cache_dir / "temp_upstox.json.gz"
    output_path = cache_dir / "upstox.json"
    
    try:
        download_and_unzip_upstox_data(url, input_path, output_path)
        token_df = process_upstox_json(output_path)
        delete_upstox_temp_data(input_path, output_path)
        # token_df['token'] = pd.to_numeric(token_df['token'], errors='coerce').fillna(-1).astype(int)

        # token_df = token_df.drop_duplicates(subset='symbol', keep='first')

        # Initialize our database and store the data
        initialize_broker_database()
        
        # Convert DataFrame to list of dictionaries for our database function
        instruments_list = token_df.to_dict("records")
        store_broker_instruments(instruments_list, "upstox")

        logger.info("Successfully Downloaded Upstox symbols")
        return {"status": "success", "message": "Successfully Downloaded"}

    except Exception as e:
        logger.info(f"{str(e)}")
        logger.error(f"Failed to download Upstox symbols: {str(e)}")
        return {"status": "error", "message": str(e)}


