"""
Angel One Master Contract Database Module
Ported from OpenAlgo, adapted for india_stocks_api standalone library.
"""

import os
from datetime import datetime

import pandas as pd
import requests

from india_stocks_api.internal.context import get_logger

logger = get_logger(__name__)


def download_json_angel_data(url, output_path):
    """
    Downloads a JSON file from the specified URL and saves it to the specified path.
    """
    logger.info("Downloading Angel One master contract JSON data...")

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    response = requests.get(url, timeout=30)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        logger.info("Download complete")
    else:
        raise Exception(f"Failed to download data. Status code: {response.status_code}")


def convert_date(date_str):
    """Convert from '19MAR2024' to '19-MAR-24'"""
    try:
        return datetime.strptime(date_str, "%d%b%Y").strftime("%d-%b-%y")
    except ValueError:
        return date_str


def process_angel_json(path):
    """
    Processes the Angel JSON file to fit our database schema.
    Args:
        path (str): The file path of the downloaded JSON data.

    Returns:
        DataFrame: The processed DataFrame ready to be inserted into the database.
    """
    logger.info("Processing Angel One JSON data...")

    # Read JSON data into a DataFrame
    df = pd.read_json(path)

    # Rename the columns based on the database schema
    df = df.rename(
        columns={
            "exch_seg": "exchange",
            "instrumenttype": "instrumenttype",
            "lotsize": "lot_size",
            "strike": "strike",
            "symbol": "symbol",
            "token": "token",
            "name": "name",
            "tick_size": "tick_size",
        }
    )

    # Populate br_symbol and tradingsymbol (before modifications)
    df["br_symbol"] = df["symbol"]
    df["tradingsymbol"] = df["symbol"]

    # Update exchange names based on the instrument type
    df.loc[(df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "NSE"), "exchange"] = "NSE_INDEX"
    df.loc[(df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "BSE"), "exchange"] = "BSE_INDEX"
    df.loc[(df["instrumenttype"] == "AMXIDX") & (df["exchange"] == "MCX"), "exchange"] = "MCX_INDEX"

    # Reformat 'symbol' based on 'br_symbol' - remove suffixes
    df["symbol"] = df["symbol"].str.replace("-EQ|-BE|-MF|-SG", "", regex=True)

    # Process expiry dates
    df["expiry"] = df["expiry"].apply(lambda x: convert_date(x) if pd.notnull(x) else x)
    df["expiry"] = df["expiry"].str.upper()

    # Convert 'strike' to float, 'lot_size' to int, and 'tick_size' to float
    df["strike"] = df["strike"].astype(float) / 100
    df.loc[(df["instrumenttype"] == "OPTCUR") & (df["exchange"] == "CDS"), "strike"] = (
        df["strike"].astype(float) / 100000
    )
    df.loc[(df["instrumenttype"] == "OPTIRC") & (df["exchange"] == "CDS"), "strike"] = (
        df["strike"].astype(float) / 100000
    )

    df["lot_size"] = df["lot_size"].astype(int)
    df["tick_size"] = df["tick_size"].astype(float) / 100

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

    # BFO Index Futures Symbol Update
    df.loc[(df["instrumenttype"] == "FUTIDX") & (df["exchange"] == "BFO"), "symbol"] = (
        df["name"] + df["expiry"].str.replace("-", "", regex=False) + "FUT"
    )
    df.loc[(df["instrumenttype"] == "FUTSTK") & (df["exchange"] == "BFO"), "symbol"] = (
        df["name"] + df["expiry"].str.replace("-", "", regex=False) + "FUT"
    )

    # BFO Index Options Symbol Update
    df.loc[
        (df["instrumenttype"] == "OPTIDX") & (df["exchange"] == "BFO") & (df["symbol"].str.endswith("CE", na=False)),
        "symbol",
    ] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + "CE"
    )
    df.loc[
        (df["instrumenttype"] == "OPTIDX") & (df["exchange"] == "BFO") & (df["symbol"].str.endswith("PE", na=False)),
        "symbol",
    ] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + "PE"
    )

    # BFO Stock Options Symbol Update
    df.loc[
        (df["instrumenttype"] == "OPTSTK") & (df["exchange"] == "BFO") & (df["symbol"].str.endswith("CE", na=False)),
        "symbol",
    ] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + "CE"
    )
    df.loc[
        (df["instrumenttype"] == "OPTSTK") & (df["exchange"] == "BFO") & (df["symbol"].str.endswith("PE", na=False)),
        "symbol",
    ] = (
        df["name"]
        + df["expiry"].str.replace("-", "", regex=False)
        + df["strike"].astype(str).str.replace(r"\.0", "", regex=True)
        + "PE"
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

    # Convert instrumenttype from OPTIDX/OPTSTK to CE/PE
    df.loc[(df["instrumenttype"] == "OPTIDX") & (df["symbol"].str.endswith("CE", na=False)), "instrumenttype"] = "CE"
    df.loc[(df["instrumenttype"] == "OPTIDX") & (df["symbol"].str.endswith("PE", na=False)), "instrumenttype"] = "PE"
    df.loc[(df["instrumenttype"] == "OPTSTK") & (df["symbol"].str.endswith("CE", na=False)), "instrumenttype"] = "CE"
    df.loc[(df["instrumenttype"] == "OPTSTK") & (df["symbol"].str.endswith("PE", na=False)), "instrumenttype"] = "PE"

    # Convert MCX OPTFUT to CE/PE
    df.loc[(df["instrumenttype"] == "OPTFUT") & (df["symbol"].str.endswith("CE", na=False)), "instrumenttype"] = "CE"
    df.loc[(df["instrumenttype"] == "OPTFUT") & (df["symbol"].str.endswith("PE", na=False)), "instrumenttype"] = "PE"

    # Convert CDS OPTCUR/OPTIRC to CE/PE
    df.loc[(df["instrumenttype"] == "OPTCUR") & (df["symbol"].str.endswith("CE", na=False)), "instrumenttype"] = "CE"
    df.loc[(df["instrumenttype"] == "OPTCUR") & (df["symbol"].str.endswith("PE", na=False)), "instrumenttype"] = "PE"
    df.loc[(df["instrumenttype"] == "OPTIRC") & (df["symbol"].str.endswith("CE", na=False)), "instrumenttype"] = "CE"
    df.loc[(df["instrumenttype"] == "OPTIRC") & (df["symbol"].str.endswith("PE", na=False)), "instrumenttype"] = "PE"

    # Convert all futures instrument types to 'FUT' for consistency
    df.loc[df["instrumenttype"] == "FUTIDX", "instrumenttype"] = "FUT"
    df.loc[df["instrumenttype"] == "FUTSTK", "instrumenttype"] = "FUT"
    df.loc[df["instrumenttype"] == "FUTCOM", "instrumenttype"] = "FUT"
    df.loc[df["instrumenttype"] == "FUTCUR", "instrumenttype"] = "FUT"
    df.loc[df["instrumenttype"] == "FUTIRC", "instrumenttype"] = "FUT"
    df.loc[df["instrumenttype"] == "FUTIRT", "instrumenttype"] = "FUT"

    # Map to our DB schema: instrument_type
    df["instrument_type"] = df["instrumenttype"]

    # Determine opt_type from instrument_type
    df["opt_type"] = None
    df.loc[df["instrument_type"] == "CE", "opt_type"] = "CE"
    df.loc[df["instrument_type"] == "PE", "opt_type"] = "PE"

    # Final cleanup: select only columns we need
    final_df = df[
        [
            "token",
            "symbol",
            "exchange",
            "tradingsymbol",
            "br_symbol",
            "expiry",
            "strike",
            "opt_type",
            "lot_size",
            "tick_size",
            "instrument_type",
        ]
    ]

    logger.info(f"Processed {len(final_df)} instruments")
    return final_df


def delete_temp_file(output_path):
    """Delete temporary download file."""
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
            logger.info(f"Deleted temporary file: {output_path}")
    except Exception as e:
        logger.error(f"Error deleting temporary file: {e}")


def master_contract_download(db_path="instruments.db"):
    """
    Main entry point: Downloads Angel One master contract and populates DB.

    Args:
        db_path: Path to the instruments database

    Returns:
        int: Number of instruments inserted
    """
    logger.info("Starting Angel One master contract download...")

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    output_path = "tmp/angel.json"

    try:
        # Download raw JSON
        download_json_angel_data(url, output_path)

        # Process and normalize
        token_df = process_angel_json(output_path)

        # Delete temp file
        delete_temp_file(output_path)

        # Convert DataFrame to list of dicts for our DB
        records = token_df.to_dict("records")

        # Import here to avoid circular dependency
        from ....instruments.database import InstrumentDB

        db = InstrumentDB(db_path)
        db.raw_bulk_insert(records, truncate=True)

        logger.info(f"Successfully inserted {len(records)} instruments into database")
        return len(records)

    except Exception as e:
        logger.error(f"Master contract download failed: {e}")
        raise
