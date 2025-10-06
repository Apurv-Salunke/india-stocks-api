# database/master_contract_db.py

import os
import pandas as pd
import json
import csv
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


def download_csv_compositedge_data(output_path):
    logger.info("Downloading Master Contract CSV Files")
    exchange_segments = ["NSECM", "NSECD", "NSEFO", "BSECM", "BSEFO", "MCXFO"]
    headers_equity = "ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low, FreezeQty,TickSize,LotSize,Multiplier,DisplayName,ISIN,PriceNumerator,PriceDenominator,DetailedDescription,ExtendedSurvIndicator,CautionIndicator,GSMIndicator\n"
    headers_fo = "ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,UnderlyingInstrumentId,UnderlyingIndexName,ContractExpiration,StrikePrice,OptionType,DisplayName, PriceNumerator,PriceDenominator,DetailedDescription\n"

    # Get the shared httpx client with connection pooling
    client = get_http_client()
    headers = {"Content-Type": "application/json"}

    downloaded_files = []
    for segment in exchange_segments:
        payload = json.dumps({"exchangeSegmentList": [segment]})
        # Get 5PaisaXTS config
        fivepaisaxts_config = get_broker_config("fivepaisaxts")
        market_data_url = fivepaisaxts_config["market_data_url"]

        response = client.post(
            f"{market_data_url}/instruments/master", headers=headers, content=payload
        )
        if response.status_code != 200:
            raise Exception(
                f"Failed to download {segment}. Status: {response.status_code}"
            )

        data = response.json()
        if "result" not in data:
            raise Exception(
                f"Invalid response format for {segment}: Missing 'result' field"
            )

        if segment in ["NSECM", "BSECM"]:
            header = headers_equity
        else:
            header = headers_fo

        segment_output_path = f"{output_path}/{segment}.csv"
        os.makedirs(output_path, exist_ok=True)

        csv_data = data["result"].split("\n")  # Convert result string to list of rows
        csv_data = [
            row.split("|") for row in csv_data if row.strip()
        ]  # Convert each row into a list

        with open(segment_output_path, "w", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header.strip().split(","))  # Write headers
            writer.writerows(csv_data)
        downloaded_files.append(segment_output_path)


def fetch_index_list():
    logger.info("Fetching Index List")
    exchange_segments = [1, 11]  # NSE and BSE indexes
    headers = {"Content-Type": "application/json"}

    # Get the shared httpx client with connection pooling
    client = get_http_client()
    index_data = []

    for segment in exchange_segments:
        # Get 5PaisaXTS config
        fivepaisaxts_config = get_broker_config("fivepaisaxts")
        market_data_url = fivepaisaxts_config["market_data_url"]

        url = f"{market_data_url}/instruments/indexlist?exchangeSegment={segment}"
        response = client.get(url, headers=headers)

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch index list for segment {segment}. Status: {response.status_code}"
            )
            continue

        data = response.json()

        if "result" not in data or "indexList" not in data["result"]:
            logger.info(f"Invalid response format for segment {segment}")
            continue

        for index_entry in data["result"]["indexList"]:
            # Extract symbol name and token
            symbol_name, token = index_entry.rsplit("_", 1)

            index_data.append(
                {
                    "brsymbol": index_entry,  # Full format (e.g., "NIFTY 100_26004")
                    "symbol": symbol_name,  # Raw symbol before mapping
                    "exchange": "NSE_INDEX" if segment == 1 else "BSE_INDEX",
                    "token": token,
                }
            )

    return index_data


def reformat_symbol_detail(s):
    parts = s.split()  # Split the string into parts
    # Reorder and format the parts to match the desired output
    # Assuming the format is consistent and always "Name DD Mon YY FUT"
    return f"{parts[0]}{parts[3]}{parts[2].upper()}{parts[1]}{parts[4]}"


def process_compositedge_nse_csv(path):
    """
    Processes the compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing compositedge NSE CSV Data")
    file_path = f"{path}/NSECM.csv"

    df = pd.read_csv(file_path)

    df = df[df["Series"].isin(["EQ"])]

    token_df = pd.DataFrame()
    token_df["symbol"] = df["Name"]
    token_df["brsymbol"] = df["DisplayName"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"NSECM": "NSE"})
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]
    token_df["expiry"] = ""
    token_df["strike"] = 1.0
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = df["Series"]
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_compositedge_bse_csv(path):
    """
    Processes the compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing compositedge BSE CSV Data")
    file_path = f"{path}/BSECM.csv"

    df = pd.read_csv(file_path)

    # df = df[df['Series'].isin(['EQ'])]

    token_df = pd.DataFrame()
    token_df["symbol"] = df["Name"]
    token_df["brsymbol"] = df["DisplayName"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"BSECM": "BSE"})
    token_df["exchange"] = df.apply(
        lambda row: "BSE_INDEX" if row["Series"] == "SPOT" else "BSE", axis=1
    )
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]
    token_df["expiry"] = ""
    token_df["strike"] = 1.0
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = df["Series"]
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_compositedge_nfo_csv(path):
    """
    Processes the Compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Compositedge NFO CSV Data")
    file_path = f"{path}/NSEFO.csv"

    df = pd.read_csv(
        file_path, dtype={"StrikePrice": str, " PriceNumerator": str}, low_memory=False
    )

    # Convert 'Expiry Date' column to datetime format
    df["ContractExpiration"] = pd.to_datetime(df["ContractExpiration"])

    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce").fillna(1.0)

    df["symbol"] = df.apply(
        lambda row: f"{row['Name']}"
        f"{row['ContractExpiration'].strftime('%d%b%y').upper()}"
        f"{'' if row['OptionType'] == 1 else (str(int(float(row['StrikePrice']))) if float(row['StrikePrice']) == int(float(row['StrikePrice'])) else str(row['StrikePrice'])) if pd.notna(row['StrikePrice']) else ''}"
        f"{'FUT' if row['OptionType'] == 1 else 'CE' if row['OptionType'] == 3 else 'PE'}",
        axis=1,
    )

    # Create token_df with the relevant columns
    token_df = pd.DataFrame()
    token_df["symbol"] = df["symbol"]
    token_df["brsymbol"] = df["Description"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"NSEFO": "NFO"})
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]

    # Convert 'Expiry Date' to desired format
    token_df["expiry"] = df["ContractExpiration"].dt.strftime("%d-%b-%y").str.upper()
    token_df["strike"] = df["StrikePrice"]
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = df["OptionType"].map({1: "FUT", 3: "CE", 4: "PE"})
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_compositedge_cds_csv(path):
    """
    Processes the compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing compositedge CDS CSV Data")
    file_path = f"{path}/NSECD.csv"

    df = pd.read_csv(file_path)

    df = df.dropna(subset=["OptionType"])

    # Convert 'Expiry Date' column to datetime format
    df["ContractExpiration"] = pd.to_datetime(df["ContractExpiration"])

    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce").fillna(1.0)

    # Generate symbols
    symbols = []
    for _, row in df.iterrows():
        symbol = f"{row['Name']}"
        symbol += f"{row['ContractExpiration'].strftime('%d%b%y').upper()}"
        if row["OptionType"] != 1:
            if pd.notna(row["StrikePrice"]):
                if float(row["StrikePrice"]) == int(float(row["StrikePrice"])):
                    symbol += str(int(float(row["StrikePrice"])))
                else:
                    symbol += str(row["StrikePrice"])
        if row["OptionType"] == 1:
            symbol += "FUT"
        elif row["OptionType"] == 3:
            symbol += "CE"
        else:
            symbol += "PE"
        symbols.append(symbol)

    df["symbol"] = symbols

    # Generate symbols based on instrument type
    # df['symbol'] = df.apply(lambda x:
    #    f"{x['Name']}{x['ContractExpiration'].strftime('%d%b%y').upper()}{'FUT' if x['OptionType']=='1' else str(int(float(x['StrikePrice'])))+('CE' if x['OptionType']=='3' else 'PE')}",
    #    axis=1
    # )
    # Remove any rows where symbol generation failed
    # df = df[df['symbol'].notna()]

    # Create token_df with the relevant columns
    token_df = pd.DataFrame()
    token_df["symbol"] = df["symbol"]
    token_df["brsymbol"] = df["Description"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"NSECD": "CDS"})
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]

    # Convert 'Expiry Date' to desired format
    token_df["expiry"] = df["ContractExpiration"].dt.strftime("%d-%b-%y").str.upper()
    token_df["strike"] = df["StrikePrice"]
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = token_df["symbol"].apply(
        lambda x: "FUT" if "FUT" in x else ("PE" if "PE" in x else "CE")
    )
    # token_df['instrumenttype'] = df['OptionType'].map({
    #        1: 'FUT',
    #        872604 : 'FUT',
    #        5892 : 'FUT',
    #        3: 'CE',
    #        4: 'PE'
    #    })
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_compositedge_bfo_csv(path):
    """
    Processes the Compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Compositedge BFO CSV Data")
    file_path = f"{path}/BSEFO.csv"

    df = pd.read_csv(
        file_path, dtype={"StrikePrice": str, " PriceNumerator": str}, low_memory=False
    )

    # Convert 'Expiry Date' column to datetime format
    df["ContractExpiration"] = pd.to_datetime(df["ContractExpiration"])

    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce").fillna(1.0)

    df["symbol"] = df.apply(
        lambda row: f"{row['Name']}"
        f"{row['ContractExpiration'].strftime('%d%b%y').upper()}"
        f"{'' if row['OptionType'] == 1 else (str(int(float(row['StrikePrice']))) if float(row['StrikePrice']) == int(float(row['StrikePrice'])) else str(row['StrikePrice'])) if pd.notna(row['StrikePrice']) else ''}"
        f"{'FUT' if row['OptionType'] == 1 else 'CE' if row['OptionType'] == 3 else 'PE'}",
        axis=1,
    )

    token_df = pd.DataFrame()
    token_df["symbol"] = df["symbol"]
    token_df["brsymbol"] = df["Description"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"BSEFO": "BFO"})
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]

    # Convert 'Expiry Date' to desired format
    token_df["expiry"] = df["ContractExpiration"].dt.strftime("%d-%b-%y").str.upper()
    token_df["strike"] = df["StrikePrice"]
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = df["OptionType"].map({1: "FUT", 3: "CE", 4: "PE"})
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_compositedge_mcx_csv(path):
    """
    Processes the Compositedge CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Compositedge MCX CSV Data")
    file_path = f"{path}/MCXFO.csv"

    df = pd.read_csv(file_path)

    # Drop rows where the 'Exch Seg' column has the value 'COMTDY'
    df = df[df["ContractExpiration"] != "1"]

    df["ContractExpiration"] = pd.to_datetime(df["ContractExpiration"])
    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce").fillna(1.0)

    df["symbol"] = df.apply(
        lambda row: f"{row['Name']}"
        f"{row['ContractExpiration'].strftime('%d%b%y').upper()}"
        f"{'' if row['OptionType'] == 1 else (str(int(float(row['StrikePrice']))) if float(row['StrikePrice']) == int(float(row['StrikePrice'])) else str(row['StrikePrice'])) if pd.notna(row['StrikePrice']) else ''}"
        f"{'FUT' if row['OptionType'] == 1 else 'CE' if row['OptionType'] == 3 else 'PE'}",
        axis=1,
    )

    # Create token_df with the relevant columns
    token_df = pd.DataFrame()
    token_df["symbol"] = df["symbol"]
    token_df["brsymbol"] = df["Description"]
    token_df["name"] = df["Name"]
    token_df["exchange"] = df["ExchangeSegment"].map({"MCXFO": "MCX"})
    token_df["brexchange"] = df["ExchangeSegment"]
    token_df["token"] = df["ExchangeInstrumentID"]

    # Convert 'Expiry Date' to desired format
    token_df["expiry"] = df["ContractExpiration"].dt.strftime("%d-%b-%y").str.upper()
    token_df["strike"] = df["StrikePrice"]
    token_df["lotsize"] = df["LotSize"]
    token_df["instrumenttype"] = df["OptionType"].map({1: "FUT", 3: "CE", 4: "PE"})
    token_df["tick_size"] = df["TickSize"]

    return token_df


def process_index_data(index_data):
    logger.info("Processing Index Data")
    df = pd.DataFrame(index_data)

    # Map Symbols to Standard Format
    df["symbol"] = df["symbol"].replace(
        {
            "NIFTY 50": "NIFTY",
            "NIFTY BANK": "BANKNIFTY",
            "INDIA VIX": "INDIAVIX",
            "NIFTY FIN SERVICE": "FINNIFTY",
            "NIFTY MID SELECT": "MIDCPNIFTY",
            "NIFTY NEXT 50": "NIFTYNXT50",
            "SENSEX": "SENSEX",
            "BANKEX": "BANKEX",
            "SNSX50": "SENSEX50",
        }
    )

    df["name"] = df["symbol"]
    df["brexchange"] = df["exchange"]
    df["expiry"] = ""
    df["strike"] = 1.0
    df["lotsize"] = 1  # Default index lot size
    df["instrumenttype"] = "INDEX"
    df["tick_size"] = 0.05
    # logger.info(f"{df}")

    return df


def delete_compositedge_temp_data(output_path):
    # Check each file in the directory
    for filename in os.listdir(output_path):
        # Construct the full file path
        file_path = os.path.join(output_path, filename)
        # If the file is a CSV, delete it
        if filename.endswith(".csv") and os.path.isfile(file_path):
            os.remove(file_path)
            logger.info(f"Deleted {file_path}")


def master_contract_download():
    logger.info("Downloading Master Contract")

    # Use config for cache directory
    cache_dir = get_cache_directory()
    output_path = cache_dir / "fivepaisaxts"
    output_path.mkdir(exist_ok=True)

    try:
        download_csv_compositedge_data(output_path)

        # Initialize our database
        initialize_broker_database()

        # Process exchange data and store in our database
        all_instruments = []

        # Process each exchange and collect all instruments
        token_df = process_compositedge_nse_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        token_df = process_compositedge_bse_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        token_df = process_compositedge_nfo_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        token_df = process_compositedge_cds_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        token_df = process_compositedge_mcx_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        token_df = process_compositedge_bfo_csv(output_path)
        all_instruments.extend(token_df.to_dict("records"))

        # Fetch and Process Index Data
        index_data = fetch_index_list()
        if index_data:
            index_df = process_index_data(index_data)
            all_instruments.extend(index_df.to_dict("records"))

        # Store all instruments in our database
        store_broker_instruments(all_instruments, "fivepaisaxts")

        delete_compositedge_temp_data(output_path)

        logger.info("Successfully Downloaded 5PaisaXTS symbols")
        return {"status": "success", "message": "Successfully Downloaded"}

    except Exception as e:
        logger.info(f"{str(e)}")
        logger.error(f"Failed to download 5PaisaXTS symbols: {str(e)}")
        return {"status": "error", "message": str(e)}
