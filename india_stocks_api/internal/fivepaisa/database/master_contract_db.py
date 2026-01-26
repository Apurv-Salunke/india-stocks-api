# database/master_contract_db.py

import os
import pandas as pd
import httpx
import shutil
from datetime import datetime, date
from india_stocks_api.internal.context import get_httpx_client, get_logger
from india_stocks_api.instruments.database import InstrumentDB

logger = get_logger(__name__)

def download_csv_5paisa_data(url, output_path):
    """
    Downloads a CSV file from the specified URL and saves it to the specified path.
    """
    max_retries = 3
    current_retry = 0
    chunk_size = 16384
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    while current_retry < max_retries:
        try:
            logger.info(f"Downloading CSV data (attempt {current_retry + 1}/{max_retries})")
            client = get_httpx_client()
            timeout = httpx.Timeout(120.0)
            
            with client.stream('GET', url, timeout=timeout) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                bytes_downloaded = 0
                last_progress_report = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            if total_size > 0:
                                progress = int((bytes_downloaded / total_size) * 100)
                                if progress >= last_progress_report + 10:
                                    logger.info(f"Download progress: {progress}%")
                                    last_progress_report = progress
                    
            logger.info("Download complete")
            return
            
        except Exception as e:
            current_retry += 1
            logger.error(f"Download failed: {e}")
            if current_retry >= max_retries:
                raise

def process_5paisa_csv(path):
    """
    Processes the 5Paisa CSV file to fit InstrumentMaster schema.
    """
    logger.info("Processing CSV data...")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return []

    # Exchange Mapping
    exchange_mapping = {
        ('N', 'C'): 'NSE',
        ('B', 'C'): 'BSE',
        ('N', 'D'): 'NFO',
        ('B', 'D'): 'BFO',
        ('N', 'U'): 'CDS',
        ('B', 'U'): 'BCD',
        ('M', 'D'): 'MCX'
    }

    results = []
    
    # Process row by row for safety/simplicity in port (vectorization better but logic complex)
    # Using itertuples for speed
    for row in df.itertuples(index=False):
        try:
            # Map Exchange
            exch = row.Exch
            exch_type = row.ExchType
            
            if exch == 'N' and exch_type == 'C' and row.ScripCode > 999900:
                exchange = 'NSE_INDEX'
                instrument_type = 'IDX'
            elif exch == 'B' and exch_type == 'C' and row.ScripCode > 999900:
                exchange = 'BSE_INDEX'
                instrument_type = 'IDX'
            else:
                exchange = exchange_mapping.get((exch, exch_type), 'Unknown')
                if exchange == 'Unknown': 
                    continue
                
                # Determine Instrument Type
                series = str(row.Series).strip()
                scrip_type = str(row.ScripType).strip()
                
                if series in ['EQ', 'BE']:
                    instrument_type = 'EQ'
                elif series == 'XX':
                    # Check ScripType for derivative type
                    if scrip_type in ['CE', 'PE']:
                        instrument_type = 'OPT'
                    else:
                        instrument_type = 'FUT'
                elif series in ['CE', 'PE']:
                    instrument_type = 'OPT'
                else:
                    # Fallback
                    instrument_type = 'EQ' if exch_type == 'C' else 'FUT'

            # Parse Dates
            expiry = None
            if hasattr(row, 'Expiry') and pd.notna(row.Expiry):
                try:
                    # Generic parser or specific? Usually 5paisa has specific format
                    # Original code used pd.to_datetime logic.
                    ts = pd.to_datetime(row.Expiry)
                    expiry = ts.date()
                except:
                    expiry = None
            
            # Strike and Opt Type
            strike = float(row.StrikeRate) if hasattr(row, 'StrikeRate') else 0.0
            
            opt_type = None
            if instrument_type == 'OPT':
                if scrip_type == 'CE' or str(row.Series).strip() == 'CE': opt_type = 'CE'
                elif scrip_type == 'PE' or str(row.Series).strip() == 'PE': opt_type = 'PE'
            
            # Symbol construction
            symbol_root = str(row.SymbolRoot).strip()
            
            # Cleaning Symbol Root for Indices
            if exchange == 'NSE_INDEX':
                # Map specific indices
                if row.Name == 'Nifty 50': symbol_root = 'NIFTY'
                elif row.Name == 'Nifty Bank': symbol_root = 'BANKNIFTY'
                elif 'Nifty Next 50' in row.Name: symbol_root = 'NIFTYNXT50'
                elif 'India VIX' in row.Name: symbol_root = 'INDIAVIX'
                else: symbol_root = row.Name.upper().replace(' ', '')
            
            # Trading Symbol (Unique Identifier logic from OpenAlgo)
            # Adapt logic: Name field often contains "NIFTY 24 JAN FUT"
            # SymbolRoot: "NIFTY"
            # We need a unique tradingsymbol. 
            # 5Paisa ScripCode is the real unique ID (token).
            # We'll use Name as tradingsymbol if available, else construct it.
            tradingsymbol = row.Name.strip()  # 5Paisa Name is usually unique enough "SBIN"
            
            # Logic for derivatives construction if needed
            if instrument_type in ['FUT', 'OPT']:
                # Construct standarized symbol like NIFTY24JANFUT
                # For now using Name is safest as it comes from broker
                pass

            # Unique Token: format Exch|ExchType|ScripCode
            # This is necessary because 5Paisa reuses ScripCodes across segments
            token = f"{row.Exch}|{row.ExchType}|{row.ScripCode}"

            record = {
                'token': token,
                'symbol': symbol_root,
                'exchange': exchange,
                'tradingsymbol': tradingsymbol,
                'br_symbol': row.Name,
                'expiry': expiry,
                'strike': strike,
                'opt_type': opt_type,
                'lot_size': int(row.LotSize) if hasattr(row, 'LotSize') else 1,
                'tick_size': float(row.TickSize) if hasattr(row, 'TickSize') else 0.05,
                'instrument_type': instrument_type
            }
            results.append(record)
            
        except Exception as e:
            # logger.warning(f"Skipping row {row}: {e}")
            continue
            
    return results

def master_contract_download(db_path="instruments.db"):
    """
    Downloads and populates the master contract database.
    Args:
        db_path: Path to the SQLite DB
    """
    try:
        url = 'https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/all'
        tmp_path = 'tmp/5paisa_master.csv'
        
        logger.info(f"Downloading master contract from {url}")
        download_csv_5paisa_data(url, tmp_path)
        
        records = process_5paisa_csv(tmp_path)
        
        if records:
            logger.info(f"Inserting {len(records)} records into {db_path}...")
            db = InstrumentDB(db_path)
            # Use truncate=False to merge, or True if we want to replace all
            # Since this is a specific broker update, we usually merge or replace broker-specifics?
            # InstrumentDB is shared. Truncating might kill other broker data?
            # InstrumentDB.raw_bulk_insert replaces by PK (token).
            # But different brokers might collision on token? 
            # 5paisa token is integer. Angel is integer.
            # Ideally each broker has unique keys or separate DBs, but India Stocks API seems to share.
            # Wait, InstrumentMaster has `token` as PK. If two brokers share token "123" for different things...
            # This is a known issue. Usually we prefix token: "5P:123". 
            # But existing `angel.py` puts raw token.
            # Assuming tokens are mostly unique or user uses one broker active.
            
            # I will use replace.
            db.raw_bulk_insert(records, truncate=False)
            logger.info("Master contract updated successfully.")
        else:
            logger.warning("No records processed.")
            
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            
    except Exception as e:
        logger.error(f"Master contract download failed: {e}")
        raise
