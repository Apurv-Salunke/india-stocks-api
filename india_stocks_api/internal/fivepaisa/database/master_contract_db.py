#database/master_contract_db.py

import os
import pandas as pd
import gzip
import shutil
from datetime import datetime

# Import httpx and shared client
import httpx

from india_stocks_api.internal.context import get_httpx_client, get_logger

logger = get_logger(__name__)


def download_csv_5paisa_data(url, output_path):
    """
    Downloads a CSV file from the specified URL and saves it to the specified path using shared httpx client.
    Implements retry logic with increased timeout for reliability.
    
    Args:
        url (str): URL to download the CSV from
        output_path (str): Path where the downloaded file should be saved
    """
    max_retries = 3
    current_retry = 0
    chunk_size = 16384  # Increased chunk size for better performance
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    while current_retry < max_retries:
        try:
            logger.info(f"Downloading CSV data (attempt {current_retry + 1}/{max_retries})")
            
            # Use a custom timeout for this specific request
            client = get_httpx_client()
            
            # Custom timeout for master contract download (2 minutes)
            timeout = httpx.Timeout(120.0)
            
            with client.stream('GET', url, timeout=timeout) as response:
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                bytes_downloaded = 0
                last_progress_report = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_bytes(chunk_size=chunk_size):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            
                            # Report progress every 10%
                            if total_size > 0:
                                progress = int((bytes_downloaded / total_size) * 100)
                                if progress >= last_progress_report + 10:
                                    logger.info(f"Download progress: {progress}% ({bytes_downloaded} / {total_size} bytes)")
                                    last_progress_report = progress
                    
            logger.info("Download complete")
            return  # Successfully downloaded, exit the function
            
        except httpx.TimeoutException as e:
            current_retry += 1
            logger.info(f"Timeout downloading master contract (attempt {current_retry}/{max_retries}): {e}")
            if current_retry >= max_retries:
                logger.info("Maximum retries reached for master contract download.")
                raise Exception(f"Failed to download master contract after {max_retries} attempts: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to download data: {e}")
            if 'time' in str(e).lower() and current_retry < max_retries:
                # If it's a timeout-related error, retry
                current_retry += 1
                logger.info(f"Retrying download (attempt {current_retry}/{max_retries})")
            else:
                # For other errors, raise immediately
                raise


def process_5paisa_csv(path):
    """
    Processes the 5Paisa CSV file to fit the existing database schema.
    Args:
    path (str): The file path of the downloaded JSON data.

    Returns:
    DataFrame: The processed DataFrame ready to be inserted into the database.
    """
    # Read JSON data into a DataFrame
    df = pd.read_csv(path)
    exchange_mapping = {
    ('N', 'C'): 'NSE',
    ('B', 'C'): 'BSE',
    ('N', 'D'): 'NFO',
    ('B', 'D'): 'BFO',
    ('N', 'U'): 'CDS',
    ('B', 'U'): 'BCD',
    ('M', 'D'): 'MCX'
    # Add other mappings as needed
    }



    # Function to map Exch and ExchType to exchange names with additional conditions
    def map_exchange(row):
        if row['Exch'] == 'N' and row['ExchType'] == 'C':
            return 'NSE_INDEX' if row['ScripCode'] > 999900 else 'NSE'
        elif row['Exch'] == 'B' and row['ExchType'] == 'C':
            return 'BSE_INDEX' if row['ScripCode'] > 999900 else 'BSE'
        else:
            return exchange_mapping.get((row['Exch'], row['ExchType']), 'Unknown')

    # Apply the function to create the exchange column
    df['exchange'] = df.apply(map_exchange, axis=1)

    # Filter the DataFrame for Series 'EQ', 'BE', 'XX'
    filtered_df = df[df['Series'].isin(['EQ', 'BE', 'XX', '  '])].copy()

    filtered_df.loc[filtered_df['Series'].isin(['XX', '  ']), 'Series'] = df['ScripType']

    #filtered_df.loc[filtered_df['Series'] == 'XX', 'Series'] = 'FUT'

    # Convert 'Expiry' to datetime format
    filtered_df['Expiry'] = pd.to_datetime(filtered_df['Expiry']).dt.date
    filtered_df['Expiry'] = filtered_df['Expiry'].where(filtered_df['Expiry'].notna(), None)

    # Function to format StrikeRate
    def format_strike(strike):
        # Convert strike to string first
        strike_str = str(strike)
        # Check if the string ends with '.0' and remove it
        if strike_str.endswith('.0'):
            # Remove the last two characters '.0'
            return strike_str[:-2]
        elif strike_str.endswith('.00'):
            # Remove the last three characters '.00'
            return strike_str[:-3]
        # Return the original string if it does not end with '.0'
        return strike_str

    # Apply the function to the StrikeRate column
    filtered_df['StrikeRate'] = filtered_df['StrikeRate'].apply(format_strike)



    # only for trading symbol construction
    filtered_df['Expiry1'] = filtered_df['Expiry'].apply(lambda d: d.strftime('%d%b%y').upper() if d else None)

    # Apply the conditions
    def create_trading_symbol(row):
        if row['Series'] in ['BE', 'EQ']:
            return row['SymbolRoot']
        elif row['Series'] == 'XX':
            return row['SymbolRoot'] + row['Expiry1'] + 'FUT'
        elif row['Series'] == 'CE':
            return row['SymbolRoot'] + row['Expiry1'] + str(row['StrikeRate']) + 'CE'
        elif row['Series'] == 'PE':
            return row['SymbolRoot'] + row['Expiry1'] + str(row['StrikeRate']) + 'PE'
        return row['SymbolRoot'] 

    filtered_df['TradingSymbol'] = filtered_df.apply(create_trading_symbol, axis=1)

    # Create a new DataFrame in OpenAlgo format
    new_df = pd.DataFrame()
    new_df['symbol'] = filtered_df['TradingSymbol'] 
    new_df['tradingsymbol'] = filtered_df['ScripData']
    new_df['br_symbol'] = filtered_df['Name'].str.upper().str.rstrip()
    new_df['name'] = filtered_df['FullName'] 
    new_df['exchange'] = filtered_df['exchange'] 
    new_df['token'] = filtered_df['ScripCode'].astype(str) 
    new_df['expiry'] = filtered_df['Expiry'] 
    new_df['strike'] = filtered_df['StrikeRate'] 
    new_df['lot_size'] = filtered_df['LotSize'] 
    new_df['instrument_type'] = filtered_df['Series'] 
    new_df['tick_size'] = filtered_df['TickSize'] 
    new_df['opt_type'] = filtered_df['ScripType'].replace({'XX': None, 'EQ': None})
    # Common Index Symbol Formats

    new_df['symbol'] = new_df['symbol'].replace({
    'Nifty Next 50': 'NIFTYNXT50',
    'MIDCPNifty': 'MIDCPNIFTY',
    'India VIX' : 'INDIAVIX',
    'BSE BANKEX' : 'BANKEX',
    'BSE SENSEX 50': 'SENSEX50'
    })
    # Return the processed DataFrame

    return new_df

def delete_5paisa_temp_data(output_path):
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


def master_contract_download(db_path='instruments.db'):
    logger.info("Starting Master Contract Download Process")
    url = 'https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/all'
    output_path = 'tmp/5paisa.csv'
    
    # Ensure tmp directory exists
    os.makedirs('tmp', exist_ok=True)
    
    try:
        logger.info(f"Initiating download from {url}")
        download_csv_5paisa_data(url, output_path)
        
        logger.info("CSV downloaded, processing data...")
        token_df = process_5paisa_csv(output_path)
        logger.info(f"Processed {len(token_df)} symbols")
        
        # Clean up temporary files
        delete_5paisa_temp_data(output_path)
        
        
        # Convert DataFrame to list of dicts for our DB
        records = token_df.to_dict('records')
        
        # Import here to avoid circular dependency
        from ....instruments.database import InstrumentDB
        
        db = InstrumentDB(db_path)
        db.raw_bulk_insert(records, truncate=True)
        
        logger.info(f"Successfully inserted {len(records)} instruments into database")
        return len(records)
    
    except Exception as e:
        logger.error(f"Master contract download failed: {e}")
        raise
