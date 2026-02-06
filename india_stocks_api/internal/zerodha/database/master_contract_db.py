#database/master_contract_db.py

import os
import pandas as pd
import io
from india_stocks_api.internal.context import get_httpx_client, get_logger

from india_stocks_api.internal.context import get_auth_token

logger = get_logger(__name__)


def download_csv_zerodha_data(output_path):
    """
    Downloads the CSV file from Zerodha using Auth Credentials, saves it to the specified path and convert.
    to pandas dataframe using shared httpx client with connection pooling.
    
    Args:
        output_path (str): Path where the CSV file will be saved
        
    Returns:
        pd.DataFrame: DataFrame containing the downloaded instrument data
    """
    try:
        AUTH_TOKEN = get_auth_token()
        
        # Get the shared httpx client with connection pooling
        client = get_httpx_client()
        
        headers = {
            'X-Kite-Version': '3',
            'Authorization': f'token {AUTH_TOKEN}'
        }
        
        # Make the GET request using the shared client
        response = client.get(
            'https://api.kite.trade/instruments',
            headers=headers  # Increased timeout for potentially large file
        )
        response.raise_for_status()  # Raises an exception for 4XX/5XX responses
        
        # Process the response directly as CSV
        csv_string = response.text
        df = pd.read_csv(io.StringIO(csv_string))
        
        # Save to output path if needed
        if output_path:
            df.to_csv(output_path, index=False)
            
        return df
        
    except Exception as e:
        error_message = str(e)
        try:
            if hasattr(e, 'response') and e.response is not None:
                error_detail = e.response.json()
                error_message = error_detail.get('message', str(e))
        except:
            pass
            
        logger.error(f"Error downloading Zerodha instruments: {error_message}")
        raise


def reformat_symbol(row):
    symbol = row['symbol']
    instrument_type = row['instrument_type']
    
    if instrument_type == 'FUT':
        # For FUT, remove the spaces and append 'FUT' at the end
        parts = symbol.split(' ')
        if len(parts) == 5:  # Make sure the symbol has the correct format
            symbol = parts[0] + parts[2] + parts[3] + parts[4] + parts[1]
    elif instrument_type in ['CE', 'PE']:
        # For CE/PE, rearrange the parts and remove spaces
        parts = symbol.split(' ')
        if len(parts) == 6:  # Make sure the symbol has the correct format
            symbol = parts[0] + parts[3] + parts[4] + parts[5] + parts[1] + parts[2]
    else:
        symbol = symbol  # No change for other instrument types

    return symbol



def process_zerodha_csv(path):
    """
    Processes the Zerodha CSV file to fit the existing database schema and performs exchange name mapping.
    """
    logger.info("Processing Zerodha CSV Data")
    df = pd.read_csv(path)

    # Map exchange names
    exchange_map = {
        "NSE": "NSE",
        "NFO": "NFO",
        "CDS": "CDS",
        "NSE_INDEX": "NSE_INDEX",
        "BSE_INDEX": "BSE_INDEX",
        "BSE": "BSE",
        "BFO": "BFO",
        "BCD": "BCD",
        "MCX": "MCX"

    }
    
    df['exchange'] = df['exchange'].map(exchange_map)

    # Update exchange names based on the instrument type
    df.loc[(df['segment'] == 'INDICES') & (df['exchange'] == 'NSE'), 'exchange'] = 'NSE_INDEX'
    df.loc[(df['segment'] == 'INDICES') & (df['exchange'] == 'BSE'), 'exchange'] = 'BSE_INDEX'
    df.loc[(df['segment'] == 'INDICES') & (df['exchange'] == 'MCX'), 'exchange'] = 'MCX_INDEX'
    df.loc[(df['segment'] == 'INDICES') & (df['exchange'] == 'CDS'), 'exchange'] = 'CDS_INDEX'

    # Format expiry date
    df['expiry'] = pd.to_datetime(df['expiry']).dt.strftime('%d-%b-%y').str.upper()

    # Combine instrument_token and exchange_token
    df['token'] = df['instrument_token'].astype(str) + '::::' + df['exchange_token'].astype(str)

    df["symbol"] = df["tradingsymbol"]

    # Select and rename columns
    df = df[['token', 'symbol', 'tradingsymbol', 'name', 'expiry', 
             'strike', 'lot_size', 'instrument_type', 'exchange', 
             'tick_size']].rename(columns={
        'tradingsymbol': 'tradingsymbol',  # Keep original tradingsymbol for DB
        'name': 'name',
        'expiry': 'expiry',
        'strike': 'strike',
        'lot_size': 'lot_size',
        'instrument_type': 'instrument_type',
        'exchange': 'exchange',
        'tick_size': 'tick_size'
    })

    df['br_symbol'] = df['tradingsymbol']  # Use tradingsymbol for brsymbol
    df['symbol'] = df.apply(reformat_symbol, axis=1)  # Create formatted symbol
    df['brexchange'] = df['exchange']
    
    # Add missing opt_type column based on instrument type
    df['opt_type'] = df['instrument_type'].apply(
        lambda x: 'CE' if x == 'CE' else 'PE' if x == 'PE' else 'XX'
    )
    
    # Fill NaN values in the 'expiry' column with an empty string
    df['expiry'] = df['expiry'].fillna('')
    
    # Futures Symbol Update 
    df.loc[(df['instrument_type'] == 'FUT'), 'symbol'] = df['name'] + df['expiry'].str.replace('-', '', regex=False) + 'FUT'
    
    # Options Symbol Update 

    def format_strike(strike):
        # Convert the string to a float, then to an integer, and finally back to a string.
        return str(int(float(strike)))


    df.loc[(df['instrument_type'] == 'CE'), 'symbol'] = df['name'] + df['expiry'].str.replace('-', '', regex=False) + df['strike'].apply(format_strike) + df['instrument_type']
    df.loc[(df['instrument_type'] == 'PE'), 'symbol'] = df['name'] + df['expiry'].str.replace('-', '', regex=False) + df['strike'].apply(format_strike) + df['instrument_type']

    df['symbol'] = df['symbol'].replace({
    'NIFTY 50': 'NIFTY',
    'NIFTY NEXT 50': 'NIFTYNXT50',
    'NIFTY FIN SERVICE': 'FINNIFTY',
    'NIFTY BANK': 'BANKNIFTY',
    'NIFTY MID SELECT': 'MIDCPNIFTY',
    'INDIA VIX': 'INDIAVIX',
    'SNSX50': 'SENSEX50'
    })

    return df
    

def delete_zerodha_temp_data(output_path):
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
    logger.info("Downloading Master Contract")
    

    output_path = 'tmp/zerodha.csv'
    try:
        download_csv_zerodha_data(output_path)
        token_df = process_zerodha_csv(output_path)
        delete_zerodha_temp_data(output_path)
        #token_df['token'] = pd.to_numeric(token_df['token'], errors='coerce').fillna(-1).astype(int)
        
        #token_df = token_df.drop_duplicates(subset='symbol', keep='first')

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
