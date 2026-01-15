"""
Centralized Master Data Management for India Stocks API.
Routes master contract download requests to broker-specific implementations.
"""

import importlib
from typing import Dict, Any
from .utils.logging import get_logger

logger = get_logger(__name__)

# Mapping follows the structure of your directory and auth.py
BROKER_MASTER_MAPPING = {
    "angelone": "india_stocks_api.brokers.angel.database.master_contract_db",
    "zerodha": "india_stocks_api.brokers.zerodha.database.master_contract_db",
    "shoonya": "india_stocks_api.brokers.shoonya.database.master_contract_db",
    "upstox": "india_stocks_api.brokers.upstox.database.master_contract_db",
    "fyers": "india_stocks_api.brokers.fyers.database.master_contract_db",
    "groww": "india_stocks_api.brokers.groww.database.master_contract_db",
    "fivepaisa": "india_stocks_api.brokers.fivepaisa.database.master_contract_db",
    "fivepaisaxts": "india_stocks_api.brokers.fivepaisaxts.database.master_contract_db",
    "dhan": "india_stocks_api.brokers.dhan.database.master_contract_db",
}

def download_master_data(broker_name: str) -> Dict[str, Any]:
    """
    Downloads, processes, and stores master contract data for a specific broker.
    
    Returns:
        {"status": "success", "message": str} or {"status": "error", "message": str}
    """
    if broker_name not in BROKER_MASTER_MAPPING:
        error_msg = f"Broker {broker_name} not supported for master data download."
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

    try:
        # Dynamically import the broker's master_contract_db module
        module_path = BROKER_MASTER_MAPPING[broker_name]
        module = importlib.import_module(module_path)
        
        # Call the standardized download function present in your files
        logger.info(f"Starting master contract download for: {broker_name}")
        result = module.master_contract_download()
        
        return result

    except Exception as e:
        logger.error(f"Failed to execute master download for {broker_name}: {e}")
        return {"status": "error", "message": str(e)}

def download_all_masters() -> Dict[str, Dict[str, Any]]:
    """Download masters for all supported brokers sequentially."""
    results = {}
    for broker in BROKER_MASTER_MAPPING.keys():
        results[broker] = download_master_data(broker)
    return results
