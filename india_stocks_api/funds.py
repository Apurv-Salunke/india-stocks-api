"""
Centralized Funds and Margin Management System for India Stocks API
Routes margin requests to specific broker implementations.
"""

import importlib
from typing import Dict, Any
from .utils.logging import get_logger
from .auth import get_broker_token, get_supported_brokers, is_authenticated

logger = get_logger(__name__)

# Mapping follows the structure in auth.py
BROKER_FUNDS_MAPPING = {
    "angelone": "india_stocks_api.brokers.angel.api.funds",
    "zerodha": "india_stocks_api.brokers.zerodha.api.funds",
    "shoonya": "india_stocks_api.brokers.shoonya.api.funds",
    "dhan": "india_stocks_api.brokers.dhan.api.funds",
    "dhan_sandbox": "india_stocks_api.brokers.dhan_sandbox.api.funds",
    "fyers": "india_stocks_api.brokers.fyers.api.funds",
    "groww": "india_stocks_api.brokers.groww.api.funds",
    "fivepaisa": "india_stocks_api.brokers.fivepaisa.api.funds",
    "upstox": "india_stocks_api.brokers.upstox.api.funds",
}

def get_margin(broker_name: str) -> Dict[str, Any]:
    """
    Get margin and funds data for a specific broker.
    
    Returns a standardized dictionary:
    {
        "availablecash": str,
        "collateral": str,
        "m2munrealized": str,
        "m2mrealized": str,
        "utiliseddebits": str
    }
    """

    # 1. Check if the broker is supported
    if broker_name not in get_supported_brokers():

        logger.error(f"Broker {broker_name} not supported for funds data.")
        return {}

    # 2. Check if authenticated and get token
    if not is_authenticated(broker_name):
        logger.warning(f"User not authenticated with {broker_name}")
        return {}

    token_data = get_broker_token(broker_name)
    auth_token = token_data.get("auth_token")
    
    try:
        # 3. Dynamically import the broker's funds module
        module_path = BROKER_FUNDS_MAPPING[broker_name]
        module = importlib.import_module(module_path)
        
        # 4. Call the standardized get_margin_data function
        margin_data = module.get_margin_data(auth_token)
        
        logger.info(f"Successfully retrieved margin for {broker_name}")
        return margin_data

    except Exception as e:
        logger.error(f"Error fetching margin for {broker_name}: {e}")
        return {}

def get_all_margins() -> Dict[str, Dict[str, Any]]:
    """Fetch margins for all currently authenticated brokers."""
    all_margins = {}
    for broker in BROKER_FUNDS_MAPPING.keys():
        if is_authenticated(broker):
            all_margins[broker] = get_margin(broker)
    return all_margins
