"""
Broker Configuration System
Manages all broker-specific settings and environment variables
"""

import os
from typing import Dict, Any
from pathlib import Path

# Base configuration
BASE_CONFIG = {
    # Database settings
    "DATABASE_URL": f"sqlite:///{Path(__file__).parent.parent.parent / '_cache' / 'broker_instruments.db'}",
    # Logging settings
    "LOG_LEVEL": "INFO",
    "LOG_TO_FILE": "False",
    "LOG_DIR": "logs",
    # HTTP client settings
    "HTTP_TIMEOUT": 30.0,
    "HTTP_MAX_CONNECTIONS": 50,
    "HTTP_MAX_KEEPALIVE": 20,
    # Cache settings
    "CACHE_DIR": "_cache",
    "CACHE_EXPIRY_HOURS": 24,
}

# Broker-specific configurations
BROKER_CONFIGS = {
    "angelone": {
        "name": "AngelOne",
        "api_base_url": "https://apiconnect.angelbroking.com",
        "auth_url": "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword",
        "market_data_url": "https://apiconnect.angelbroking.com/rest/secure/angelbroking/market/v1",
        "order_url": "https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1",
        "master_contract_url": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
        },
    },
    "zerodha": {
        "name": "Zerodha",
        "api_base_url": "https://api.kite.trade",
        "auth_url": "https://kite.zerodha.com/connect/login",
        "market_data_url": "https://api.kite.trade/quote",
        "order_url": "https://api.kite.trade/orders",
        "master_contract_url": "https://api.kite.trade/instruments",
        "required_env_vars": ["BROKER_API_KEY", "BROKER_API_SECRET"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "upstox": {
        "name": "Upstox",
        "api_base_url": "https://api.upstox.com/v2",
        "auth_url": "https://api.upstox.com/v2/login/authorization/token",
        "market_data_url": "https://api.upstox.com/v2/market-quote/quotes",
        "order_url": "https://api.upstox.com/v2/order/place",
        "master_contract_url": "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
        "required_env_vars": ["BROKER_API_KEY", "BROKER_API_SECRET"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "dhan": {
        "name": "Dhan",
        "api_base_url": "https://api.dhan.co",
        "auth_url": "https://api.dhan.co/auth/login",
        "market_data_url": "https://api.dhan.co/marketdata",
        "order_url": "https://api.dhan.co/orders",
        "master_contract_url": "https://images.dhan.co/api-data/api-scrip-master.csv",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "dhan_sandbox": {
        "name": "Dhan Sandbox",
        "api_base_url": "https://api.dhan.co",
        "auth_url": "https://api.dhan.co/auth/login",
        "market_data_url": "https://api.dhan.co/marketdata",
        "order_url": "https://api.dhan.co/orders",
        "master_contract_url": "https://images.dhan.co/api-data/api-scrip-master.csv",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "shoonya": {
        "name": "Shoonya",
        "api_base_url": "https://api.shoonya.com",
        "auth_url": "https://api.shoonya.com/NorenWClientTP/QuickAuth",
        "market_data_url": "https://api.shoonya.com/NorenWClientTP/GetMarketData",
        "order_url": "https://api.shoonya.com/NorenWClientTP/PlaceOrder",
        "master_contract_urls": {
            "NSE": "https://api.shoonya.com/NSE_symbols.txt.zip",
            "NFO": "https://api.shoonya.com/NFO_symbols.txt.zip",
            "CDS": "https://api.shoonya.com/CDS_symbols.txt.zip",
            "MCX": "https://api.shoonya.com/MCX_symbols.txt.zip",
            "BSE": "https://api.shoonya.com/BSE_symbols.txt.zip",
            "BFO": "https://api.shoonya.com/BFO_symbols.txt.zip",
        },
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "groww": {
        "name": "Groww",
        "api_base_url": "https://growwapi-assets.groww.in",
        "auth_url": "https://growwapi-assets.groww.in/auth/login",
        "market_data_url": "https://growwapi-assets.groww.in/marketdata",
        "order_url": "https://growwapi-assets.groww.in/orders",
        "master_contract_url": "https://growwapi-assets.groww.in/instruments/instrument.csv",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "fivepaisa": {
        "name": "5Paisa",
        "api_base_url": "https://openapi.5paisa.com",
        "auth_url": "https://openapi.5paisa.com/VendorsAPI/Service1.svc/V1/LoginRequestMobileNewbyEmail",
        "market_data_url": "https://openapi.5paisa.com/VendorsAPI/Service1.svc/V1/MarketData",
        "order_url": "https://openapi.5paisa.com/VendorsAPI/Service1.svc/V1/OrderRequest",
        "master_contract_url": "https://openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/all",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
    "fivepaisaxts": {
        "name": "5PaisaXTS",
        "api_base_url": "https://xtsmum.5paisa.com",
        "auth_url": "https://xtsmum.5paisa.com/apimarketdata/auth/login",
        "market_data_url": "https://xtsmum.5paisa.com/apimarketdata",
        "order_url": "https://xtsmum.5paisa.com/interactive",
        "master_contract_url": "https://xtsmum.5paisa.com/apimarketdata/instruments/master",
        "required_env_vars": ["BROKER_API_KEY"],
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    },
}

# Supported brokers
SUPPORTED_BROKERS = list(BROKER_CONFIGS.keys())


def get_broker_config(broker_name: str) -> Dict[str, Any]:
    """
    Get broker-specific configuration

    Args:
        broker_name: Name of the broker (e.g., 'angelone', 'zerodha')

    Returns:
        Broker configuration dictionary

    Raises:
        ValueError: If broker is not supported
    """
    if broker_name not in SUPPORTED_BROKERS:
        raise ValueError(
            f"Broker '{broker_name}' not supported. Supported brokers: {SUPPORTED_BROKERS}"
        )

    config = BROKER_CONFIGS[broker_name].copy()

    # Add environment-specific values
    config["api_key"] = os.getenv("BROKER_API_KEY")
    config["api_secret"] = os.getenv("BROKER_API_SECRET")

    return config


def validate_broker_config(broker_name: str) -> bool:
    """
    Validate broker configuration

    Args:
        broker_name: Name of the broker

    Returns:
        True if configuration is valid, False otherwise
    """
    try:
        config = get_broker_config(broker_name)
        required_vars = config.get("required_env_vars", [])

        for var in required_vars:
            if not os.getenv(var):
                print(f"❌ Missing required environment variable: {var}")
                return False

        return True
    except ValueError:
        return False


def get_all_broker_configurations() -> Dict[str, Any]:
    """Get all broker configurations"""
    return {
        "broker_configs": BROKER_CONFIGS,
        "supported_brokers": SUPPORTED_BROKERS,
    }


def print_broker_configuration_summary():
    """Print broker configuration summary"""
    print("🔧 Broker Configuration Summary")
    print("=" * 50)

    print(f"📊 Supported Brokers: {len(SUPPORTED_BROKERS)}")
    for broker in SUPPORTED_BROKERS:
        config = BROKER_CONFIGS[broker]
        print(f"  - {config['name']} ({broker})")

    print("\n✅ Broker configuration loaded successfully!")


if __name__ == "__main__":
    print_broker_configuration_summary()
