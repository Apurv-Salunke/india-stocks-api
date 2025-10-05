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
