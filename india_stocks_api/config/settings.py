"""
Application settings and environment variable management
"""

import os
from typing import Optional, Dict, Any
from pathlib import Path
from .broker_config import BASE_CONFIG


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get setting from environment variable or config

    Args:
        key: Setting key
        default: Default value if not found

    Returns:
        Setting value or default
    """
    # First try environment variable
    env_value = os.getenv(key)
    if env_value is not None:
        return env_value

    # Then try base config
    config_value = BASE_CONFIG.get(key)
    if config_value is not None:
        return str(config_value)

    return default


def get_broker_setting(
    broker_name: str, key: str, default: Optional[str] = None
) -> Optional[str]:
    """
    Get broker-specific setting

    Args:
        broker_name: Name of the broker
        key: Setting key
        default: Default value if not found

    Returns:
        Broker setting value or default
    """
    from .broker_config import BROKER_CONFIGS

    broker_config = BROKER_CONFIGS.get(broker_name, {})
    return broker_config.get(key, default)


def validate_required_env_vars(required_vars: list) -> bool:
    """
    Validate that required environment variables are set

    Args:
        required_vars: List of required environment variable keys

    Returns:
        True if all required vars are set, False otherwise
    """
    missing_vars = []

    for var in required_vars:
        if not get_setting(var):
            missing_vars.append(var)

    if missing_vars:
        from india_stocks_api.utils.logging import get_logger

        get_logger(__name__).error(
            f"Missing required environment variables: {missing_vars}"
        )
        return False

    return True


def get_database_url() -> str:
    """Get database URL from config"""
    return get_setting("DATABASE_URL", BASE_CONFIG["DATABASE_URL"])


def get_cache_directory() -> Path:
    """Get cache directory path from config"""
    cache_dir = Path(get_setting("CACHE_DIR", BASE_CONFIG["CACHE_DIR"]))
    cache_dir.mkdir(exist_ok=True)
    return cache_dir


def get_log_settings() -> Dict[str, Any]:
    """Get logging settings from config"""
    return {
        "level": get_setting("LOG_LEVEL", BASE_CONFIG["LOG_LEVEL"]),
        "to_file": get_setting("LOG_TO_FILE", BASE_CONFIG["LOG_TO_FILE"]).lower()
        == "true",
        "log_dir": get_setting("LOG_DIR", BASE_CONFIG["LOG_DIR"]),
    }


def get_http_settings() -> Dict[str, Any]:
    """Get HTTP client settings from config"""
    return {
        "timeout": float(get_setting("HTTP_TIMEOUT", BASE_CONFIG["HTTP_TIMEOUT"])),
        "max_connections": int(
            get_setting("HTTP_MAX_CONNECTIONS", BASE_CONFIG["HTTP_MAX_CONNECTIONS"])
        ),
        "max_keepalive": int(
            get_setting("HTTP_MAX_KEEPALIVE", BASE_CONFIG["HTTP_MAX_KEEPALIVE"])
        ),
    }
