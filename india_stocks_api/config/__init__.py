"""
Configuration module for India Stocks API
Manages broker settings and environment variables
"""

from .broker_config import (
    get_broker_config,
    validate_broker_config,
    get_all_broker_configurations,
    print_broker_configuration_summary,
    SUPPORTED_BROKERS,
    BROKER_CONFIGS,
    BASE_CONFIG,
)

from .settings import (
    get_setting,
    get_broker_setting,
    validate_required_env_vars,
    get_database_url,
    get_cache_directory,
    get_log_settings,
    get_http_settings,
)

__all__ = [
    # Broker configuration
    "get_broker_config",
    "validate_broker_config",
    "get_all_broker_configurations",
    "print_broker_configuration_summary",
    "SUPPORTED_BROKERS",
    "BROKER_CONFIGS",
    "BASE_CONFIG",
    # Settings management
    "get_setting",
    "get_broker_setting",
    "validate_required_env_vars",
    "get_database_url",
    "get_cache_directory",
    "get_log_settings",
    "get_http_settings",
]

__version__ = "1.0.0"
