"""
Centralized Funds and Margin Management System for India Stocks API.

This module provides a unified interface to retrieve margin and funds data
across multiple supported Indian stock brokers. Broker-specific implementations
are dynamically resolved based on a naming convention to avoid static mapping
divergence between authentication and funds layers.

Supported operations:
- Fetch margin data for an individual broker
- Fetch margin data for all authenticated brokers
- Retrieve available cash margin for a broker
"""

import importlib
from typing import Dict, Any
from .utils.logging import get_logger
from .auth import get_broker_token, get_supported_brokers, is_authenticated

logger = get_logger(__name__)


def _resolve_funds_module(broker_name: str):
    """
    Dynamically resolve and import the funds module for a given broker.

    The module is resolved using the convention:
        india_stocks_api.brokers.<broker_name>.api.funds

    Args:
        broker_name (str): Broker identifier (e.g., "zerodha", "angelone").

    Returns:
        module: Imported broker-specific funds module.

    Raises:
        ModuleNotFoundError: If the funds module does not exist for the broker.
        AttributeError: If the module exists but does not implement
                        `get_margin_data`.
    """
    module_path = f"india_stocks_api.brokers.{broker_name}.api.funds"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            f"No funds module found for broker '{broker_name}' "
            f"(expected at '{module_path}')"
        ) from e

    if not hasattr(module, "get_margin_data"):
        raise AttributeError(
            f"Funds module for broker '{broker_name}' does not implement "
            f"'get_margin_data'"
        )

    return module


def get_margin(broker_name: str) -> Dict[str, Any]:
    """
    Retrieve margin and funds data for a specific broker.

    This function validates broker support, authentication state, and token
    availability before dynamically resolving the broker's funds module and
    fetching margin data.

    Args:
        broker_name (str): Broker identifier.

    Returns:
        Dict[str, Any]: A standardized margin dictionary with keys such as:
            - availablecash
            - collateral
            - m2munrealized
            - m2mrealized
            - utiliseddebits

        Returns an empty dictionary if margin data cannot be retrieved due to
        unsupported broker, authentication failure, invalid token, or runtime
        errors.

    Notes:
        Errors are logged with context; callers should treat an empty return
        value as a failure to fetch margin data.
    """

    if broker_name not in get_supported_brokers():
        logger.error(f"Broker {broker_name} not supported for funds data.")
        return {}

    if not is_authenticated(broker_name):
        logger.warning(f"User not authenticated with {broker_name}")
        return {}

    token_data = get_broker_token(broker_name)
    if not token_data or not isinstance(token_data, dict):
        logger.error(f"Token data missing or invalid for broker {broker_name}")
        return {}

    auth_token = token_data.get("auth_token")
    if not auth_token or not isinstance(auth_token, str) or not auth_token.strip():
        logger.error(
            f"Auth token missing or empty for broker {broker_name}. ")
        return {}

    try:
        module = _resolve_funds_module(broker_name)
        margin_data = module.get_margin_data(auth_token)

        if margin_data:
            logger.info(f"Successfully retrieved margin for {broker_name}")
        else:
            logger.warning(f"Empty margin data returned for {broker_name}")
        return margin_data

    except ImportError as e:
        logger.error(f"Funds module not found for {broker_name}: {e}")
        return {}
    except AttributeError as e:
        logger.error(f"get_margin_data not implemented for {broker_name}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error fetching margin for {broker_name}: {e}")
        return {}


def get_all_margins() -> Dict[str, Dict[str, Any]]:
    """
    Retrieve margin data for all authenticated brokers.

    Iterates over all brokers supported by the authentication layer and attempts
    to fetch margin data for those currently authenticated.

    Returns:
        Dict[str, Dict[str, Any]]: A mapping of broker name to its margin data.
        Brokers for which margin retrieval fails are skipped.

    Notes:
        If no authenticated brokers are found or no margin data can be fetched,
        an empty dictionary is returned.
    """
    all_margins = {}

    for broker in get_supported_brokers():
        if not is_authenticated(broker):
            continue

        try:
            margin = get_margin(broker)
            if not margin:
                logger.warning(f"No margin data returned for broker '{broker}'")
                continue
            all_margins[broker] = margin
        except Exception as e:
            logger.error(f"Skipping broker '{broker}': {e}")

    if not all_margins:
        logger.warning("No margin data retrieved from any broker.")

    return all_margins


def get_available_cash(broker_name: str) -> str:
    """
    Retrieve the available cash margin for a specific broker.

    Args:
        broker_name (str): Broker identifier.

    Returns:
        str: Available cash margin.

    Raises:
        ValueError: If no margin data is available for the broker.
        KeyError: If the margin data does not contain the `availablecash` field.
    """
    margin = get_margin(broker_name)

    if not margin:
        raise ValueError(f"No margin data available for broker '{broker_name}'")

    try:
        return margin["availablecash"]
    except KeyError:
        raise KeyError(
            f"'availablecash' not present in margin data for broker '{broker_name}'"
        )
