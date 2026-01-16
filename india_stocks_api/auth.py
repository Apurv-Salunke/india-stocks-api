"""
Simple authentication system for India Stocks API
Uses existing auth_api.py files from OpenAlgo brokers
"""

import json
import os
import pyotp
import base64
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
import importlib
import pytz
from dotenv import load_dotenv

from .utils.logging import get_logger
from .utils.common import get_india_time, IST

logger = get_logger(__name__)
load_dotenv()

# Token storage file
TOKEN_STORAGE_PATH = Path(__file__).parent.parent / "_cache" / "auth_tokens.json"

# Broker authentication mapping
BROKER_AUTH_MAPPING = {
    "angelone": {
        "module": "india_stocks_api.brokers.angelone.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "ANGELONE_CLIENT_CODE",
            "ANGELONE_PIN",
            "ANGELONE_TOTP_SECRET",
            "BROKER_API_KEY",
        ],
    },
    "zerodha": {
        "module": "india_stocks_api.brokers.zerodha.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "ZERODHA_USER_ID",
            "ZERODHA_PASSWORD",
            "ZERODHA_TOTP_SECRET",
            "BROKER_API_KEY",
            "BROKER_API_SECRET",
        ],
    },
    "shoonya": {
        "module": "india_stocks_api.brokers.shoonya.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "SHOONYA_USER_ID",
            "SHOONYA_PASSWORD",
            "SHOONYA_TOTP_SECRET",
            "BROKER_API_KEY",
            "BROKER_API_SECRET",
        ],
    },
    "dhan": {
        "module": "india_stocks_api.brokers.dhan.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": ["DHAN_CLIENT_ID", "DHAN_PASSWORD", "BROKER_API_KEY"],
    },
    "dhan_sandbox": {
        "module": "india_stocks_api.brokers.dhan_sandbox.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "DHAN_SANDBOX_CLIENT_ID",
            "DHAN_SANDBOX_PASSWORD",
            "BROKER_API_KEY",
        ],
    },
    "fyers": {
        "module": "india_stocks_api.brokers.fyers.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "FYERS_CLIENT_ID",
            "FYERS_PASSWORD",
            "FYERS_PIN",
            "BROKER_API_KEY",
            "BROKER_API_SECRET",
        ],
    },
    "groww": {
        "module": "india_stocks_api.brokers.groww.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "GROWW_USER_ID",
            "GROWW_PASSWORD",
            "GROWW_TOTP_SECRET",
            "BROKER_API_KEY",
        ],
    },
    "fivepaisa": {
        "module": "india_stocks_api.brokers.fivepaisa.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "FIVEPAISA_CLIENT_CODE",
            "FIVEPAISA_PASSWORD",
            "FIVEPAISA_TOTP_SECRET",
            "BROKER_API_KEY",
        ],
    },
    "fivepaisaxts": {
        "module": "india_stocks_api.brokers.fivepaisaxts.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "FIVEPAISAXTS_CLIENT_CODE",
            "FIVEPAISAXTS_PASSWORD",
            "FIVEPAISAXTS_TOTP_SECRET",
            "BROKER_API_KEY",
        ],
    },
    "upstox": {
        "module": "india_stocks_api.brokers.upstox.api.auth_api",
        "function": "authenticate_broker",
        "env_vars": [
            "UPSTOX_CLIENT_ID",
            "UPSTOX_CLIENT_SECRET",
            "UPSTOX_REDIRECT_URI",
            "BROKER_API_KEY",
        ],
    },
}


def generate_totp_code(secret: str) -> str:
    """Generate TOTP code from secret"""
    try:
        totp = pyotp.TOTP(secret)
        return totp.now()
    except Exception as e:
        logger.error(f"Error generating TOTP code: {e}")
        return None


def decode_jwt_expiry(jwt_token: str) -> Optional[datetime]:
    """Decode JWT token to get actual expiry time in IST"""
    try:
        # JWT has 3 parts separated by dots
        parts = jwt_token.split(".")
        if len(parts) != 3:
            return None

        # Decode the payload (middle part)
        payload = parts[1]
        # Add padding if needed
        payload += "=" * (4 - len(payload) % 4)
        decoded = base64.b64decode(payload)
        payload_data = json.loads(decoded)

        # Get expiry timestamp and convert to IST
        exp_timestamp = payload_data.get("exp")
        if exp_timestamp:
            # JWT timestamps are in UTC, so we need to create UTC datetime first
            utc_datetime = datetime.fromtimestamp(exp_timestamp, tz=pytz.UTC)
            return utc_datetime.astimezone(IST)

        return None

    except Exception as e:
        logger.error(f"Error decoding JWT expiry: {e}")
        return None


def decode_jwt_issued_at(jwt_token: str) -> Optional[datetime]:
    """Decode JWT token to get actual issued time in IST"""
    try:
        # JWT has 3 parts separated by dots
        parts = jwt_token.split(".")
        if len(parts) != 3:
            return None

        # Decode the payload (middle part)
        payload = parts[1]
        # Add padding if needed
        payload += "=" * (4 - len(payload) % 4)
        decoded = base64.b64decode(payload)
        payload_data = json.loads(decoded)

        # Get issued timestamp and convert to IST
        iat_timestamp = payload_data.get("iat")
        if iat_timestamp:
            # JWT timestamps are in UTC, so we need to create UTC datetime first
            utc_datetime = datetime.fromtimestamp(iat_timestamp, tz=pytz.UTC)
            return utc_datetime.astimezone(IST)

        return None

    except Exception as e:
        logger.error(f"Error decoding JWT issued time: {e}")
        return None


def get_broker_credentials(broker_name: str) -> Dict[str, str]:
    """Get broker credentials from environment variables"""
    if broker_name not in BROKER_AUTH_MAPPING:
        raise ValueError(f"Unsupported broker: {broker_name}")

    credentials = {}
    for env_var in BROKER_AUTH_MAPPING[broker_name]["env_vars"]:
        value = os.getenv(env_var)
        if value:
            credentials[env_var] = value
        else:
            logger.warning(f"Missing environment variable: {env_var}")

    return credentials


def validate_broker_credentials(broker_name: str) -> bool:
    """Validate that all required credentials are present"""
    credentials = get_broker_credentials(broker_name)
    required_vars = BROKER_AUTH_MAPPING[broker_name]["env_vars"]

    missing_vars = [var for var in required_vars if var not in credentials]
    if missing_vars:
        logger.error(f"Missing required credentials for {broker_name}: {missing_vars}")
        return False

    return True


def store_broker_token(
    broker_name: str,
    auth_token: str,
    feed_token: str = None,
    broker_user_id: str = None,
    expires_in_hours: int = 24,
) -> bool:
    """Store broker authentication token"""
    try:
        # Ensure directory exists
        TOKEN_STORAGE_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Load existing tokens
        tokens = {}
        if TOKEN_STORAGE_PATH.exists():
            with open(TOKEN_STORAGE_PATH, "r") as f:
                tokens = json.load(f)

        # Try to get actual expiry from JWT token, fallback to default
        expires_at = decode_jwt_expiry(auth_token)
        if not expires_at:
            expires_at = get_india_time() + timedelta(hours=expires_in_hours)
            logger.warning(
                f"Could not decode JWT expiry for {broker_name}, using default {expires_in_hours}h"
            )

        # Try to get actual issued time from JWT token, fallback to current time
        created_at = decode_jwt_issued_at(auth_token)
        if not created_at:
            created_at = get_india_time()
            logger.warning(
                f"Could not decode JWT issued time for {broker_name}, using current time"
            )

        tokens[broker_name] = {
            "auth_token": auth_token,
            "feed_token": feed_token,
            "broker_user_id": broker_user_id,
            "expires_at": expires_at.isoformat(),
            "created_at": created_at.isoformat(),
        }

        # Save tokens
        with open(TOKEN_STORAGE_PATH, "w") as f:
            json.dump(tokens, f, indent=2)

        logger.info(f"Stored authentication token for {broker_name}")
        return True

    except Exception as e:
        logger.error(f"Error storing token for {broker_name}: {e}")
        return False


def get_broker_token(broker_name: str) -> Optional[Dict]:
    """Get broker authentication token"""
    try:
        if not TOKEN_STORAGE_PATH.exists():
            return None

        with open(TOKEN_STORAGE_PATH, "r") as f:
            tokens = json.load(f)

        return tokens.get(broker_name)

    except Exception as e:
        logger.error(f"Error getting token for {broker_name}: {e}")
        return None


def is_token_valid(broker_name: str) -> bool:
    """Check if token is still valid (not expired)"""
    token_data = get_broker_token(broker_name)
    if not token_data:
        return False

    try:
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        # Ensure expires_at is timezone-aware (IST)
        if expires_at.tzinfo is None:
            expires_at = IST.localize(expires_at)

        current_time = get_india_time()
        return current_time < expires_at
    except Exception as e:
        logger.error(f"Error checking token validity for {broker_name}: {e}")
        return False


def clear_broker_token(broker_name: str) -> bool:
    """Clear broker authentication token"""
    try:
        if not TOKEN_STORAGE_PATH.exists():
            return True

        with open(TOKEN_STORAGE_PATH, "r") as f:
            tokens = json.load(f)

        if broker_name in tokens:
            del tokens[broker_name]

            with open(TOKEN_STORAGE_PATH, "w") as f:
                json.dump(tokens, f, indent=2)

            logger.info(f"Cleared authentication token for {broker_name}")

        return True

    except Exception as e:
        logger.error(f"Error clearing token for {broker_name}: {e}")
        return False


def authenticate_broker(broker_name: str, force_refresh: bool = False) -> Dict:
    """
    Authenticate with broker using existing auth_api.py functions

    Args:
        broker_name: Broker name (angelone, zerodha, shoonya, etc.)
        force_refresh: Force re-authentication even if token exists

    Returns:
        {
            "success": bool,
            "auth_token": str,
            "feed_token": str,
            "broker_user_id": str,
            "error": str
        }
    """
    try:
        # Check if already authenticated and token is valid
        if not force_refresh and is_token_valid(broker_name):
            token_data = get_broker_token(broker_name)
            logger.info(f"Using existing valid token for {broker_name}")
            return {
                "success": True,
                "auth_token": token_data["auth_token"],
                "feed_token": token_data.get("feed_token"),
                "broker_user_id": token_data.get("broker_user_id"),
                "error": None,
            }

        # Validate credentials
        if not validate_broker_credentials(broker_name):
            return {
                "success": False,
                "auth_token": None,
                "feed_token": None,
                "broker_user_id": None,
                "error": f"Missing required credentials for {broker_name}",
            }

        # Get credentials
        credentials = get_broker_credentials(broker_name)

        # Import and call broker-specific auth function
        auth_config = BROKER_AUTH_MAPPING[broker_name]
        module = importlib.import_module(auth_config["module"])
        auth_function = getattr(module, auth_config["function"])

        # Prepare parameters based on broker
        if broker_name == "angelone":
            totp_code = generate_totp_code(credentials["ANGELONE_TOTP_SECRET"])
            if not totp_code:
                return {
                    "success": False,
                    "auth_token": None,
                    "feed_token": None,
                    "broker_user_id": None,
                    "error": "Failed to generate TOTP code",
                }

            auth_token, feed_token, error = auth_function(
                credentials["ANGELONE_CLIENT_CODE"],
                credentials["ANGELONE_PIN"],
                totp_code,
            )

        elif broker_name == "shoonya":
            totp_code = generate_totp_code(credentials["SHOONYA_TOTP_SECRET"])
            if not totp_code:
                return {
                    "success": False,
                    "auth_token": None,
                    "feed_token": None,
                    "broker_user_id": None,
                    "error": "Failed to generate TOTP code",
                }

            auth_token, error = auth_function(
                credentials["SHOONYA_USER_ID"],
                credentials["SHOONYA_PASSWORD"],
                totp_code,
            )
            feed_token = None

        elif broker_name == "dhan":
            auth_token, error = auth_function(
                credentials["DHAN_CLIENT_ID"], credentials["DHAN_PASSWORD"]
            )
            feed_token = None

        elif broker_name == "dhan_sandbox":
            auth_token, error = auth_function(
                credentials["DHAN_SANDBOX_CLIENT_ID"],
                credentials["DHAN_SANDBOX_PASSWORD"],
            )
            feed_token = None

        elif broker_name == "fyers":
            auth_token, error = auth_function(
                credentials["FYERS_CLIENT_ID"],
                credentials["FYERS_PASSWORD"],
                credentials["FYERS_PIN"],
            )
            feed_token = None

        elif broker_name == "groww":
            totp_code = generate_totp_code(credentials["GROWW_TOTP_SECRET"])
            if not totp_code:
                return {
                    "success": False,
                    "auth_token": None,
                    "feed_token": None,
                    "broker_user_id": None,
                    "error": "Failed to generate TOTP code",
                }

            auth_token, error = auth_function(
                credentials["GROWW_USER_ID"], credentials["GROWW_PASSWORD"], totp_code
            )
            feed_token = None

        elif broker_name == "fivepaisa":
            totp_code = generate_totp_code(credentials["FIVEPAISA_TOTP_SECRET"])
            if not totp_code:
                return {
                    "success": False,
                    "auth_token": None,
                    "feed_token": None,
                    "broker_user_id": None,
                    "error": "Failed to generate TOTP code",
                }

            auth_token, error = auth_function(
                credentials["FIVEPAISA_CLIENT_CODE"],
                credentials["FIVEPAISA_PASSWORD"],
                totp_code,
            )
            feed_token = None

        elif broker_name == "fivepaisaxts":
            totp_code = generate_totp_code(credentials["FIVEPAISAXTS_TOTP_SECRET"])
            if not totp_code:
                return {
                    "success": False,
                    "auth_token": None,
                    "feed_token": None,
                    "broker_user_id": None,
                    "error": "Failed to generate TOTP code",
                }

            auth_token, error = auth_function(
                credentials["FIVEPAISAXTS_CLIENT_CODE"],
                credentials["FIVEPAISAXTS_PASSWORD"],
                totp_code,
            )
            feed_token = None

        elif broker_name == "upstox":
            auth_token, error = auth_function(
                credentials["UPSTOX_CLIENT_ID"],
                credentials["UPSTOX_CLIENT_SECRET"],
                credentials["UPSTOX_REDIRECT_URI"],
            )
            feed_token = None

        else:
            return {
                "success": False,
                "auth_token": None,
                "feed_token": None,
                "broker_user_id": None,
                "error": f"Authentication not implemented for {broker_name}",
            }

        # Check authentication result
        if auth_token and not error:
            # Store token
            broker_user_id = None
            if broker_name == "angelone":
                broker_user_id = credentials["ANGELONE_CLIENT_CODE"]
            elif broker_name == "shoonya":
                broker_user_id = credentials["SHOONYA_USER_ID"]
            elif broker_name == "dhan":
                broker_user_id = credentials["DHAN_CLIENT_ID"]
            elif broker_name == "dhan_sandbox":
                broker_user_id = credentials["DHAN_SANDBOX_CLIENT_ID"]
            elif broker_name == "fyers":
                broker_user_id = credentials["FYERS_CLIENT_ID"]
            elif broker_name == "groww":
                broker_user_id = credentials["GROWW_USER_ID"]
            elif broker_name == "fivepaisa":
                broker_user_id = credentials["FIVEPAISA_CLIENT_CODE"]
            elif broker_name == "fivepaisaxts":
                broker_user_id = credentials["FIVEPAISAXTS_CLIENT_CODE"]
            elif broker_name == "upstox":
                broker_user_id = credentials["UPSTOX_CLIENT_ID"]

            store_broker_token(broker_name, auth_token, feed_token, broker_user_id)

            logger.info(f"Successfully authenticated with {broker_name}")
            return {
                "success": True,
                "auth_token": auth_token,
                "feed_token": feed_token,
                "broker_user_id": broker_user_id,
                "error": None,
            }
        else:
            logger.error(f"Authentication failed for {broker_name}: {error}")
            return {
                "success": False,
                "auth_token": None,
                "feed_token": None,
                "broker_user_id": None,
                "error": error or "Authentication failed",
            }

    except Exception as e:
        logger.error(f"Error authenticating with {broker_name}: {e}")
        return {
            "success": False,
            "auth_token": None,
            "feed_token": None,
            "broker_user_id": None,
            "error": str(e),
        }


def is_authenticated(broker_name: str) -> bool:
    """Check if broker is authenticated and token is valid"""
    return is_token_valid(broker_name)


def logout(broker_name: str) -> bool:
    """Logout and clear broker tokens"""
    return clear_broker_token(broker_name)


def get_auth_headers(broker_name: str) -> Dict[str, str]:
    """Get authentication headers for broker API calls"""
    token_data = get_broker_token(broker_name)
    if not token_data:
        return {}

    auth_token = token_data["auth_token"]

    # Broker-specific header formats
    if broker_name == "angelone":
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name == "zerodha":
        return {"Authorization": f"token {auth_token}"}
    elif broker_name == "shoonya":
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name in ["dhan", "dhan_sandbox"]:
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name == "fyers":
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name == "groww":
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name in ["fivepaisa", "fivepaisaxts"]:
        return {"Authorization": f"Bearer {auth_token}"}
    elif broker_name == "upstox":
        return {"Authorization": f"Bearer {auth_token}"}
    else:
        return {"Authorization": f"Bearer {auth_token}"}


def get_supported_brokers() -> list:
    """Get list of supported brokers"""
    return list(BROKER_AUTH_MAPPING.keys())


def get_missing_credentials(broker_name: str) -> list:
    """Get list of missing required credentials"""
    if broker_name not in BROKER_AUTH_MAPPING:
        return []

    credentials = get_broker_credentials(broker_name)
    required_vars = BROKER_AUTH_MAPPING[broker_name]["env_vars"]

    return [var for var in required_vars if var not in credentials]
