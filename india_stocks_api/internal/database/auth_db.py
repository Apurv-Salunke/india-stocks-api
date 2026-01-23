"""
Simple authentication system for India Stocks API
Uses existing auth_api.py files from OpenAlgo brokers
"""

import json
import pyotp
import base64
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
import pytz
from dotenv import load_dotenv

from algotrade.internal.utils.logging import get_logger
from algotrade.internal.utils.common import get_india_time, IST

logger = get_logger(__name__)
load_dotenv()

# Token storage file
TOKEN_STORAGE_PATH = Path(__file__).parent.parent.parent / "_cache" / "auth_tokens.json"

# Initialize directory once on module import
TOKEN_STORAGE_PATH.parent.mkdir(parents=True, exist_ok=True)


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


def store_broker_token(
    broker_name: str,
    auth_token: str,
    feed_token: str = None,
    client_id: str = None,
    expires_in_hours: int = 24,
) -> bool:
    """Store broker authentication token with optimized file handling"""
    try:
        # 1. Prepare New Data
        expires_at = decode_jwt_expiry(auth_token)
        if not expires_at:
            expires_at = get_india_time() + timedelta(hours=expires_in_hours)
            logger.warning(f"Using default expiry for {broker_name}")

        created_at = decode_jwt_issued_at(auth_token) or get_india_time()

        new_entry = {
            "auth_token": auth_token,
            "feed_token": feed_token,
            "client_id": client_id,
            "expires_at": expires_at.isoformat(),
            "created_at": created_at.isoformat(),
        }

        # 2. Read-Modify-Write
        tokens = {}
        if TOKEN_STORAGE_PATH.exists():
            with open(TOKEN_STORAGE_PATH, "r") as f:
                try:
                    tokens = json.load(f)
                except json.JSONDecodeError:
                    logger.error("Auth file corrupted, resetting storage.")

        tokens[broker_name] = new_entry

        # 3. Atomic Write (Optional but recommended)
        # Writing to a temp file then renaming is safer than writing directly
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

def is_authenticated(broker_name: str) -> bool:
    """Check if broker is authenticated and token is valid"""
    return is_token_valid(broker_name)


