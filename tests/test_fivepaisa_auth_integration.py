"""Integration test for FivePaisa authentication"""

import json
from pathlib import Path
import os
from india_stocks_api.brokers.fivepaisa import FivePaisa
from india_stocks_api.internal.context import get_auth_token, get_credentials
from dotenv import load_dotenv

load_dotenv()


def test_fivepaisa_auth_integration():
    """Test FivePaisa authentication with real credentials"""
    
    # Hardcoded placeholder credentials - user should replace these
    api_key = os.getenv("FIVEPAISA_API_KEY")
    clientcode = os.getenv("FIVEPAISA_CLIENT_CODE")  # For FivePaisa, this is the email ID
    broker_pin = os.getenv("FIVEPAISA_PIN")
    totp =os.getenv("FIVEPAISA_TOTP")
    user_id=os.getenv("FIVEPAISA_USER_ID")
    api_secret=os.getenv("FIVEPAISA_ENCRYPTION_KEY")
    
    # If environment variables are not set, use stored credentials
    if not all([api_key, clientcode, broker_pin, totp]):
        stored_creds = get_credentials("fivepaisa")
        api_key = api_key or stored_creds.get("api_key")
        clientcode = clientcode or stored_creds.get("clientcode")
        broker_pin = broker_pin or stored_creds.get("broker_pin")
        totp = totp or stored_creds.get("totp_code")
        user_id = user_id or stored_creds.get("user_id")
        api_secret = api_secret or stored_creds.get("api_secret")

    # Clean up any existing creds.json
    creds_file = Path("_cache/creds.json")
    if creds_file.exists():
        creds_file.unlink()
    
    # Create FivePaisa instance with credentials
    broker = FivePaisa(api_key=api_key, clientcode=clientcode, broker_pin=broker_pin, totp_code=totp, api_secret=api_secret, user_id=user_id)
    # Test authentication
    auth_result = broker.authenticate()
    assert auth_result is True, "FivePaisa authentication should succeed"
    
    # Verify tokens are set in context
    auth_token = get_auth_token()
    
    assert auth_token is not None, "Auth token should be set after successful authentication"
    
    # Verify credentials are persisted
    assert creds_file.exists(), "creds.json should be created after successful authentication"
    
    # Load and verify saved credentials
    with open(creds_file, 'r') as f:
        saved_creds = json.load(f)
    
    assert "fivepaisa" in saved_creds, "FivePaisa credentials should be saved"
    fivepaisa_creds = saved_creds["fivepaisa"]
    
    assert fivepaisa_creds["api_key"] == api_key, "API key should be saved"
    assert fivepaisa_creds["clientcode"] == clientcode, "Client code should be saved"
    assert fivepaisa_creds["broker_pin"] == broker_pin, "Broker pin should be saved"
    assert fivepaisa_creds["totp_code"] == totp, "TOTP code should be saved"
    assert fivepaisa_creds["user_id"] == user_id, "User ID should be saved"
    assert fivepaisa_creds["api_secret"] == api_secret, "API secret should be saved"
    
    print("FivePaisa authentication test passed!")
    print(f"Auth token: {auth_token[:10]}****")
