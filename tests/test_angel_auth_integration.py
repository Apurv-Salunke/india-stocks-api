import os
from dotenv import load_dotenv
from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.internal.context import get_auth_token, get_feed_token, get_credentials

# Load environment variables from .env file
load_dotenv()

def test_angel_one_authentication():
    """
    Verify end-to-end AngelOne authentication and credential persistence.
    
    This integration test uses credentials from environment variables (ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET) falling back to stored credentials from get_credentials("angel") if any are missing. It authenticates with AngelOne, asserts authentication succeeds, verifies that auth and feed tokens are present, and checks that stored credentials contain the expected keys and match the values used for authentication.
    
    Raises:
        AssertionError: If required credentials are missing, authentication fails, tokens are absent, or stored credentials do not match.
    """
    # Try environment variables first, fallback to stored credentials
    api_key = os.getenv("ANGEL_API_KEY")
    clientcode = os.getenv("ANGEL_CLIENT_ID") 
    password = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")
    
    # If environment variables are not set, use stored credentials
    if not all([api_key, clientcode, password, totp]):
        stored_creds = get_credentials("angel")
        api_key = api_key or stored_creds.get("api_key")
        clientcode = clientcode or stored_creds.get("client_code")
        password = password or stored_creds.get("password")
        totp = totp or stored_creds.get("totp_key")
    
    # Ensure we have all required credentials
    assert all([api_key, clientcode, password, totp]), "Missing credentials - set environment variables or ensure creds.json exists"
    
    # Show which credentials are being used (masked for security)
    print(f"Using credentials: clientcode={clientcode[:4]}XXXX")
    
    # Create AngelOne with credentials
    broker = AngelOne(api_key=api_key, client_code=clientcode, password=password, totp_key=totp)
    
    # Call authenticate()
    auth_result = broker.authenticate()
    
    # Assert authentication succeeded
    assert auth_result is True
    
    # Assert tokens exist
    auth_token = get_auth_token()
    feed_token = get_feed_token()
    
    assert auth_token is not None
    assert feed_token is not None
    

    creds = get_credentials("angel")
    
    assert "api_key" in creds
    assert "client_code" in creds
    assert "password" in creds
    assert "totp_key" in creds
    
    # Verify the saved credentials match what we passed
    assert creds["api_key"] == api_key
    assert creds["client_code"] == clientcode
    assert creds["password"] == password
    assert creds["totp_key"] == totp