"""
Example: How to Connect to Brokers with India Stocks API
"""

import os
from india_stocks_api.auth import (
    authenticate_broker,
    is_authenticated,
    get_auth_headers,
    get_supported_brokers,
    get_missing_credentials,
)


def setup_angelone_credentials():
    """Set up AngelOne credentials (example)"""
    # Method 1: Direct environment variables
    os.environ["ANGELONE_CLIENT_CODE"] = "A1357374"
    os.environ["ANGELONE_PIN"] = "8989"
    os.environ["ANGELONE_TOTP_SECRET"] = "EL7KI5RGSJ5ZXZ625FDETRO44I"
    os.environ["BROKER_API_KEY"] = "SP7RmmCu"

    print("✅ AngelOne credentials set up")


def connect_to_broker(broker_name):
    """Connect to a broker with proper error handling"""

    print(f"\n🔍 Checking {broker_name} setup...")

    # Check if broker is supported
    supported_brokers = get_supported_brokers()
    if broker_name not in supported_brokers:
        print(f"❌ Unsupported broker: {broker_name}")
        print(f"   Supported brokers: {supported_brokers}")
        return False

    # Check missing credentials
    missing = get_missing_credentials(broker_name)
    if missing:
        print(f"❌ Missing credentials for {broker_name}:")
        for cred in missing:
            print(f"   - {cred}")
        print("💡 Please set these environment variables")
        return False

    print(f"✅ All required credentials present for {broker_name}")

    # Authenticate
    print(f"🔐 Authenticating with {broker_name}...")
    result = authenticate_broker(broker_name)

    if result["success"]:
        print(f"✅ Successfully connected to {broker_name}!")
        print(f"   Auth Token: {result['auth_token'][:30]}...")
        if result["feed_token"]:
            print(f"   Feed Token: {result['feed_token'][:30]}...")
        print(f"   Broker User ID: {result['broker_user_id']}")
        return True
    else:
        print(f"❌ Authentication failed: {result['error']}")
        return False


def check_authentication_status(broker_name):
    """Check if broker is authenticated"""
    if is_authenticated(broker_name):
        print(f"✅ {broker_name} is authenticated and ready to use")

        # Get authentication headers for API calls
        headers = get_auth_headers(broker_name)
        print(f"📋 Auth headers: {headers}")
        return True
    else:
        print(f"❌ {broker_name} is not authenticated")
        return False


def main():
    """Main example function"""
    print("🚀 India Stocks API - Broker Connection Example")
    print("=" * 50)

    # Step 1: Set up credentials
    print("\n📋 Step 1: Setting up credentials...")
    setup_angelone_credentials()

    # Step 2: Connect to broker
    print("\n🔗 Step 2: Connecting to broker...")
    broker_name = "angel"

    if connect_to_broker(broker_name):
        # Step 3: Check authentication status
        print("\n🔍 Step 3: Checking authentication status...")
        check_authentication_status(broker_name)

        print(f"\n🎉 {broker_name} is ready for trading operations!")
        print("\n💡 Next steps:")
        print("   - Use get_auth_headers() to get API headers")
        print("   - Call broker APIs with authentication headers")
        print("   - Tokens are automatically stored and managed")
    else:
        print(f"\n❌ Failed to connect to {broker_name}")
        print("\n💡 Troubleshooting:")
        print("   1. Check your credentials are correct")
        print("   2. Ensure TOTP secret is valid")
        print("   3. Verify API key is active")
        print("   4. Check broker's API status")


if __name__ == "__main__":
    main()
