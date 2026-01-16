"""
Simple authentication example for India Stocks API
"""

from india_stocks_api.auth import (
    authenticate_broker,
    is_authenticated,
    get_auth_headers,
    get_supported_brokers,
    get_missing_credentials,
)


def main():
    print("🚀 India Stocks API Authentication Example")
    print("=" * 50)

    # Show supported brokers
    print(f"📋 Supported brokers: {', '.join(get_supported_brokers())}")
    print()

    # Example 1: Check missing credentials
    broker_name = "angelone"
    print(f"🔍 Checking credentials for {broker_name}...")
    missing = get_missing_credentials(broker_name)
    if missing:
        print(f"❌ Missing credentials: {missing}")
        print("💡 Please set these environment variables in your .env file")
        return
    else:
        print("✅ All required credentials are present")

    print()

    # Example 2: Authenticate with broker
    print(f"🔐 Authenticating with {broker_name}...")
    result = authenticate_broker(broker_name)

    if result["success"]:
        print("✅ Authentication successful!")
        print(f"   Auth Token: {result['auth_token'][:20]}...")
        if result["feed_token"]:
            print(f"   Feed Token: {result['feed_token'][:20]}...")
        print(f"   Broker User ID: {result['broker_user_id']}")
    else:
        print(f"❌ Authentication failed: {result['error']}")
        return

    print()

    # Example 3: Check authentication status
    print("🔍 Checking authentication status...")
    if is_authenticated(broker_name):
        print("✅ Broker is authenticated and ready to use")
    else:
        print("❌ Broker is not authenticated")
        return

    print()

    # Example 4: Get authentication headers
    print("📋 Getting authentication headers...")
    headers = get_auth_headers(broker_name)
    print(f"   Headers: {headers}")

    print()

    # # Example 5: Logout
    # print(f"🚪 Logging out from {broker_name}...")
    # if logout(broker_name):
    #     print("✅ Successfully logged out")
    # else:
    #     print("❌ Failed to logout")

    # print()
    # print("🎉 Authentication example completed!")


if __name__ == "__main__":
    main()
