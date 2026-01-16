"""
Angel Broker Funds API Example
Demonstrates how to use the get_margin_data function from Angel Broker
"""

from india_stocks_api.funds import get_margin
from india_stocks_api.auth import authenticate_broker, get_missing_credentials
from dotenv import load_dotenv

load_dotenv()


def main():
    print("🚀 Angel Broker Funds API Example")
    print("=" * 50)

    broker_name = "angelone"

    # Step 1: Check missing credentials
    print(f"🔍 Checking credentials for {broker_name}...")
    missing = get_missing_credentials(broker_name)
    if missing:
        print(f"❌ Missing credentials: {missing}")
        print("💡 Please set these environment variables in your .env file")
        return
    else:
        print("✅ All required credentials are present")

    print()

    # Step 2: Authenticate with broker
    print(f"🔐 Authenticating with {broker_name}...")
    auth_result = authenticate_broker(broker_name)

    if not auth_result["success"]:
        print(f"❌ Authentication failed: {auth_result['error']}")
        return

    print("✅ Authentication successful!")

    print()

    # Step 3: Get margin data using the auth token
    print("📊 Fetching margin data...")
    try:
        margin_data = get_margin(broker_name)
        
        if margin_data:
            print("✅ Margin data retrieved successfully!")
            print()
            print("💰 Margin Details:")
            print(f"   Available Cash: ₹{margin_data.get('availablecash', 'N/A')}")
            print(f"   Collateral: ₹{margin_data.get('collateral', 'N/A')}")
            print(f"   M2M Realized: ₹{margin_data.get('m2mrealized', 'N/A')}")
            print(f"   M2M Unrealized: ₹{margin_data.get('m2munrealized', 'N/A')}")
            print(f"   Utilised Debits: ₹{margin_data.get('utiliseddebits', 'N/A')}")
        else:
            print("❌ No margin data returned")

    except Exception as e:
        print(f"❌ Error fetching margin data: {str(e)}")
        return

    print()
    print("🎉 Angel Broker funds example completed!")


if __name__ == "__main__":
    main()
