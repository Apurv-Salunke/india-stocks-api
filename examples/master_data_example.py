"""
Simple master data example for India Stocks API
Demonstrates downloading master contract data for brokers
"""

from india_stocks_api.master_data import (
    download_master_data,
    download_all_masters,
)
from india_stocks_api.auth import (
    authenticate_broker,
    get_supported_brokers,
)


def main():
    print("🗄️  India Stocks API Master Data Example")
    print("=" * 50)

    # Show supported brokers
    print(f"📋 Supported brokers: {', '.join(get_supported_brokers())}")
    print()

    # Example 1: Download master data for a specific broker
    print("📥 Example 1: Download Master Data for Specific Broker")
    print("-" * 50)
    
    broker_name = "angelone"
    print(f"🔐 Authenticating with {broker_name}...")
    
    result = authenticate_broker(broker_name)
    if not result["success"]:
        print(f"❌ Authentication failed: {result['error']}")
        print("   Master data download requires authentication!")
        return
    
    print("✅ Authentication successful!")
    print()
    
    print(f"📥 Starting master contract download for {broker_name}...")
    print("   This may take a few moments as it fetches contract data from the broker...")
    print()
    
    master_result = download_master_data(broker_name)
    
    if master_result["status"] == "success":
        print(f"✅ Master data download successful!")
        print(f"   Message: {master_result['message']}")
        print(f"   ✓ All contracts for {broker_name} have been downloaded and stored")
    else:
        print(f"❌ Master data download failed!")
        print(f"   Error: {master_result['message']}")
    
    print()

    print("💡 Using Downloaded Master Data")
    print("-" * 50)
    print("""
The downloaded master data is now available for use:

✓ Access instruments by symbol
✓ Get contract specifications (lot size, tick size, etc.)
✓ Filter by exchange (NSE, BSE, NFO, NCDEX, MCX)
✓ Get instrument type (EQUITY, FUTURES, OPTIONS)
✓ Find trading sessions and market hours

After master download, you can:
- Query instrument details without API calls
- Build symbol mapping for your strategies
- Validate symbols before placing orders
- Access contract specifications for position sizing
""")
    
    print()
    print("🎉 Master Data example completed!")


if __name__ == "__main__":
    main()
