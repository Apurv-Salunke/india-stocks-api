"""
Test Auto-Provisioning of Instruments Database
Verifies that instruments.db is automatically downloaded on broker instantiation.
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_auto_provisioning():
    print("--- Testing Auto-Provisioning of Instruments DB ---\n")

    # 1. Remove existing DB to force fresh download
    db_path = Path(__file__).parent.parent / "instruments.db"
    if db_path.exists():
        print(f"Removing existing DB: {db_path}")
        os.remove(db_path)

    # 2. Instantiate broker (should trigger auto-download)
    print("\n🎯 Creating AngelOne instance...")
    print("   (This should automatically download master contract)\n")

    from india_stocks_api.brokers import AngelOne

    # Use dummy credentials - we're only testing DB provisioning, not authentication
    broker = AngelOne(api_key="DUMMY_KEY", client_code="DUMMY_CODE", password="DUMMY_PIN", totp_key="DUMMY_TOTP")

    # 3. Verify DB was created
    if db_path.exists():
        file_size = db_path.stat().st_size
        print("✅ SUCCESS! Instruments DB created automatically")
        print(f"   Location: {db_path}")
        print(f"   Size: {file_size:,} bytes\n")

        # 4. Verify DB has data
        from india_stocks_api.instruments.database import InstrumentDB

        db = InstrumentDB(str(db_path))
        session = db.get_session()
        from india_stocks_api.instruments.database import InstrumentMaster

        count = session.query(InstrumentMaster).count()
        session.close()

        print(f"✅ Database contains {count:,} instruments\n")

        # 5. Test staleness check
        print("🔍 Testing staleness check...")
        print(f"   DB is stale: {broker._is_db_stale(str(db_path))}")
        print("   (Should be False since we just created it)\n")

    else:
        print(f"❌ FAILED: Instruments DB was not created at {db_path}\n")
        return False

    print("🎉 All tests passed! Auto-provisioning works correctly.")
    return True


if __name__ == "__main__":
    try:
        success = test_auto_provisioning()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
