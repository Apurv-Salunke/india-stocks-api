import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_imports():
    print("Testing imports...")
    try:
        from india_stocks_api.brokers import AngelOne

        print("  Imported nested symbols successfully.")

        # Instantiate (Dry Run)
        client = AngelOne("KEY", "CODE", "PASS", "TOTP")
        print("  Instantiated AngelOne client successfully.")

        print("Test Passed!")
    except Exception as e:
        print(f"Test Failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_imports()
