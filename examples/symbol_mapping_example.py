#!/usr/bin/env python3
"""
Symbol Mapping Example - OpenAlgo-style Functions
Demonstrates how to use the symbol mapping functions to convert between
standardized symbols and broker-specific symbols.
"""

from india_stocks_api.database.broker_instruments_db import (
    get_broker_token,
    get_broker_symbol,
    get_standardized_symbol,
    get_broker_exchange_code,
    get_symbol_info,
    get_all_broker_symbols,
    get_symbol_count,
)


def main():
    print("🔗 SYMBOL MAPPING EXAMPLES")
    print("=" * 50)

    # Example 1: Get broker-specific token for RELIANCE
    print("\n📊 Example 1: Get broker tokens for RELIANCE")
    print("-" * 40)

    brokers = ["angelone"]
    for broker in brokers:
        token = get_broker_token("RELIANCE", "NSE", broker)
        symbol = get_broker_symbol("RELIANCE", "NSE", broker)
        print(f"  {broker:12}: {symbol:20} (token: {token})")

    # Example 2: Get broker-specific symbols for BANKNIFTY
    print("\n📊 Example 2: Get broker symbols for BANKNIFTY")
    print("-" * 40)

    for broker in brokers:
        symbol = get_broker_symbol("BANKNIFTY", "NSE_INDEX", broker)
        token = get_broker_token("BANKNIFTY", "NSE_INDEX", broker)
        print(f"  {broker:12}: {symbol:30} (token: {token})")

    # Example 3: Convert broker symbol back to standardized symbol
    print("\n📊 Example 3: Convert broker symbols to standardized symbols")
    print("-" * 40)

    broker_symbols = [
        ("NSE:RELIANCE-EQ", "NSE", "fyers"),
        ("RELIANCE-EQ", "NSE", "angelone"),
        ("NSE:NIFTYBANK-INDEX", "NSE_INDEX", "fyers"),
        ("Nifty Bank", "NSE_INDEX", "angelone"),
    ]

    for broker_symbol, exchange, broker in broker_symbols:
        std_symbol = get_standardized_symbol(broker_symbol, exchange, broker)
        print(f"  {broker:12}: {broker_symbol:25} -> {std_symbol}")

    # Example 4: Get broker exchange codes
    print("\n📊 Example 4: Get broker exchange codes")
    print("-" * 40)

    for broker in brokers:
        broker_exchange = get_broker_exchange_code("RELIANCE", "NSE", broker)
        print(f"  {broker:12}: {broker_exchange}")

    # Example 5: Get complete symbol information
    print("\n📊 Example 5: Get complete symbol information")
    print("-" * 40)

    info = get_symbol_info("RELIANCE", "NSE", "angelone")
    if info:
        print(f"  Symbol: {info['standardized_symbol']}")
        print(f"  Broker Symbol: {info['broker_symbol']}")
        print(f"  Token: {info['broker_token']}")
        print(f"  Name: {info['instrument_name']}")
        print(f"  Exchange: {info['exchange_code']}")
        print(f"  Broker Exchange: {info['broker_exchange_code']}")
        print(f"  Lot Size: {info['lot_size']}")
        print(f"  Tick Size: {info['tick_size']}")
        print(f"  Instrument Type: {info['instrument_type']}")

    # Example 6: Get all broker mappings for a symbol
    print("\n📊 Example 6: Get all broker mappings for RELIANCE")
    print("-" * 40)

    all_mappings = get_all_broker_symbols("RELIANCE", "NSE")
    for broker, data in all_mappings.items():
        print(
            f"  {broker:15}: {data['broker_symbol']:20} (token: {data['broker_token']})"
        )

    # Example 7: Get all broker mappings for BANKNIFTY
    print("\n📊 Example 7: Get all broker mappings for BANKNIFTY")
    print("-" * 40)

    all_mappings = get_all_broker_symbols("BANKNIFTY", "NSE_INDEX")
    for broker, data in all_mappings.items():
        print(
            f"  {broker:15}: {data['broker_symbol']:30} (token: {data['broker_token']})"
        )

    # Example 8: Get all broker mappings for INDIAVIX
    print("\n📊 Example 8: Get all broker mappings for INDIAVIX")
    print("-" * 40)

    all_mappings = get_all_broker_symbols("INDIAVIX", "NSE_INDEX")
    for broker, data in all_mappings.items():
        print(
            f"  {broker:15}: {data['broker_symbol']:30} (token: {data['broker_token']})"
        )

    # Example 9: Database statistics
    print("\n📊 Example 9: Database statistics")
    print("-" * 40)

    total_symbols = get_symbol_count()
    print(f"  Total symbols in database: {total_symbols:,}")

    # Example 10: Test with different instrument types
    print("\n📊 Example 10: Test with different instrument types")
    print("-" * 40)

    # Test with options
    print("  Testing with NIFTY options:")
    nifty_options = [
        ("NIFTY28OCT2525000CE", "NFO", "angelone"),
        ("NIFTY28OCT2525000PE", "NFO", "angelone"),
    ]

    for symbol, exchange, broker in nifty_options:
        token = get_broker_token(symbol, exchange, broker)
        broker_symbol = get_broker_symbol(symbol, exchange, broker)
        if token:
            print(f"    {symbol:20} -> {broker_symbol:25} (token: {token})")
        else:
            print(f"    {symbol:20} -> Not found")

    # Example 11: Test with futures
    print("\n📊 Example 11: Test with futures")
    print("-" * 40)

    print("  Testing with NIFTY futures:")
    nifty_futures = [
        ("NIFTY28OCT25FUT", "NFO", "angelone"),
        ("NIFTY30DEC25FUT", "NFO", "angelone"),
    ]

    for symbol, exchange, broker in nifty_futures:
        token = get_broker_token(symbol, exchange, broker)
        broker_symbol = get_broker_symbol(symbol, exchange, broker)
        if token:
            print(f"    {symbol:20} -> {broker_symbol:25} (token: {token})")
        else:
            print(f"    {symbol:20} -> Not found")

    # Example 12: Test with BSE symbols
    print("\n📊 Example 12: Test with BSE symbols")
    print("-" * 40)

    print("  Testing with BSE RELIANCE:")
    bse_brokers = ["angelone"]
    for broker in bse_brokers:
        token = get_broker_token("RELIANCE", "BSE", broker)
        symbol = get_broker_symbol("RELIANCE", "BSE", broker)
        broker_exchange = get_broker_exchange_code("RELIANCE", "BSE", broker)
        print(
            f"    {broker:12}: {symbol:20} (token: {token}, exchange: {broker_exchange})"
        )

    print("\n✅ Symbol mapping examples completed!")


if __name__ == "__main__":
    main()
