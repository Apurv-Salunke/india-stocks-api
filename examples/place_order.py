"""
Order Placement Example

WARNING: THIS EXAMPLE PLACES REAL ORDERS WITH REAL MONEY.
Do NOT run this script unless you understand the financial implications.
Consider using a paper trading account or commenting out the actual order call.

Demonstrates:
    - Placing market and limit orders
    - Modifying orders
    - Canceling orders
    - Checking order status
    - Viewing positions and holdings

Expected output (DRY RUN):
    === DRY RUN MODE ===
    Would place order:
      Symbol: SBIN
      Action: BUY
      Quantity: 1
      Type: MARKET
      Product: INTRADAY

    To execute real orders, set DRY_RUN=False
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

# SAFETY FLAG - Set to False to actually place orders
DRY_RUN = True


def main():
    from india_stocks_api.brokers import AngelOne
    from india_stocks_api.constants import (
        OrderType,
        OrderValidity,
        ProductType,
        TransactionType,
    )
    from india_stocks_api.instruments import Equity

    # Setup credentials
    api_key = os.getenv("ANGEL_API_KEY")
    client_code = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp_key = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client_code, password, totp_key]):
        print("Error: Missing credentials. Check your .env file.")
        sys.exit(1)

    broker = AngelOne(
        api_key=api_key,
        client_code=client_code,
        password=password,
        totp_key=totp_key,
    )

    broker.authenticate()
    print("Authenticated successfully")
    print()

    # --- Order Parameters ---
    instrument = Equity("SBIN", exchange="NSE")
    transaction = TransactionType.BUY
    quantity = 1
    order_type = OrderType.MARKET
    product = ProductType.INTRADAY

    if DRY_RUN:
        print("=== DRY RUN MODE ===")
        print("Would place order:")
        print(f"  Symbol: {instrument.symbol}")
        print(f"  Action: {transaction.value}")
        print(f"  Quantity: {quantity}")
        print(f"  Type: {order_type.value}")
        print(f"  Product: {product.value}")
        print()
        print("To execute real orders, set DRY_RUN=False")
        print()

        # Show account info even in dry run
        print("=== Current Account State ===")
        show_account_state(broker)
        return

    # --- LIVE ORDER PLACEMENT ---
    print("=== PLACING LIVE ORDER ===")
    print("WARNING: This will use real money!")
    print()
    confirm = input("Type YES to place this live order: ").strip()
    if confirm != "YES":
        print("Live order aborted.")
        return

    # Place a market order
    response = broker.place_order(
        instrument=instrument,
        transaction_type=transaction,
        quantity=quantity,
        order_type=order_type,
        product_type=product,
        validity=OrderValidity.DAY,
    )

    print("Order Response:")
    print(f"  Order ID: {response.order_id}")
    print(f"  Status: {response.status}")
    print(f"  Message: {response.message}")
    print()

    if response.order_id and response.status == "success":
        # Check order status
        order_details = broker.get_order_details(response.order_id)
        print(f"Order Status: {order_details.status}")
        print(f"Average Price: {order_details.average_price}")
        print()

        # Example: Modify order (only works for pending limit orders)
        # response = broker.modify_order(
        #     order_id=response.order_id,
        #     price=750.0,
        #     quantity=2
        # )

        # Example: Cancel order (only works for pending orders)
        # response = broker.cancel_order(response.order_id)

    show_account_state(broker)


def show_account_state(broker):
    """Display current positions, orders, and funds."""

    # Funds
    funds = broker.get_funds()
    print("--- Funds ---")
    print(f"Available Cash: {funds.available_cash}")
    print(f"Collateral: {funds.collateral}")
    print()

    # Open Orders
    orders = broker.get_orders()
    print(f"--- Orders ({len(orders)} total) ---")
    for order in orders[:5]:
        print(f"  {order.order_id} | {order.symbol} | {order.transaction_type} | {order.status}")
    if len(orders) > 5:
        print(f"  ... and {len(orders) - 5} more")
    print()

    # Positions
    positions = broker.get_positions()
    print(f"--- Positions ({len(positions)} total) ---")
    for pos in positions:
        print(f"  {pos.symbol} | Qty: {pos.quantity} | P&L: {pos.pnl}")
    print()

    # Holdings (long-term)
    holdings = broker.get_holdings()
    print(f"--- Holdings ({len(holdings)} total) ---")
    for holding in holdings[:5]:
        print(f"  {holding.symbol} | Qty: {holding.quantity} | P&L: {holding.pnl:.2f}")
    if len(holdings) > 5:
        print(f"  ... and {len(holdings) - 5} more")


def example_limit_order(broker):
    """Example of placing a limit order with specific price."""
    from india_stocks_api.constants import OrderType, ProductType, TransactionType
    from india_stocks_api.instruments import Equity

    instrument = Equity("RELIANCE", exchange="NSE")

    # Get current price first
    quote = broker.get_quote(instrument)
    limit_price = quote.ltp - 5.0  # Place bid below current price

    response = broker.place_order(
        instrument=instrument,
        transaction_type=TransactionType.BUY,
        quantity=1,
        order_type=OrderType.LIMIT,
        product_type=ProductType.DELIVERY,
        price=limit_price,
    )

    return response


def example_stop_loss_order(broker):
    """Example of placing a stop-loss order."""
    from india_stocks_api.constants import OrderType, ProductType, TransactionType
    from india_stocks_api.instruments import Equity

    instrument = Equity("SBIN", exchange="NSE")

    quote = broker.get_quote(instrument)
    trigger = quote.ltp * 0.98  # Trigger 2% below current price
    limit_price = trigger - 1.0  # Limit price slightly below trigger

    response = broker.place_order(
        instrument=instrument,
        transaction_type=TransactionType.SELL,
        quantity=1,
        order_type=OrderType.SL,  # Stop-Loss Limit
        product_type=ProductType.INTRADAY,
        price=limit_price,
        trigger_price=trigger,
    )

    return response


if __name__ == "__main__":
    main()
