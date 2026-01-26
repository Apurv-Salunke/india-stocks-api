"""
5Paisa Adapter
Thin wrapper around india_stocks_api.internal.fivepaisa
"""
from typing import Optional, List, Dict, Any
from .base import BaseBroker
from ..instruments.models import Equity, Future, Option, Index
from ..constants import TransactionType, OrderType, ProductType, OrderValidity, CandleInterval
from ..internal import context

# Import Ported Logic
from ..internal.fivepaisa.api.order_api import (
    place_order_api, 
    get_positions, 
    get_holdings as get_holdings_api, 
    cancel_order as cancel_order_api,
    get_order_book as get_orders_api,
    get_trade_book as get_trades_api,
    cancel_all_orders_api,
    close_all_positions as close_all_positions_api,
    modify_order as modify_order_api
)
from ..internal.fivepaisa.api.auth_api import authenticate_broker
from ..internal.fivepaisa.api.funds import get_margin_data
from ..internal.fivepaisa.api.data import BrokerData
from ..internal.fivepaisa.mapping.order_data import (
    map_order_data,
    transform_order_data,
    map_trade_data,
    transform_tradebook_data,
    map_position_data,
    transform_positions_data,
    map_portfolio_data,
    transform_holdings_data
)
import os
import pyotp


class FivePaisa(BaseBroker, broker_name="fivepaisa"):
    """
    5Paisa broker adapter implementing the BaseBroker interface.
    
    Args:
        api_key: Format "app_user_id:::user_id:::client_code"
        api_secret: The encryption key from 5paisa
        email: Client email ID for authentication
        broker_pin: 5paisa PIN
        totp_key: TOTP secret for 2FA
    """
    
    def __init__(self, api_key: str, api_secret: str, email: str, broker_pin: str, totp_key: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.email = email
        self.broker_pin = broker_pin
        self.totp_key = totp_key
        
        # Shim Configuration
        context.set_api_key(api_key)
        # Note: Access Token is set after authenticate()
    
    def _download_master_contract(self, db_path: str = 'instruments.db'):
        """Download and populate 5Paisa master contract."""
        # TODO: Implement 5paisa master contract download
        # For now, we'll reuse the default mechanism or skip
        pass

    def authenticate(self) -> bool:
        """
        Login using 5Paisa TOTP API.
        Updates the internal context with the session token.
        """
        # Helper to extract parts from formatted API key
        if ':::' in self.api_key:
            app_key, user_id, client_code = self.api_key.split(':::')
        else:
            # Fallback or error - assume user provided individual parts differently 
            # (though __init__ suggests standard format)
            app_key, user_id, client_code = None, None, None
            
        context.set_broker_creds({
            "api_key": self.api_key, # Full string
            "api_secret": self.api_secret,
            "app_key": app_key,
            "user_id": user_id,
            "client_code": client_code,
            "broker_pin": self.broker_pin,
            "email": self.email
        })
        
        try:
            # Generate TOTP Code
            totp_obj = pyotp.TOTP(self.totp_key)
            generated_totp = totp_obj.now()
            
            # Call Internal API
            access_token, error = authenticate_broker(
                clientcode=self.email,
                broker_pin=self.broker_pin,
                totp_code=generated_totp
            )
            
            if access_token:
                context.set_auth_token(access_token)
                return True
            
            print(f"Auth failed: {error}")
            return False
        except Exception as e:
            print(f"Auth failed exception: {e}")
            return False

    def place_order(
        self,
        instrument: Equity | Future | Option,
        transaction_type: TransactionType,
        quantity: int,
        order_type: OrderType = OrderType.MARKET,
        product_type: ProductType = ProductType.INTRADAY,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: OrderValidity = OrderValidity.DAY,
        **kwargs
    ) -> dict:
        """Place an order with 5Paisa."""
        jwt_token = context.get_auth_token()
        resolved = self._resolve_instrument(instrument)
        
        # Map our product types to 5paisa format
        product_map = {
            ProductType.INTRADAY: "MIS",
            ProductType.DELIVERY: "CNC",
            ProductType.NORMAL: "NRML"
        }
        
        order_data = {
            "symbol": resolved["symbol"],
            "exchange": resolved["exchange"],
            "action": transaction_type.value,
            "quantity": str(quantity),
            "product": product_map.get(product_type, "MIS"),
            "price": str(price),
            "trigger_price": str(trigger_price),
        }
        
        response, data, orderid = place_order_api(order_data, jwt_token)
        return {"status": "success" if orderid else "error", "orderid": orderid, "data": data}

    def get_positions(self) -> list:
        """Get current open positions."""
        jwt_token = context.get_auth_token()
        resp = get_positions(jwt_token)
        # Map and transform the response
        mapped = map_position_data(resp)
        if mapped:
            return transform_positions_data(mapped)
        return []

    def get_funds(self) -> dict:
        """Get funds/margin info."""
        jwt_token = context.get_auth_token()
        return get_margin_data(jwt_token)

    def get_history(self, instrument: Equity | Future | Option | Index, 
                   start_date: str, end_date: str, interval: CandleInterval) -> Any:
        """Get historical candles."""
        jwt_token = context.get_auth_token()
        resolved = self._resolve_instrument(instrument)
        
        broker_data = BrokerData(jwt_token)
        
        interval_map = {
            CandleInterval.ONE_MINUTE: "1m",
            CandleInterval.FIVE_MINUTES: "5m",
            CandleInterval.FIFTEEN_MINUTES: "15m",
            CandleInterval.THIRTY_MINUTES: "30m",
            CandleInterval.ONE_HOUR: "1h",
            CandleInterval.ONE_DAY: "1d",
        }
        
        return broker_data.get_history(
            resolved["symbol"],
            resolved["exchange"],
            interval_map.get(interval, "1d"),
            start_date,
            end_date
        )

    def get_depth(self, instrument: Equity | Future | Option | Index) -> dict:
        """Get market depth (Level 2 data)."""
        jwt_token = context.get_auth_token()
        resolved = self._resolve_instrument(instrument)
        broker_data = BrokerData(jwt_token)
        return broker_data.get_depth(resolved["symbol"], resolved["exchange"])

    def get_quote(self, instrument: Equity | Future | Option | Index) -> dict:
        """Get current quote for an instrument."""
        jwt_token = context.get_auth_token()
        resolved = self._resolve_instrument(instrument)
        broker_data = BrokerData(jwt_token)
        return broker_data.get_quotes(resolved["symbol"], resolved["exchange"])

    def get_holdings(self) -> list:
        """Get long term holdings."""
        jwt_token = context.get_auth_token()
        resp = get_holdings_api(jwt_token)
        # Map and transform the response
        mapped = map_portfolio_data(resp)
        if mapped:
            return transform_holdings_data(mapped)
        return []

    def get_orders(self) -> list:
        """Get order book."""
        jwt_token = context.get_auth_token()
        resp = get_orders_api(jwt_token)
        # Map and transform the response
        mapped = map_order_data(resp)
        if mapped:
            return transform_order_data(mapped)
        return []

    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> dict:
        """Modify an open order."""
        jwt_token = context.get_auth_token()
        data = {
            "orderid": order_id,
            "price": str(price),
            "trigger_price": str(trigger_price),
            "quantity": str(quantity)
        }
        result, status_code = modify_order_api(data, jwt_token)
        return result

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        jwt_token = context.get_auth_token()
        result, status_code = cancel_order_api(order_id, jwt_token)
        return result

    def get_profile(self) -> dict:
        """Get user profile details."""
        # 5Paisa doesn't have a dedicated profile API
        # Return empty dict
        return {}

    def get_trades(self) -> list:
        """Get executed trades."""
        jwt_token = context.get_auth_token()
        resp = get_trades_api(jwt_token)
        # Map and transform the response
        mapped = map_trade_data(resp)
        if mapped:
            return transform_tradebook_data(mapped)
        return []

    def get_order_details(self, order_id: str) -> dict:
        """Get detailed status for a specific order."""
        # Get full order book and filter
        orders = self.get_orders()
        for order in orders:
            if str(order.get('orderid', '')) == str(order_id):
                return order
        return {}

    def cancel_all_orders(self) -> dict:
        """Cancel all open/pending orders."""
        jwt_token = context.get_auth_token()
        canceled, failed = cancel_all_orders_api({}, jwt_token)
        return {"canceled": canceled, "failed": failed}

    def square_off_all_positions(self) -> dict:
        """Exit all open positions at market price."""
        jwt_token = context.get_auth_token()
        result, status_code = close_all_positions_api(self.api_key, jwt_token)
        return result

    # --- GTT Orders (Not fully supported by 5Paisa) ---
    
    def create_gtt(self, instrument: Equity | Future | Option, transaction_type: TransactionType, 
                  quantity: int, trigger_price: float, price: float, 
                  product_type: ProductType = ProductType.DELIVERY, time_period: int = 365) -> dict:
        """Create a new GTT rule - Not supported by 5Paisa."""
        return {"status": "error", "message": "GTT orders not supported by 5Paisa"}

    def modify_gtt(self, id: int, instrument: Equity | Future | Option, 
                  quantity: int, trigger_price: float, price: float) -> dict:
        """Modify an existing GTT rule - Not supported by 5Paisa."""
        return {"status": "error", "message": "GTT orders not supported by 5Paisa"}

    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> dict:
        """Cancel an active GTT rule - Not supported by 5Paisa."""
        return {"status": "error", "message": "GTT orders not supported by 5Paisa"}

    def get_gtt_list(self, status: list = ["FOR_SETTLEMENT", "CANCELLED", "TRIGGERED"]) -> list:
        """Get list of GTT rules - Not supported by 5Paisa."""
        return []

    def get_gtt_details(self, id: int) -> dict:
        """Get details for a specific GTT rule - Not supported by 5Paisa."""
        return {"status": "error", "message": "GTT orders not supported by 5Paisa"}
