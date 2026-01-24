"""
Angel One Adapter
Thin wrapper around india_stocks_api.internal.angel
"""
from typing import Optional
from .base import BaseBroker
from ..instruments.models import Equity, Future, Option, Index
from ..constants import TransactionType, OrderType, ProductType, OrderValidity, CandleInterval
from ..internal import context

# Import Ported Logic
from ..internal.angel.api.order_api import (
    place_order_api, 
    get_positions, 
    get_holdings as get_holdings_api, 
    cancel_order as cancel_order_api,
    get_order_book as get_orders_api,
    get_trade_book as get_trades_api,
    get_profile as get_profile_api,
    get_order_details as get_order_details_api,
    cancel_all_orders_api,
    close_all_positions as close_all_positions_api,
    modify_order as modify_order_api
)
from ..internal.angel.api.gtt_api import (
    create_gtt_rule,
    modify_gtt_rule,
    cancel_gtt_rule,
    get_gtt_list as get_gtt_list_api,
    get_gtt_status as get_gtt_details_api
)
from ..internal.angel.api.auth_api import authenticate_broker
from ..internal.angel.api.funds import get_margin_data
from ..internal.angel.api.data import BrokerData
import os
import pyotp

class AngelOne(BaseBroker, broker_name="angel"):
    
    def __init__(self, api_key: str, client_code: str, password: str, totp_key: str):
        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.totp_key = totp_key
        
        # Shim Configuration
        context.set_api_key(api_key)
        # Note: Access Token is set after authenticate()

    def authenticate(self) -> bool:
        """
        Login using SmartAPI.
        Updates the internal context with the session token.
        """
        # Hack: The ported code reads BROKER_API_KEY from os.environ
        # We must set it here for the internal function to work.
        # Ideally, we would patch the internal code to use context.get_api_key()
        os.environ['BROKER_API_KEY'] = self.api_key
        
        try:
             # Generate TOTP Code
             totp_obj = pyotp.TOTP(self.totp_key)
             generated_totp = totp_obj.now()
             
             # Call Internal API
             jwt_token, feed_token, error = authenticate_broker(
                 clientcode=self.client_code,
                 broker_pin=self.password,
                 totp_code=generated_totp
             )
             
             if jwt_token:
                 context.set_auth_token(jwt_token)
                 context.set_feed_token(feed_token)
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
        
        # 1. Resolve Instrument
        token_info = self._resolve_instrument(instrument)
        
        # 2. Map to OpenAlgo Internal Format
        data = {
            "symbol": token_info["symbol"],
            "exchange": token_info["exchange"],
            "action": transaction_type.value, # BUY/SELL
            "quantity": str(quantity),
            "pricetype": order_type.value,
            "product": product_type.value,
            "price": str(price),
            "trigger_price": str(trigger_price),
            "validity": validity.value
        }
        
        # 3. Call Internal API
        jwt_token = context.get_auth_token()
        res, response, orderid = place_order_api(data, jwt_token)
        
        return {
            "order_id": orderid,
            "raw_response": response,
            "status": "success" if orderid else "failed"
        }

    def get_positions(self) -> list:
        jwt_token = context.get_auth_token()
        return get_positions(jwt_token)

    def get_funds(self) -> dict:
        jwt_token = context.get_auth_token()
        return get_margin_data(jwt_token)

    def get_history(self, instrument: Equity | Future | Option | Index, 
                   start_date: str, end_date: str, interval: CandleInterval):
        """
        Get historical data.
        """
        # 1. Resolve
        token_info = self._resolve_instrument(instrument)
        
        # 2. Auth
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Auth required.")
            
        # 3. Fetch
        bd = BrokerData(jwt_token)
        return bd.get_history(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
            interval=interval.value,
            start_date=start_date,
            end_date=end_date
        )

    def get_depth(self, instrument: Equity | Future | Option | Index) -> dict:
        """
        Get market depth.
        """
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        if not jwt_token: raise RuntimeError("Auth required.")
        
        bd = BrokerData(jwt_token)
        return bd.get_depth(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_quote(self, instrument: Equity | Future | Option | Index) -> dict:
        """
        Get real-time quote from Angel One.
        """
        # 1. Resolve to Token/Symbol
        token_info = self._resolve_instrument(instrument)
        
        # 2. Get Auth Token
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        # 3. Call Internal Logic
        bd = BrokerData(jwt_token)
        # Note: We pass the standardized symbol/exchange, 
        # and checking if internal/angel/api/data.py:get_quotes calls get_br_symbol correctly.
        # Yes, lines 92-93 of data.py call get_br_symbol/get_token again.
        return bd.get_quotes(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_holdings(self) -> list:
        jwt_token = context.get_auth_token()
        resp = get_holdings_api(jwt_token)
        return resp.get('data') or []

    def get_orders(self) -> list:
        jwt_token = context.get_auth_token()
        resp = get_orders_api(jwt_token)
        return resp.get('data') or []

    def cancel_order(self, order_id: str) -> dict:
        jwt_token = context.get_auth_token()
        resp, status = cancel_order_api(order_id, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> dict:
        jwt_token = context.get_auth_token()
        
        # 1. Find Order to get Symbol/Exchange
        all_orders = self.get_orders()
        target_order = next((o for o in all_orders if o.get('orderid') == order_id), None)
        
        if not target_order:
            raise ValueError(f"Order {order_id} not found locally.")
            
        # 2. Construct Payload
        # modify_order_api requires data dict with symbol, exchange, etc.
        data = {
            "orderid": order_id,
            "symbol": target_order.get('tradingsymbol'), # Verify naming
            "exchange": target_order.get('exchange'),
            "transactiontype": target_order.get('transactiontype'),
            "ordertype": target_order.get('ordertype'),
            "producttype": target_order.get('producttype'),
            "quantity": str(quantity) if quantity > 0 else target_order.get('quantity'),
            "price": str(price) if price > 0 else target_order.get('price'),
            "triggerprice": str(trigger_price) if trigger_price > 0 else target_order.get('triggerprice')
        }
        
        # Note: modify_order_api inside order_api.py tries to resolve token again.
        # It calls get_token(data['symbol'], data['exchange']).
        # If 'tradingsymbol' (e.g. RELIANCE-EQ) is passed as 'symbol', my Shim get_token should find it.
        # BUT 'data.symbol' must match what shim expects. 
        # Target Order from Angel likely has 'tradingsymbol': 'RELIANCE-EQ'.
        # Shim logic (Step 614) supports tradingsymbol lookup. So this should work.
        
        # 3. Call Modify
        resp, status = modify_order_api(data, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def get_trades(self) -> list:
        jwt_token = context.get_auth_token()
        resp = get_trades_api(jwt_token)
        return resp.get('data') or []

    def get_profile(self) -> dict:
        jwt_token = context.get_auth_token()
        resp = get_profile_api(jwt_token)
        return resp.get('data') or {}

    def get_order_details(self, order_id: str) -> dict:
        jwt_token = context.get_auth_token()
        resp = get_order_details_api(order_id, jwt_token)
        return resp.get('data') or {}

    def cancel_all_orders(self) -> dict:
        jwt_token = context.get_auth_token()
        # cancel_all_orders_api(data, auth)
        # Internal API expects data arg but doesn't seem to use it for basic cancellation?
        # Actually it calls get_order_book(auth) internally.
        canceled, failed = cancel_all_orders_api({}, jwt_token)
        return {"status": "success", "canceled": canceled, "failed": failed}

    def square_off_all_positions(self) -> dict:
        jwt_token = context.get_auth_token()
        # close_all_positions(current_api_key,auth)
        resp, status = close_all_positions_api(self.api_key, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    # --- GTT Orders ---

    def create_gtt(self, instrument: Equity | Future | Option, transaction_type: TransactionType, 
                  quantity: int, trigger_price: float, price: float, 
                  product_type: ProductType = ProductType.DELIVERY, time_period: int = 365) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        
        # Use existing internal mapping
        from ..internal.angel.mapping.transform_data import map_product_type
        
        payload = {
            "tradingsymbol": token_info["tradingsymbol"],
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"],
            "transactiontype": transaction_type.value.upper(), # BUY/SELL
            "producttype": map_product_type(product_type.value), # CNC -> DELIVERY
            "price": str(price),
            "qty": str(quantity),
            "triggerprice": str(trigger_price),
            "discloseqty": str(quantity),
            "timeperiod": str(time_period)
        }
        
        return create_gtt_rule(payload, jwt_token)

    def modify_gtt(self, id: int, instrument: Equity | Future | Option, 
                  quantity: int, trigger_price: float, price: float) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        
        payload = {
            "id": id,
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"],
            "price": price,
            "qty": quantity,
            "triggerprice": trigger_price
        }
        
        return modify_gtt_rule(payload, jwt_token)

    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        
        payload = {
            "id": id,
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"]
        }
        
        return cancel_gtt_rule(payload, jwt_token)

    def get_gtt_list(self, status: list = ["FOR_SETTLEMENT", "CANCELLED", "TRIGGERED"]) -> list:
        jwt_token = context.get_auth_token()
        payload = {"status": status, "page": 1, "count": 50}
        resp = get_gtt_list_api(payload, jwt_token)
        return resp.get('data') or []

    def get_gtt_details(self, id: int) -> dict:
        jwt_token = context.get_auth_token()
        resp = get_gtt_details_api(id, jwt_token)
        return resp.get('data') or {}
