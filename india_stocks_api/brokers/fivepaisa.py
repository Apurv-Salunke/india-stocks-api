"""
5paisa Adapter
Thin wrapper around india_stocks_api.internal.fivepaisa
"""
from typing import Optional, Callable, List, Dict, Any
import json
import websocket
from .base import BaseBroker
from ..instruments.models import Equity, Future, Option, Index
from ..constants import TransactionType, OrderType, ProductType, OrderValidity, CandleInterval, StreamMode
from ..internal import context
import pyotp

# Import Ported Logic
from ..internal.fivepaisa.api.order_api import (
    place_order_api, 
    get_positions as get_positions_api,
    get_order_book as get_orders_api,
    get_trade_book as get_trades_api,
    cancel_order as cancel_order_api,
    modify_order as modify_order_api,
    get_holdings as get_holdings_api
)
from ..internal.fivepaisa.api.auth_api import authenticate_broker
from ..internal.fivepaisa.api.funds import get_margin_data
from ..internal.fivepaisa.api.data import BrokerData
from ..internal.context import get_credentials 

class FivePaisa(BaseBroker, broker_name="fivepaisa"):
    
    def __init__(self, api_key: str, clientcode: str, broker_pin: str, totp_code: str, api_secret: str, user_id: str):
        self.api_key = api_key
        self.clientcode = clientcode  # For 5paisa, this is the email ID
        self.broker_pin = broker_pin
        self.totp_code = totp_code
        self.api_secret = api_secret
        self.user_id = user_id
        # Note: Access Token and persisted credentials are handled by authenticate()
        # Note: Metaclass will call _ensure_instruments_ready() after this returns
    
    def _download_master_contract(self, db_path: str = 'instruments.db'):
        """Download and populate 5paisa master contract."""
        from ..internal.fivepaisa.database import master_contract_download
        master_contract_download(db_path)

    def authenticate(self) -> bool:
        """
        Login using 5paisa API.
        Updates the internal context with the session token.
        Also persists credentials to creds.json **only** after successful auth.
        """
        # Hack: The ported code reads BROKER_API_KEY from os.environ
        # We must set it here for the internal function to work.
        # Ideally, we would patch the internal code to use context.get_api_key()
        if not all([self.api_key, self.clientcode, self.broker_pin, self.totp_code, self.api_secret, self.user_id]):
            creds = context.get_credentials("fivepaisa")
            if not creds:
                raise RuntimeError("No credentials provided or stored.")

            self.api_key = self.api_key or creds.get("api_key")
            self.clientcode = self.clientcode or creds.get("clientcode")
            self.broker_pin = self.broker_pin or creds.get("broker_pin")
            self.totp_code = self.totp_code or creds.get("totp_code")
            self.api_secret = self.api_secret or creds.get("api_secret")
            self.user_id = self.user_id or creds.get("user_id")

        if not all([self.api_key, self.clientcode, self.broker_pin, self.totp_code, self.api_secret, self.user_id]):
            raise RuntimeError("Missing credentials after loading stored values.")
        
        try:
            # Generate TOTP Code
            totp_obj = pyotp.TOTP(self.totp_code)
            generated_totp = totp_obj.now()
            
            # Call Internal API
            access_token, error = authenticate_broker(
                api_key=self.api_key,
                clientcode=self.clientcode,
                broker_pin=self.broker_pin,
                totp_code=generated_totp,
                api_secret=self.api_secret,
                user_id=self.user_id
            )
            
            if access_token:
                context.set_auth_token(access_token)
                # 5paisa doesn't use feed token like Angel One
                context.set_credentials("fivepaisa", {
                    "api_key": self.api_key,
                    "clientcode": self.clientcode,
                    "broker_pin": self.broker_pin,
                    "totp_code": self.totp_code,
                    "api_secret": self.api_secret,
                    "user_id": self.user_id
                })
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
        
        # 2. Map to 5paisa Internal Format
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
        access_token = context.get_auth_token()
        res, response, orderid = place_order_api(data, access_token)
        
        return {
            "order_id": orderid,
            "raw_response": response,
            "status": "success" if orderid else "failed"
        }

    def get_positions(self) -> list:
        access_token = context.get_auth_token()
        resp = get_positions_api(access_token)
        return resp.get('body', {}).get('NetPositionDetail', [])

    def get_funds(self) -> dict:
        access_token = context.get_auth_token()
        return get_margin_data(access_token)

    def get_history(self, instrument: Equity | Future | Option | Index, 
                   start_date: str, end_date: str, interval: CandleInterval):
        """
        Get historical data.
        """
        # 1. Resolve
        token_info = self._resolve_instrument(instrument)
        
        # 2. Auth
        access_token = context.get_auth_token()
        if not access_token:
            raise RuntimeError("Auth required.")
            
        # 3. Fetch
        creds=get_credentials("fivepaisa")
        bd = BrokerData(auth_token=access_token, creds=creds)
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
        access_token = context.get_auth_token()
        if not access_token:
            raise RuntimeError("Auth required.")

        creds=get_credentials("fivepaisa")
        bd = BrokerData(auth_token=access_token, creds=creds)
        return bd.get_depth(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_quote(self, instrument: Equity | Future | Option | Index) -> dict:
        """
        Get real-time quote from 5paisa.
        """
        # 1. Resolve to Token/Symbol
        token_info = self._resolve_instrument(instrument)
        
        # 2. Get Auth Token
        access_token = context.get_auth_token()
        if not access_token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        # 3. Call Internal Logic
        creds=get_credentials("fivepaisa")
        bd = BrokerData(auth_token=access_token, creds=creds)
        return bd.get_quotes(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_holdings(self) -> list:
        access_token = context.get_auth_token()
        resp = get_holdings_api(access_token)
        return resp.get('body', {}).get('HoldingDetail', [])

    def get_orders(self) -> list:
        access_token = context.get_auth_token()
        resp = get_orders_api(access_token)
        return resp.get('body', {}).get('OrderBookDetail', [])
    
    def get_pending_orders(self) -> list:
        orders = self.get_orders()
        print(orders)
        return [o for o in orders if o.get("OrderStatus") in ["Pending", "Xmitted", "AH Placed", "Modified"]]

    def cancel_order(self, order_id: str) -> dict:
        access_token = context.get_auth_token()
        resp, status = cancel_order_api(order_id, access_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> dict:
        access_token = context.get_auth_token()
        
        # 1. Find Order to get Symbol/Exchange
        all_orders = self.get_orders()
        target_order = next((o for o in all_orders if o.get('OrderNo') == order_id), None)
        
        if not target_order:
            raise ValueError(f"Order {order_id} not found locally.")
            
        # 2. Construct Payload
        data = {
            "orderid": order_id,
            "symbol": target_order.get('ScripName'),
            "exchange": target_order.get('Exchange'),
            "transactiontype": target_order.get('BuySell'),
            "ordertype": target_order.get('OrderType'),
            "producttype": target_order.get('ProductType'),
            "quantity": str(quantity) if quantity > 0 else target_order.get('Qty'),
            "price": str(price) if price > 0 else target_order.get('Rate'),
            "triggerprice": str(trigger_price) if trigger_price > 0 else target_order.get('TriggerRate')
        }
        
        # 3. Call Modify
        resp, status = modify_order_api(data, access_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def get_trades(self) -> list:
        access_token = context.get_auth_token()
        resp = get_trades_api(access_token)
        return resp.get('body', {}).get('TradeBookDetail', [])

    def get_profile(self) -> dict:
        # 5paisa profile implementation would go here
        # TODO: Implement profile API for 5paisa
        return {}

    def get_order_details(self, order_id: str) -> dict:
        # Find order in order book
        all_orders = self.get_orders()
        target_order = next((o for o in all_orders if o.get('OrderNo') == order_id), None)
        return target_order or {}

    def cancel_all_orders(self) -> dict:
        all_orders = self.get_orders()
        pending_orders = [o for o in all_orders if o.get('OrderStatus') in ['Pending', 'Trigger Pending']]
        
        canceled = []
        failed = []
        
        for order in pending_orders:
            order_id = order.get('OrderNo')
            if order_id:
                result = self.cancel_order(order_id)
                if result.get('status') == 'success':
                    canceled.append(order_id)
                else:
                    failed.append(order_id)
        
        return {"status": "success", "canceled": canceled, "failed": failed}

    def square_off_all_positions(self) -> dict:
        # 5paisa position squaring off implementation
        # TODO: Implement square off all positions for 5paisa
        return {"status": "success", "message": "Square off all positions not yet implemented for 5paisa"}

    # --- GTT Orders ---
    # Note: 5paisa might not have GTT functionality like Angel One
    # These methods are placeholders that would need to be implemented
    # if 5paisa provides similar functionality

    def create_gtt(self, instrument: Equity | Future | Option, transaction_type: TransactionType, 
                  quantity: int, trigger_price: float, price: float, 
                  product_type: ProductType = ProductType.DELIVERY, time_period: int = 365) -> dict:
        # TODO: Implement GTT for 5paisa if available
        return {"status": "error", "message": "GTT not yet implemented for 5paisa"}

    def modify_gtt(self, id: int, instrument: Equity | Future | Option, 
                  quantity: int, trigger_price: float, price: float) -> dict:
        # TODO: Implement GTT modify for 5paisa if available
        return {"status": "error", "message": "GTT not yet implemented for 5paisa"}

    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> dict:
        # TODO: Implement GTT cancel for 5paisa if available
        return {"status": "error", "message": "GTT not yet implemented for 5paisa"}

    def get_gtt_list(self, status: list = ["FOR_SETTLEMENT", "CANCELLED", "TRIGGERED"]) -> list:
        # TODO: Implement GTT list for 5paisa if available
        return []

    def get_gtt_details(self, id: int) -> dict:
        # TODO: Implement GTT details for 5paisa if available
        return {}

    # 5paisa exchange mapping from docs
    _WS_EXCHANGE_MAP = {
        "NSE": ("N", "C"),
        "BSE": ("B", "C"),
        "NFO": ("N", "D"),
        "BFO": ("B", "D"),
        "MCX": ("M", "D"),
        "CDS": ("N", "U"),
    }

    def __init_streaming(self):
        """Lazy initialize streaming state."""
        if not hasattr(self, "_ws"):
            self._ws: Optional[websocket.WebSocketApp] = None
            self._pending_subscriptions: List[Dict] = []

            # user callbacks (same API as Angel)
            self.on_tick: Optional[Callable[[Dict[str, Any]], None]] = None
            self.on_error: Optional[Callable[[str, str], None]] = None
            self.on_close: Optional[Callable[[], None]] = None
            self.on_open: Optional[Callable[[], None]] = None

    def subscribe(
        self,
        instruments: List[Equity | Future | Option | Index],
        mode: StreamMode = StreamMode.QUOTE   # ignored (5paisa doesn't use modes)
    ) -> None:
        """
        Buffer subscriptions. Sent automatically after connection.
        """
        self.__init_streaming()

        items = []

        for inst in instruments:
            resolved = self._resolve_instrument(inst)

            exch, exch_type = self._WS_EXCHANGE_MAP.get(resolved["exchange"], ("N", "C"))

            items.append({
                "Exch": exch,
                "ExchType": exch_type,
                "ScripCode": int(resolved["token"])
            })

        self._pending_subscriptions.extend(items)

        # If already connected, send immediately
        if self._ws:
            self._send_subscription(items)

    def _send_subscription(self, items: List[Dict]):
        payload = {
            "Method": "MarketFeedV3",
            "Operation": "Subscribe",
            "ClientCode": self.clientcode,
            "MarketFeedData": items
        }

        self._ws.send(json.dumps(payload))


    def start_streaming(self) -> None:
        """
        Blocking streaming loop.
        Exactly same usage style as AngelOne but pure 5paisa protocol.
        """
        self.__init_streaming()

        access_token = context.get_auth_token()
        if not access_token:
            raise RuntimeError("Authenticate first")

        url = (
            f"wss://openfeed.5paisa.com/feeds/api/chat"
            f"?Value1={access_token}|{self.clientcode}"
        )

        # ---------- callbacks ----------

        def _on_open(ws):
            print("Connected")
            if self.on_open:
                self.on_open()

            if self._pending_subscriptions:
                self._send_subscription(self._pending_subscriptions)

        def _on_message(ws, message):
            try:
                print(message)
                data = json.loads(message)
            except Exception:
                return

            if not self.on_tick:
                return

            # 5paisa sends list of ticks
            if isinstance(data, list):
                for tick in data:
                    self.on_tick(tick)
            else:
                self.on_tick(data)


        def _on_error(ws, error):
            if self.on_error:
                self.on_error("socket", str(error))

        def _on_close(ws, *_):
            if self.on_close:
                self.on_close()

        # ---------- create socket ----------

        self._ws = websocket.WebSocketApp(
            url,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close
        )

        # blocking (same behavior as Angel)
        self._ws.run_forever()

    def stop_streaming(self) -> None:
        if self._ws:
            self._ws.close()
            self._ws = None