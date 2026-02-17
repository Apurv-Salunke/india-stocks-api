"""
Angel One Adapter
Thin wrapper around india_stocks_api.internal.angel
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

import pyotp

from ..constants import CandleInterval, OrderType, OrderValidity, ProductType, StreamMode, TransactionType
from ..exceptions import AuthenticationError
from ..instruments.models import Equity, Future, Index, Option
from ..internal import context
from ..internal.angel.api.auth_api import authenticate_broker
from ..internal.angel.api.data import BrokerData
from ..internal.angel.api.funds import get_margin_data
from ..internal.angel.api.gtt_api import cancel_gtt_rule, create_gtt_rule, modify_gtt_rule
from ..internal.angel.api.gtt_api import get_gtt_list as get_gtt_list_api
from ..internal.angel.api.gtt_api import get_gtt_status as get_gtt_details_api

# Import Ported Logic
from ..internal.angel.api.order_api import cancel_all_orders_api, get_positions, place_order_api
from ..internal.angel.api.order_api import cancel_order as cancel_order_api
from ..internal.angel.api.order_api import close_all_positions as close_all_positions_api
from ..internal.angel.api.order_api import get_holdings as get_holdings_api
from ..internal.angel.api.order_api import get_order_book as get_orders_api
from ..internal.angel.api.order_api import get_order_details as get_order_details_api
from ..internal.angel.api.order_api import get_profile as get_profile_api
from ..internal.angel.api.order_api import get_trade_book as get_trades_api
from ..internal.angel.api.order_api import modify_order as modify_order_api
from .base import BaseBroker

_IST = ZoneInfo("Asia/Kolkata")


class AngelOne(BaseBroker, broker_name="angel"):
    def __init__(self, api_key: str, client_code: str, password: str, totp_key: str):
        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.totp_key = totp_key

    def _download_master_contract(self):
        """Download and populate Angel One master contract."""
        from ..internal.angel.database import master_contract_download

        master_contract_download()

    def authenticate(self) -> bool:
        """
        Login using Angel SmartAPI.

        Generates a TOTP, authenticates via HTTP, and persists the session
        (tokens + expiry) to _cache/sessions.json. Sensitive secrets
        (password, totp_key) are never written to disk.

        Raises:
            AuthenticationError: If credentials are incomplete or login fails.

        Returns:
            True on successful authentication.
        """
        if not all([self.api_key, self.client_code, self.password, self.totp_key]):
            raise AuthenticationError(
                "Incomplete credentials. All of api_key, client_code, password, totp_key are required."
            )

        try:
            totp_code = pyotp.TOTP(self.totp_key).now()
        except Exception as exc:
            raise AuthenticationError(f"Invalid TOTP secret: {exc}") from exc

        jwt_token, feed_token, error = authenticate_broker(
            api_key=self.api_key, clientcode=self.client_code, broker_pin=self.password, totp_code=totp_code
        )

        if not jwt_token:
            raise AuthenticationError(f"Authentication failed: {error}")

        # Store tokens in-memory for the shim layer
        context.set_auth_token(jwt_token)
        context.set_feed_token(feed_token)

        # Compute session expiry: next midnight IST
        now_ist = datetime.now(_IST)
        expires_at = (now_ist + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        # Persist session — only tokens and non-secret identifiers
        context.save_session(
            "angel",
            {
                "access_token": jwt_token,
                "feed_token": feed_token,
                "api_key": self.api_key,
                "client_code": self.client_code,
                "authenticated_at": now_ist.isoformat(),
                "expires_at": expires_at.isoformat(),
            },
        )

        return True

    # --- Orders ---

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
        **kwargs,
    ) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        data = {
            "symbol": token_info["symbol"],
            "exchange": token_info["exchange"],
            "action": transaction_type.value,
            "quantity": str(quantity),
            "pricetype": order_type.value,
            "product": product_type.value,
            "price": str(price),
            "trigger_price": str(trigger_price),
            "validity": validity.value,
        }

        res, response, orderid = place_order_api(data, jwt_token)

        return {"order_id": orderid, "raw_response": response, "status": "success" if orderid else "failed"}

    def get_positions(self) -> list:
        jwt_token = self._require_auth()
        return get_positions(jwt_token)

    def get_funds(self) -> dict:
        jwt_token = self._require_auth()
        return get_margin_data(jwt_token)

    def get_history(
        self, instrument: Equity | Future | Option | Index, start_date: str, end_date: str, interval: CandleInterval
    ):
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        return bd.get_history(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
            interval=interval.value,
            start_date=start_date,
            end_date=end_date,
        )

    def get_depth(self, instrument: Equity | Future | Option | Index) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        return bd.get_depth(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_quote(self, instrument: Equity | Future | Option | Index) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        return bd.get_quotes(symbol=token_info["symbol"], exchange=token_info["exchange"])

    def get_holdings(self) -> list:
        jwt_token = self._require_auth()
        resp = get_holdings_api(jwt_token)
        return resp.get("data") or []

    def get_orders(self) -> list:
        jwt_token = self._require_auth()
        resp = get_orders_api(jwt_token)
        return resp.get("data") or []

    def cancel_order(self, order_id: str) -> dict:
        jwt_token = self._require_auth()
        resp, status = cancel_order_api(order_id, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> dict:
        jwt_token = self._require_auth()

        all_orders = self.get_orders()
        target_order = next((o for o in all_orders if o.get("orderid") == order_id), None)

        if not target_order:
            raise ValueError(f"Order {order_id} not found.")

        data = {
            "orderid": order_id,
            "symbol": target_order.get("tradingsymbol"),
            "exchange": target_order.get("exchange"),
            "transactiontype": target_order.get("transactiontype"),
            "ordertype": target_order.get("ordertype"),
            "producttype": target_order.get("producttype"),
            "quantity": str(quantity) if quantity > 0 else target_order.get("quantity"),
            "price": str(price) if price > 0 else target_order.get("price"),
            "triggerprice": str(trigger_price) if trigger_price > 0 else target_order.get("triggerprice"),
        }

        resp, status = modify_order_api(data, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    def get_trades(self) -> list:
        jwt_token = self._require_auth()
        resp = get_trades_api(jwt_token)
        return resp.get("data") or []

    def get_profile(self) -> dict:
        jwt_token = self._require_auth()
        resp = get_profile_api(jwt_token)
        return resp.get("data") or {}

    def get_order_details(self, order_id: str) -> dict:
        jwt_token = self._require_auth()
        resp = get_order_details_api(order_id, jwt_token)
        return resp.get("data") or {}

    def cancel_all_orders(self) -> dict:
        jwt_token = self._require_auth()
        canceled, failed = cancel_all_orders_api({}, jwt_token)
        return {"status": "success", "canceled": canceled, "failed": failed}

    def square_off_all_positions(self) -> dict:
        jwt_token = self._require_auth()
        resp, status = close_all_positions_api(self.api_key, jwt_token)
        return {"status": "success" if status == 200 else "error", "response": resp}

    # --- GTT Orders ---

    def create_gtt(
        self,
        instrument: Equity | Future | Option,
        transaction_type: TransactionType,
        quantity: int,
        trigger_price: float,
        price: float,
        product_type: ProductType = ProductType.DELIVERY,
        time_period: int = 365,
    ) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        from ..internal.angel.mapping.transform_data import map_product_type

        payload = {
            "tradingsymbol": token_info["tradingsymbol"],
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"],
            "transactiontype": transaction_type.value.upper(),
            "producttype": map_product_type(product_type.value),
            "price": str(price),
            "qty": str(quantity),
            "triggerprice": str(trigger_price),
            "discloseqty": str(quantity),
            "timeperiod": str(time_period),
        }

        return create_gtt_rule(payload, jwt_token)

    def modify_gtt(
        self, id: int, instrument: Equity | Future | Option, quantity: int, trigger_price: float, price: float
    ) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        payload = {
            "id": id,
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"],
            "price": price,
            "qty": quantity,
            "triggerprice": trigger_price,
        }

        return modify_gtt_rule(payload, jwt_token)

    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> dict:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        payload = {"id": id, "symboltoken": token_info["token"], "exchange": token_info["exchange"]}

        return cancel_gtt_rule(payload, jwt_token)

    def get_gtt_list(self, status: list | None = None) -> list:
        jwt_token = self._require_auth()
        if status is None:
            status = ["FOR_SETTLEMENT", "CANCELLED", "TRIGGERED"]
        payload = {"status": status, "page": 1, "count": 50}
        resp = get_gtt_list_api(payload, jwt_token)
        return resp.get("data") or []

    def get_gtt_details(self, id: int) -> dict:
        jwt_token = self._require_auth()
        resp = get_gtt_details_api(id, jwt_token)
        return resp.get("data") or {}

    # --- WebSocket Streaming ---

    _WS_EXCHANGE_MAP = {
        "NSE": 1,
        "NFO": 2,
        "BSE": 3,
        "BFO": 4,
        "MCX": 5,
        "NCX": 7,
        "CDS": 13,
    }

    def __init_streaming(self):
        """Lazy initialization of streaming components."""
        if not hasattr(self, "_ws_client"):
            self._ws_client = None
        if not hasattr(self, "_pending_subscriptions"):
            self._pending_subscriptions: List[tuple] = []
        if not hasattr(self, "on_tick"):
            self.on_tick: Optional[Callable[[Dict[str, Any]], None]] = None
        if not hasattr(self, "on_error"):
            self.on_error: Optional[Callable[[str, str], None]] = None
        if not hasattr(self, "on_close"):
            self.on_close: Optional[Callable[[], None]] = None
        if not hasattr(self, "on_open"):
            self.on_open: Optional[Callable[[], None]] = None

    def subscribe(
        self, instruments: List[Equity | Future | Option | Index], mode: StreamMode = StreamMode.QUOTE
    ) -> None:
        self.__init_streaming()

        token_list = []
        for inst in instruments:
            resolved = self._resolve_instrument(inst)
            if not resolved.get("token") or resolved.get("token") == "DUMMY":
                raise ValueError(
                    f"Unable to subscribe: instrument token not found for {inst.symbol} on {resolved.get('exchange')}."
                )
            token_list.append({"exchange": resolved["exchange"], "token": resolved["token"]})

        self._pending_subscriptions.append((token_list, mode.value))

        if self._ws_client and self._ws_client.wsapp:
            self._send_subscription(token_list, mode.value)

    def _send_subscription(self, token_list: List[Dict], mode: int) -> None:
        """Send subscription request to WebSocket."""
        exchange_tokens: Dict[int, List[str]] = {}
        for item in token_list:
            exch_type = self._WS_EXCHANGE_MAP.get(item["exchange"], 1)
            if exch_type not in exchange_tokens:
                exchange_tokens[exch_type] = []
            exchange_tokens[exch_type].append(str(item["token"]))

        ws_token_list = [{"exchangeType": exch, "tokens": tokens} for exch, tokens in exchange_tokens.items()]

        self._ws_client.subscribe(correlation_id=f"sub_{uuid4().hex[:12]}", mode=mode, token_list=ws_token_list)

    def unsubscribe(
        self, instruments: List[Equity | Future | Option | Index], mode: StreamMode = StreamMode.QUOTE
    ) -> None:
        self.__init_streaming()

        token_list = []
        for inst in instruments:
            resolved = self._resolve_instrument(inst)
            if not resolved.get("token") or resolved.get("token") == "DUMMY":
                exchange = resolved.get("exchange")
                raise ValueError(f"Unable to unsubscribe: instrument token not found for {inst.symbol} on {exchange}.")
            token_list.append({"exchange": resolved["exchange"], "token": resolved["token"]})

        self._pending_subscriptions = [
            (tokens, m) for (tokens, m) in self._pending_subscriptions if not (m == mode.value and tokens == token_list)
        ]

        if self._ws_client and self._ws_client.wsapp:
            exchange_tokens: Dict[int, List[str]] = {}
            for item in token_list:
                exch_type = self._WS_EXCHANGE_MAP.get(item["exchange"], 1)
                if exch_type not in exchange_tokens:
                    exchange_tokens[exch_type] = []
                exchange_tokens[exch_type].append(str(item["token"]))
            ws_token_list = [{"exchangeType": exch, "tokens": tokens} for exch, tokens in exchange_tokens.items()]
            self._ws_client.unsubscribe(
                correlation_id=f"unsub_{uuid4().hex[:12]}", mode=mode.value, token_list=ws_token_list
            )

    def start_streaming(self) -> None:
        """
        Connect to the WebSocket server and start streaming.

        This is a BLOCKING call. Make sure to:
        1. Call authenticate() first
        2. Set on_tick callback before calling this
        3. Call subscribe() before or after (subscriptions are buffered)
        """
        self.__init_streaming()

        jwt_token = self._require_auth()
        feed_token = context.get_feed_token()

        if not feed_token:
            raise AuthenticationError("No feed token available. Call authenticate() first.")

        from ..internal.angel.streaming import SmartWebSocketV2

        self._ws_client = SmartWebSocketV2(
            auth_token=jwt_token,
            api_key=self.api_key,
            client_code=self.client_code,
            feed_token=feed_token,
            max_retry_attempt=3,
            retry_delay=5,
        )

        def handle_open(wsapp):
            for token_list, mode in self._pending_subscriptions:
                self._send_subscription(token_list, mode)
            if self.on_open:
                self.on_open()

        def handle_data(wsapp, data):
            if self.on_tick:
                self.on_tick(data)

        def handle_error(error_type, error_msg):
            if self.on_error:
                self.on_error(error_type, error_msg)

        def handle_close(wsapp):
            if self.on_close:
                self.on_close()

        # SmartWebSocketV2 exposes these as overridable handler attributes.
        setattr(self._ws_client, "on_open", handle_open)
        setattr(self._ws_client, "on_data", handle_data)
        setattr(self._ws_client, "on_error", handle_error)
        setattr(self._ws_client, "on_close", handle_close)

        self._ws_client.connect()

    def stop_streaming(self) -> None:
        """Disconnect from the WebSocket server."""
        self.__init_streaming()
        if self._ws_client:
            self._ws_client.close_connection()
            self._ws_client = None
