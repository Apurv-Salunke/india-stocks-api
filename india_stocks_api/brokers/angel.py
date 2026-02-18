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
from ..internal.angel.api.order_api import cancel_order as cancel_order_api
from ..internal.angel.api.order_api import get_holdings as get_holdings_api
from ..internal.angel.api.order_api import get_order_book as get_orders_api
from ..internal.angel.api.order_api import get_order_details as get_order_details_api

# Import Ported Logic
from ..internal.angel.api.order_api import get_positions, place_order_api
from ..internal.angel.api.order_api import get_profile as get_profile_api
from ..internal.angel.api.order_api import get_trade_book as get_trades_api
from ..internal.angel.api.order_api import modify_order as modify_order_api
from ..responses import (
    Candle,
    DepthLevel,
    DepthResponse,
    FundsResponse,
    HistoryResponse,
    Holding,
    Order,
    OrderResponse,
    Position,
    ProfileResponse,
    QuoteResponse,
    StreamDepthLevel,
    Trade,
    WebSocketTick,
)
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
    ) -> OrderResponse:
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

        return self._map_order_response(orderid, response)

    def get_positions(self) -> list[Position]:
        jwt_token = self._require_auth()
        resp = get_positions(jwt_token)
        raw_list: list = []
        if isinstance(resp, list):
            raw_list = resp
        elif isinstance(resp, dict):
            data = resp.get("data")
            if isinstance(data, list):
                raw_list = data
        return [self._map_position(p) for p in raw_list]

    def get_funds(self) -> FundsResponse:
        jwt_token = self._require_auth()
        payload = get_margin_data(jwt_token) or {}
        return self._map_funds_response(payload)

    def get_history(
        self, instrument: Equity | Future | Option | Index, start_date: str, end_date: str, interval: CandleInterval
    ) -> HistoryResponse:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        df = bd.get_history(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
            interval=interval.value,
            start_date=start_date,
            end_date=end_date,
        )
        return self._map_history_response(
            symbol=token_info["symbol"], exchange=token_info["exchange"], interval=interval.value, data=df
        )

    def get_depth(self, instrument: Equity | Future | Option | Index) -> DepthResponse:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        payload = bd.get_depth(symbol=token_info["symbol"], exchange=token_info["exchange"])
        return self._map_depth_response(payload)

    def get_quote(self, instrument: Equity | Future | Option | Index) -> QuoteResponse:
        token_info = self._resolve_instrument(instrument)
        jwt_token = self._require_auth()

        bd = BrokerData(jwt_token)
        payload = bd.get_quotes(symbol=token_info["symbol"], exchange=token_info["exchange"])
        return self._map_quote_response(payload)

    def get_holdings(self) -> list[Holding]:
        jwt_token = self._require_auth()
        resp = get_holdings_api(jwt_token)
        if not isinstance(resp, dict):
            return []

        raw_list: list = []
        data = resp.get("data")
        if isinstance(data, list):
            raw_list = data
        elif isinstance(data, dict):
            holdings = data.get("holdings")
            if isinstance(holdings, list):
                raw_list = holdings
        return [self._map_holding(h) for h in raw_list]

    def get_orders(self) -> list[Order]:
        jwt_token = self._require_auth()
        resp = get_orders_api(jwt_token)
        raw_list = resp.get("data") or []
        return [self._map_order(o) for o in raw_list]

    def cancel_order(self, order_id: str) -> OrderResponse:
        jwt_token = self._require_auth()
        resp, status = cancel_order_api(order_id, jwt_token)
        st = "success" if status == 200 else "error"
        msg = resp.get("message", "") if isinstance(resp, dict) else str(resp)
        return OrderResponse(order_id=order_id, status=st, message=msg)

    def modify_order(
        self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0
    ) -> OrderResponse:
        jwt_token = self._require_auth()

        all_orders_raw = get_orders_api(jwt_token).get("data") or []
        target_order = next((o for o in all_orders_raw if o.get("orderid") == order_id), None)

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
        st = "success" if status == 200 else "error"
        msg = resp.get("message", "") if isinstance(resp, dict) else str(resp)
        return OrderResponse(order_id=order_id, status=st, message=msg)

    def get_trades(self) -> list[Trade]:
        jwt_token = self._require_auth()
        resp = get_trades_api(jwt_token)
        raw_list = resp.get("data") or []
        return [self._map_trade(t) for t in raw_list]

    def get_profile(self) -> ProfileResponse:
        jwt_token = self._require_auth()
        resp = get_profile_api(jwt_token)
        data = resp.get("data") if isinstance(resp, dict) else {}
        return self._map_profile_response(data if isinstance(data, dict) else {})

    def get_order_details(self, order_id: str) -> Order:
        jwt_token = self._require_auth()
        resp = get_order_details_api(order_id, jwt_token)
        data = resp.get("data") or {}
        return self._map_order(data)

    # TODO: re-enable when tested
    # def cancel_all_orders(self) -> dict:
    #     jwt_token = self._require_auth()
    #     canceled, failed = cancel_all_orders_api({}, jwt_token)
    #     return {"status": "success", "canceled": canceled, "failed": failed}
    #
    # def square_off_all_positions(self) -> dict:
    #     jwt_token = self._require_auth()
    #     resp, status = close_all_positions_api(self.api_key, jwt_token)
    #     return {"status": "success" if status == 200 else "error", "response": resp}

    # --- GTT Orders ---
    # TODO: re-enable when tested

    # def create_gtt(self, ...): ...
    # def modify_gtt(self, id, instrument, quantity, trigger_price, price): ...
    # def cancel_gtt(self, id, instrument): ...
    # def get_gtt_list(self, status=None): ...
    # def get_gtt_details(self, id): ...

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
    _WS_EXCHANGE_REVERSE_MAP = {v: k for k, v in _WS_EXCHANGE_MAP.items()}

    def __init_streaming(self):
        """Lazy initialization of streaming components."""
        if not hasattr(self, "_ws_client"):
            self._ws_client = None
        if not hasattr(self, "_pending_subscriptions"):
            self._pending_subscriptions: List[tuple] = []
        if not hasattr(self, "on_tick"):
            self.on_tick: Optional[Callable[[WebSocketTick], None]] = None
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
            token_list.append(
                {"symbol": inst.symbol, "exchange": resolved["exchange"], "token": str(resolved["token"])}
            )

        self._pending_subscriptions.append((token_list, mode.value))

        if self._ws_client and self._ws_client.wsapp:
            self._send_subscription(token_list, mode.value)

    def _send_subscription(self, token_list: List[Dict], mode: int) -> None:
        """Send subscription request to WebSocket."""
        ws_token_list = self._build_ws_token_list(token_list)

        self._ws_client.subscribe(correlation_id=f"sub_{uuid4().hex[:12]}", mode=mode, token_list=ws_token_list)

    def _build_ws_token_list(self, token_list: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Build websocket token payload grouped by exchange type."""
        exchange_tokens: Dict[int, List[str]] = {}
        for item in token_list:
            exch_type = self._WS_EXCHANGE_MAP.get(item["exchange"], 1)
            if exch_type not in exchange_tokens:
                exchange_tokens[exch_type] = []
            exchange_tokens[exch_type].append(str(item["token"]))

        return [{"exchangeType": exch, "tokens": tokens} for exch, tokens in exchange_tokens.items()]

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
            token_list.append(
                {"symbol": inst.symbol, "exchange": resolved["exchange"], "token": str(resolved["token"])}
            )

        self._pending_subscriptions = [
            (tokens, m) for (tokens, m) in self._pending_subscriptions if not (m == mode.value and tokens == token_list)
        ]

        if self._ws_client and self._ws_client.wsapp:
            ws_token_list = self._build_ws_token_list(token_list)
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
            # On reconnect, SmartWebSocketV2._on_open already calls resubscribe()
            # from its internal state; replaying pending subscriptions here would
            # duplicate subscription requests.
            if not getattr(self._ws_client, "RESUBSCRIBE_FLAG", False):
                for token_list, mode in self._pending_subscriptions:
                    self._send_subscription(token_list, mode)
            if self.on_open:
                self.on_open()

        def handle_data(wsapp, data):
            if self.on_tick:
                self.on_tick(self._map_stream_tick(data))

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

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    def _map_quote_response(self, payload: dict) -> QuoteResponse:
        return QuoteResponse(
            bid=self._to_float(payload.get("bid")),
            ask=self._to_float(payload.get("ask")),
            open=self._to_float(payload.get("open")),
            high=self._to_float(payload.get("high")),
            low=self._to_float(payload.get("low")),
            ltp=self._to_float(payload.get("ltp")),
            prev_close=self._to_float(payload.get("prev_close")),
            volume=self._to_int(payload.get("volume")),
            oi=self._to_int(payload.get("oi")),
        )

    def _map_depth_response(self, payload: dict) -> DepthResponse:
        bids = tuple(
            DepthLevel(price=self._to_float(level.get("price")), quantity=self._to_int(level.get("quantity")))
            for level in (payload.get("bids") or [])
        )
        asks = tuple(
            DepthLevel(price=self._to_float(level.get("price")), quantity=self._to_int(level.get("quantity")))
            for level in (payload.get("asks") or [])
        )

        return DepthResponse(
            bids=bids,
            asks=asks,
            high=self._to_float(payload.get("high")),
            low=self._to_float(payload.get("low")),
            ltp=self._to_float(payload.get("ltp")),
            ltq=self._to_int(payload.get("ltq")),
            open=self._to_float(payload.get("open")),
            prev_close=self._to_float(payload.get("prev_close")),
            volume=self._to_int(payload.get("volume")),
            oi=self._to_int(payload.get("oi")),
            total_buy_qty=self._to_int(payload.get("totalbuyqty")),
            total_sell_qty=self._to_int(payload.get("totalsellqty")),
        )

    def _map_history_response(self, symbol: str, exchange: str, interval: str, data: Any) -> HistoryResponse:
        candles: list[Candle] = []
        if hasattr(data, "iterrows"):
            for _, row in data.iterrows():
                candles.append(
                    Candle(
                        timestamp=self._to_int(row.get("timestamp")),
                        open=self._to_float(row.get("open")),
                        high=self._to_float(row.get("high")),
                        low=self._to_float(row.get("low")),
                        close=self._to_float(row.get("close")),
                        volume=self._to_int(row.get("volume")),
                        oi=self._to_int(row.get("oi")),
                    )
                )
        return HistoryResponse(symbol=symbol, exchange=exchange, interval=interval, candles=tuple(candles))

    def _map_funds_response(self, payload: dict) -> FundsResponse:
        return FundsResponse(
            available_cash=self._to_float(payload.get("availablecash")),
            collateral=self._to_float(payload.get("collateral")),
            m2m_realized=self._to_float(payload.get("m2mrealized")),
            m2m_unrealized=self._to_float(payload.get("m2munrealized")),
            utilized_debits=self._to_float(payload.get("utiliseddebits")),
        )

    def _map_order_response(self, order_id: str | None, raw: Any) -> OrderResponse:
        status = "success" if order_id else "failed"
        msg = ""
        if isinstance(raw, dict):
            msg = raw.get("message", "")
        return OrderResponse(order_id=order_id, status=status, message=msg)

    def _map_order(self, raw: dict) -> Order:
        return Order(
            order_id=str(raw.get("orderid", "")),
            symbol=str(raw.get("tradingsymbol", "")),
            exchange=str(raw.get("exchange", "")),
            transaction_type=str(raw.get("transactiontype", "")),
            order_type=str(raw.get("ordertype", "")),
            product_type=str(raw.get("producttype", "")),
            quantity=self._to_int(raw.get("quantity")),
            price=self._to_float(raw.get("price")),
            trigger_price=self._to_float(raw.get("triggerprice")),
            average_price=self._to_float(raw.get("averageprice")),
            status=str(raw.get("status", "")),
            timestamp=str(raw.get("updatetime") or raw.get("exchorderupdatetime") or ""),
            raw=raw,
        )

    def _map_position(self, raw: dict) -> Position:
        buy_qty = self._to_int(raw.get("buyqty"))
        sell_qty = self._to_int(raw.get("sellqty"))
        net_qty = buy_qty - sell_qty
        return Position(
            symbol=str(raw.get("tradingsymbol", "")),
            exchange=str(raw.get("exchange", "")),
            product_type=str(raw.get("producttype", "")),
            quantity=net_qty,
            average_price=self._to_float(raw.get("netprice") or raw.get("averageprice")),
            ltp=self._to_float(raw.get("ltp")),
            pnl=self._to_float(raw.get("pnl") or raw.get("realised")),
            raw=raw,
        )

    def _map_holding(self, raw: dict) -> Holding:
        avg = self._to_float(raw.get("averageprice"))
        ltp = self._to_float(raw.get("ltp"))
        qty = self._to_int(raw.get("quantity") or raw.get("t1quantity"))
        pnl = (ltp - avg) * qty if avg else 0.0
        pnl_pct = ((ltp - avg) / avg * 100.0) if avg else 0.0
        return Holding(
            symbol=str(raw.get("tradingsymbol", "")),
            exchange=str(raw.get("exchange", "")),
            quantity=qty,
            average_price=avg,
            ltp=ltp,
            pnl=round(pnl, 2),
            pnl_percent=round(pnl_pct, 2),
            raw=raw,
        )

    def _map_trade(self, raw: dict) -> Trade:
        qty = self._to_int(raw.get("fillquantity") or raw.get("quantity"))
        price = self._to_float(raw.get("fillprice") or raw.get("price"))
        return Trade(
            order_id=str(raw.get("orderid", "")),
            symbol=str(raw.get("tradingsymbol", "")),
            exchange=str(raw.get("exchange", "")),
            transaction_type=str(raw.get("transactiontype", "")),
            quantity=qty,
            price=price,
            trade_value=round(qty * price, 2),
            timestamp=str(raw.get("filltime") or raw.get("updatetime") or ""),
            raw=raw,
        )

    def _map_profile_response(self, payload: dict) -> ProfileResponse:
        exchanges = payload.get("exchanges") or ()
        products = payload.get("products") or ()
        if not isinstance(exchanges, (list, tuple)):
            exchanges = ()
        if not isinstance(products, (list, tuple)):
            products = ()

        return ProfileResponse(
            client_code=payload.get("clientcode") or payload.get("client_code"),
            name=payload.get("name"),
            exchanges=tuple(str(x) for x in exchanges),
            products=tuple(str(x) for x in products),
            email=payload.get("email"),
            mobile=payload.get("mobileno") or payload.get("mobile"),
            raw=payload,
        )

    def _resolve_stream_symbol_exchange(self, token: str, exchange_type: int) -> tuple[str, str]:
        for token_list, _mode in self._pending_subscriptions:
            for entry in token_list:
                if str(entry.get("token")) != str(token):
                    continue
                exchange = str(entry.get("exchange", "NSE"))
                if self._WS_EXCHANGE_MAP.get(exchange, 1) == exchange_type:
                    return str(entry.get("symbol", token)), exchange
        return token, self._WS_EXCHANGE_REVERSE_MAP.get(exchange_type, "NSE")

    def _map_stream_tick(self, payload: dict[str, Any]) -> WebSocketTick:
        token = str(payload.get("token", ""))
        exchange_type = self._to_int(payload.get("exchange_type"))
        symbol, exchange = self._resolve_stream_symbol_exchange(token, exchange_type)

        mode = payload.get("subscription_mode_val")
        if not mode:
            mode_map = {1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE", 4: "DEPTH"}
            mode = mode_map.get(self._to_int(payload.get("subscription_mode")), "UNKNOWN")

        bids_data = payload.get("best_5_buy_data") or payload.get("depth_20_buy_data") or ()
        asks_data = payload.get("best_5_sell_data") or payload.get("depth_20_sell_data") or ()

        bids = tuple(
            StreamDepthLevel(
                price=self._to_float(level.get("price")) / 100.0,
                quantity=self._to_int(level.get("quantity")),
                orders=self._to_int(level.get("no of orders", level.get("num_of_orders", 0))),
            )
            for level in bids_data
        )
        asks = tuple(
            StreamDepthLevel(
                price=self._to_float(level.get("price")) / 100.0,
                quantity=self._to_int(level.get("quantity")),
                orders=self._to_int(level.get("no of orders", level.get("num_of_orders", 0))),
            )
            for level in asks_data
        )

        bid = bids[0].price if bids else 0.0
        ask = asks[0].price if asks else 0.0

        return WebSocketTick(
            symbol=symbol,
            exchange=exchange,
            token=token,
            mode=str(mode),
            exchange_type=exchange_type,
            timestamp=self._to_int(payload.get("exchange_timestamp") or payload.get("packet_received_time")),
            ltp=self._to_float(payload.get("last_traded_price")) / 100.0,
            ltq=self._to_int(payload.get("last_traded_quantity")),
            open=self._to_float(payload.get("open_price_of_the_day")) / 100.0,
            high=self._to_float(payload.get("high_price_of_the_day")) / 100.0,
            low=self._to_float(payload.get("low_price_of_the_day")) / 100.0,
            close=self._to_float(payload.get("closed_price")) / 100.0,
            volume=self._to_int(payload.get("volume_trade_for_the_day")),
            oi=self._to_int(payload.get("open_interest")),
            bid=bid,
            ask=ask,
            total_buy_quantity=self._to_float(payload.get("total_buy_quantity")),
            total_sell_quantity=self._to_float(payload.get("total_sell_quantity")),
            bids=bids,
            asks=asks,
        )
