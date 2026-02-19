"""
Angel One Adapter
Thin wrapper around india_stocks_api.internal.angel
"""
from typing import Optional, Callable, List, Dict, Any

from .base import BaseBroker
from ..instruments.models import Equity, Future, Option, Index
from ..constants import TransactionType, OrderType, ProductType, OrderValidity, CandleInterval, StreamMode, ExecutionStatus, GTTRuleStatus
from ..internal import context
from .response import (
    DepthResponse,
    ErrorDetail,
    AuthResponse,
    OrderExecutionResponse, 
    OrdersResponse,
    OrderResponse,
    Order,
    GTTRule,
    GTTRuleResponse,
    GTTRulesResponse,
    GTTExecutionResponse,
    Profile,
    ProfileResponse,
    Funds,
    FundsResponse,
    Holding,
    HoldingsResponse,
    Position,
    PositionsResponse,
    Trade,
    TradesResponse,
    Candle,
    CandlesResponse,
    Quote,
    QuoteResponse,
    DepthLevel,
    MarketDepth,
    BatchExecutionResponse
)

# Import Ported Logic
from ..internal.angel.mapping.transform_data import (
    map_product_type,
    reverse_map_product_type, 
    map_order_status,
    map_gtt_status
)
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
import pyotp

class AngelOne(BaseBroker, broker_name="angel"):
    
    def __init__(self, api_key: str, client_code: str, password: str, totp_key: str):
        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.totp_key = totp_key
        
        # Note: Access Token is set after authenticate()
        # Note: Metaclass will call _ensure_instruments_ready() after this returns
    
    def _download_master_contract(self, db_path: str = 'instruments.db'):
        """Download and populate Angel One master contract."""
        from ..internal.angel.database import master_contract_download
        master_contract_download(db_path)

    def authenticate(self) -> AuthResponse:
        """
        Login using SmartAPI.
        Updates the internal context with the session token.
        """
        # Hack: The ported code reads BROKER_API_KEY from os.environ
        # We must set it here for the internal function to work.
        # Ideally, we would patch the internal code to use context.get_api_key()
        if not all([self.api_key, self.client_code, self.password, self.totp_key]):
            creds = context.get_credentials("angel")
            if not creds:
                raise RuntimeError("No credentials provided or stored.")

            # Only fill missing values so explicit args win
            self.api_key = self.api_key or creds.get("api_key")
            self.client_code = self.client_code or creds.get("client_code")
            self.password = self.password or creds.get("password")
            self.totp_key = self.totp_key or creds.get("totp_key")

        if not all([self.api_key, self.client_code, self.password, self.totp_key]):
            raise RuntimeError("Missing credentials after loading stored values.")
        
        try:
             # Generate TOTP Code
             totp_obj = pyotp.TOTP(self.totp_key)
             generated_totp = totp_obj.now()
             
             # Call Internal API
             jwt_token, feed_token, state, error_code, error_msg = authenticate_broker(
                api_key=self.api_key,
                clientcode=self.client_code,
                broker_pin=self.password,
                totp_code=generated_totp
             )
             
             if jwt_token and feed_token:
                context.set_auth_token(jwt_token)
                context.set_feed_token(feed_token)
                context.set_credentials("angel", {
                    "api_key": self.api_key,
                    "client_code": self.client_code,
                    "password": self.password,
                    "totp_key": self.totp_key
                })
                return AuthResponse(
                success=True,
                broker="angel",
                session_active=True,
                environment=state
            )
             
             
             return AuthResponse(
            success=False,
            broker="angel",
            error=ErrorDetail(
                code=error_code or "AUTH_FAILED",
                message=error_msg or "Authentication failed"
            )
        )
        except Exception as e:
            return AuthResponse( # Create custom exception class for this
            success=False,
            broker="angel",
            error=ErrorDetail(
                code="AUTH_EXCEPTION",
                message=str(e)
            )
        )

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
    ) -> OrderExecutionResponse:
        
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
        try: 
            # 3. Call Internal API
            jwt_token = context.get_auth_token()
            res, response, orderid, uniqueorderid = place_order_api(data, jwt_token)
            
            if orderid:
                return OrderExecutionResponse(
                success=True,
                broker="angel",
                execution_id=orderid,
                broker_reference_id=uniqueorderid,
                execution_status=ExecutionStatus.ACCEPTED,
            )
        
            # BROKER REJECTED
            return OrderExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.REJECTED,
                error=ErrorDetail(
                    code=response.get("errorcode", "ORDER_REJECTED"),
                    message=response.get("message", "Order rejected")
                )
            )

        except Exception as e:
            return OrderExecutionResponse(  #TODO: Create custom exception class for this
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                error=ErrorDetail(
                    code="INTERNAL_ERROR",
                    message=str(e)
                )
            )


    def get_positions(self) -> PositionsResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")

        resp = get_positions(jwt_token)
        
        if not resp.get("status"):
            return PositionsResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code=resp.get("errorcode", "POSITIONS_FETCH_FAILED"),
                    message=resp.get("message", "Failed to fetch positions"),
                )
            )

        positions = []

        for p in resp.get("data", []):
            qty = int(p.get("netqty", 0))
            if qty == 0:
                continue

            positions.append(Position(
                symbol=p["tradingsymbol"], #TODO: send standardized symbol after coorect the function in context fiel     
                exchange=p["exchange"],
                quantity=qty,
                average_price=float(p.get("avgnetprice") or 0),
                last_price=None, # TODO: check documentation
                pnl=None,
                unrealized_pnl=None,
                realized_pnl=None,
                product_type=reverse_map_product_type(p["producttype"]),
                multiplier=abs(int(p.get("multiplier", 1))) or 1
            ))

        return PositionsResponse(
            success=True,
            broker="angel",
            data=positions
        )

    def get_funds(self) -> FundsResponse:
        jwt_token = context.get_auth_token()
        
        if not jwt_token:
            raise RuntimeError("Authentication required.")

        resp = get_margin_data(jwt_token)

        if not resp:
            return FundsResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="FUNDS_FETCH_FAILED",
                    message="Unable to retrieve funds"
                ),
            )

        funds = Funds(
            available=resp.get("availablecash"),
            used=resp.get("utiliseddebits"),
            collateral=resp.get("collateral"),
            realized_pnl=resp.get("m2mrealized"),
            unrealized_pnl=resp.get("m2munrealized"),
        )

        return FundsResponse(
            success=True,
            broker="angel",
            data=funds
        )

    def get_history(self, instrument: Equity | Future | Option | Index, 
                   start_date: str, end_date: str, interval: CandleInterval) -> CandlesResponse:
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
        resp= bd.get_history(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
            interval=interval.value,
            start_date=start_date,
            end_date=end_date
        )

        if not resp:
            return CandlesResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="HISTORY_FETCH_FAILED",
                    message="No historical data returned"
                ),
            )

        candles = []
        for row in resp:
                try:
                    candle = Candle(
                        timestamp=row[0],
                        open=float(row[1] or 0),
                        high=float(row[2] or 0),
                        low=float(row[3] or 0),
                        close=float(row[4] or 0),
                        volume=int(row[5] or 0),
                        oi=int(row[6]) if len(row) > 6 and row[6] else None,
                    )
                    candles.append(candle)

                except Exception:
                    # skip malformed rows
                    continue

        return CandlesResponse(
            success=True,
            broker="angel",
            data=candles,
        )

    def get_depth(self, instrument: Equity | Future | Option | Index) -> DepthResponse:
        """
        Get market depth.
        """
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Auth required.")

        # Fetch depth
        bd = BrokerData(jwt_token)
        resp = bd.get_depth(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
        )

        if not resp:
            return DepthResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="DEPTH_FETCH_FAILED",
                    message="No depth data received",
                ),
            )

        # Angel sometimes returns list instead of dict
        q = resp[0] if isinstance(resp, list) and resp else resp

        depth = q.get("depth", {}) or {}
        buy_levels = depth.get("buy", [])
        sell_levels = depth.get("sell", [])

        def build_levels(levels):
            result = []
            for i in range(5):  # enforce 5 levels
                if i < len(levels):
                    lvl = levels[i] or {}
                    try:
                        result.append(
                            DepthLevel(
                                price=float(lvl.get("price") or 0),
                                quantity=int(lvl.get("quantity") or 0),
                                orders=int(lvl.get("orders")) if lvl.get("orders") else None,
                            )
                        )
                    except Exception:
                        result.append(DepthLevel(price=0.0, quantity=0, orders=None))
                else:
                    result.append(DepthLevel(price=0.0, quantity=0, orders=None))
            return result

        market_depth = MarketDepth(
            symbol=token_info["symbol"],
            exchange=token_info["exchange"],
            bids=build_levels(buy_levels),
            asks=build_levels(sell_levels),
            last_price=float(q.get("ltp") or 0),
            last_quantity=int(q.get("lastTradeQty")) if q.get("lastTradeQty") else None,
            total_bid_qty=int(q.get("totBuyQuan")) if q.get("totBuyQuan") else None,
            total_ask_qty=int(q.get("totSellQuan")) if q.get("totSellQuan") else None,
            volume=int(q.get("tradeVolume")) if q.get("tradeVolume") else None,
            oi=int(q.get("opnInterest")) if q.get("opnInterest") else None,
        )

        return DepthResponse(
            success=True,
            broker="angel",
            data=market_depth,
        )
     
    def get_quote(self, instrument: Equity | Future | Option | Index) -> QuoteResponse:
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
        resp= bd.get_quotes(symbol=token_info["symbol"], exchange=token_info["exchange"])
        if not resp:
            return QuoteResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="QUOTE_FETCH_FAILED",
                    message="No quote data received",
                ),
            )
        q = resp[0] if isinstance(resp, list) and resp else resp

        try:
            depth = q.get("depth", {})
            bids = depth.get("buy", [])
            asks = depth.get("sell", [])

            ltp_val = float(q.get("ltp") or 0)

            quote = Quote(
                symbol=token_info["symbol"],
                exchange=token_info["exchange"],
                last_price=ltp_val,
                open=float(q.get("open")) if q.get("open") else None,
                high=float(q.get("high")) if q.get("high") else None,
                low=float(q.get("low")) if q.get("low") else None,
                close=float(q.get("close")) if q.get("close") else None,
                volume=int(q.get("tradeVolume")) if q.get("tradeVolume") else None,
                oi=int(q.get("opnInterest")) if q.get("opnInterest") else None,
                bid=float(bids[0]["price"]) if bids else None,
                ask=float(asks[0]["price"]) if asks else None,
            )

        except Exception:
            return QuoteResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="QUOTE_PARSE_FAILED",
                    message="Malformed quote received from broker",
                ),
            )

        return QuoteResponse(
            success=True,
            broker="angel",
            data=quote,
        )

    def get_holdings(self) -> list:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")

        resp = get_holdings_api(jwt_token)

        if not resp.get("status"):
            return HoldingsResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code=resp.get("errorcode", "HOLDINGS_FETCH_FAILED"),
                    message=resp.get("message", "Failed to fetch holdings")
                )
            )

        holdings = []

        for h in resp.get("data", {}).get("holdings", []):
            holdings.append(
                Holding(
                    symbol=h["tradingsymbol"],
                    exchange=h["exchange"],
                    quantity=int(h.get("quantity", 0)),
                    average_price=float(h.get("averageprice", 0)),
                    current_price=float(h.get("ltp", 0)),
                    pnl=float(h.get("profitandloss", 0)),
                    pnl_percent=float(h.get("pnlpercentage", 0)),
                    product_type=reverse_map_product_type(h["product"]),
                )
            )

        return HoldingsResponse(
            success=True,
            broker="angel",
            data=holdings,
        )

    def get_orders(self) -> OrdersResponse:
        """
        Get all orders.
        """
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        
        try:
            resp = get_orders_api(jwt_token)
            if not resp.get("status"):
                return OrdersResponse(
                    success=False,
                    broker="angel",
                    error=ErrorDetail(
                        code=resp.get("errorcode", "ORDER_FETCH_FAILED"),
                        message=resp.get("message", "Failed to fetch orders")
                    )
                )
            orders: List[Order] = []

            for o in resp.get("data", []):
                filled = int(o.get("filledshares", 0))
                qty = int(o.get("quantity", 0))

                orders.append(
                    Order(
                        order_id=str(o["orderid"]),
                        symbol=o["tradingsymbol"],
                        exchange=o["exchange"],
                        transaction_type=TransactionType(o["transactiontype"]),
                        order_type=OrderType(o["ordertype"]),
                        product_type=reverse_map_product_type(o["producttype"]),
                        quantity=qty,
                        filled_quantity=filled,
                        price=float(o.get("price", 0)),
                        average_price=float(o.get("averageprice", 0)),
                        status=map_order_status(o.get("orderstatus")),
                        timestamp=o.get("updatetime")
                    )
                )

            return OrdersResponse(
                success=True,
                broker="angel",
                data=orders
            )

        except Exception as e:
            return OrdersResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="ORDER_FETCH_FAILED",
                    message=str(e)
                )
            )

    def cancel_order(self, order_id: str) -> OrderExecutionResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Auth failed")
        
        try:
            resp, status = cancel_order_api(order_id, jwt_token)
            if resp.get("status") == "success":
                data = resp.get("data", {})

                return OrderExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_id=data.get("orderid", order_id),
                    broker_reference_id=data.get("uniqueorderid", order_id),
                    execution_status=ExecutionStatus.CANCELLED,
                    raw=resp,
                )

            # BROKER REJECTED
            return OrderExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.REJECTED,
                error=ErrorDetail(
                    code=resp.get("errorcode", "CANCEL_REJECTED"),
                    message=resp.get("message", "Cancel rejected"),
                )
            )

        except Exception as e:
            return OrderExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                error=ErrorDetail(
                    code="INTERNAL_ERROR",
                    message=str(e),
                ),
            )

    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> OrderExecutionResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Auth failed")
        
        try:
            # 1. Find Order to get Symbol/Exchange
            all_orders = self.get_orders()
            if not all_orders.success:
                return OrderExecutionResponse(
                    success=False,
                    broker="angel",
                    execution_status=ExecutionStatus.FAILED,
                    error=all_orders.error
                )

            target_order = next((o for o in all_orders.data if o.order_id == order_id), None)

            if not target_order:
                return OrderExecutionResponse(
                    success=False,
                    broker="angel",
                    execution_status=ExecutionStatus.FAILED,
                    error=ErrorDetail(
                        code="ORDER_NOT_FOUND",
                        message=f"Order {order_id} not found."
                    )
                )

                
            # 2. Construct Payload
            # modify_order_api requires data dict with symbol, exchange, etc.
            data = {
                "orderid": order_id,
                "symbol": target_order.symbol, # Verify naming
                "exchange": target_order.exchange,
                "ordertype": target_order.order_type.value,
                "producttype": map_product_type(target_order.product_type.value),
                "quantity": str(quantity) if quantity > 0 else str(target_order.quantity),
                "price": str(price) if price > 0 else str(target_order.price)
            }
            
            # Note: modify_order_api inside order_api.py tries to resolve token again.
            # It calls get_token(data['symbol'], data['exchange']).
            # If 'tradingsymbol' (e.g. RELIANCE-EQ) is passed as 'symbol', my Shim get_token should find it.
            # BUT 'data.symbol' must match what shim expects. 
            # Target Order from Angel likely has 'tradingsymbol': 'RELIANCE-EQ'.
            # Shim logic (Step 614) supports tradingsymbol lookup. So this should work.
            
            # 3. Call Modify
            resp, status = modify_order_api(data, jwt_token)
            if resp.get("status") == "success":
                return OrderExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_id=resp.get("orderid"),
                    broker_reference_id=resp.get("uniqueorderid"),
                    execution_status=ExecutionStatus.MODIFIED,
                )

            return OrderExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.REJECTED,
                error=ErrorDetail(
                    code=resp.get("errorcode", "MODIFY_REJECTED"),
                    message=resp.get("message", "Modify rejected")
                )
            )

        except Exception as e:
            return OrderExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                error=ErrorDetail(
                    code="INTERNAL_ERROR",
                    message=str(e)
                )
            )

    def get_trades(self):
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")

        resp = get_trades_api(jwt_token)

        # Validate response
        if not resp or not resp.get("status"):
            return TradesResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code=resp.get("errorcode", "TRADES_FETCH_FAILED") if resp else "TRADES_FETCH_FAILED",
                    message=resp.get("message", "Failed to fetch trades") if resp else "No response from broker",
                ),
            )

        trades = []

        for t in resp.get("data", []):
            try:
                trade = Trade(
                    trade_id=t.get("fillid"),
                    order_id=t.get("orderid"),
                    symbol=t.get("tradingsymbol"),
                    exchange=t.get("exchange"),
                    side=TransactionType(t.get("transactiontype")),
                    quantity=int(t.get("fillsize", 0) or 0),
                    price=float(t.get("fillprice", 0) or 0),
                    product_type=reverse_map_product_type(t.get("producttype")),
                    timestamp=None,  # Angel does not provide full timestamp
                )
                trades.append(trade)

            except Exception as e:
                # skip malformed trades but log if logger available
                # logger.warning(f"Skipping malformed trade: {t} error={e}")
                continue

        return TradesResponse(
            success=True,
            broker="angel",
            data=trades,
        )

    def get_profile(self) -> ProfileResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")

        raw = get_profile_api(jwt_token)

        if not raw.get("status"):
            return ProfileResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code=raw.get("errorcode", "PROFILE_FETCH_FAILED"),
                    message=raw.get("message", "Failed to fetch profile")
                ),
                raw=raw
            )

        d = raw["data"]

        profile = Profile(
            user_id=d["clientcode"],
            name=d["name"],
            broker="angel",
            email=d.get("email") or None,
            phone=d.get("mobileno") or None,
            last_login=d.get("lastlogintime"),
        )

        return ProfileResponse(
            success=True,
            broker="angel",
            data=profile,
            raw=raw
        )

    def get_order_details(self, unique_order_id: str) -> OrderResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")
        
        try:
            resp = get_order_details_api(unique_order_id, jwt_token)

            if not resp.get("status"):
                return OrderResponse(
                    success=False,
                    broker="angel",
                    error=ErrorDetail(
                        code=resp.get("errorcode", "API_ERROR"),
                        message=resp.get("message", "Failed to fetch order details")
                    )
                )

            data = resp.get("data", {})

            order = Order(
                order_id=data.get("orderid"),
                symbol=data.get("tradingsymbol"),
                exchange=data.get("exchange"),
                transaction_type=TransactionType(data.get("transactiontype")),
                order_type=OrderType(data.get("ordertype")),
                product_type=reverse_map_product_type(data.get("producttype")),
                quantity=int(data.get("quantity", 0)),
                filled_quantity=int(data.get("filledshares", 0)),
                price=float(data.get("price", 0)),
                average_price=float(data.get("averageprice", 0) or 0),
                status=data.get("status"),
                timestamp=data.get("updatetime")
            )

            return OrderResponse(
                success=True,
                broker="angel",
                data=order
            )

        except Exception as e:
            return OrderResponse(
                success=False,
                broker="angel",
                error=ErrorDetail(
                    code="INTERNAL_ERROR",
                    message=str(e)
                )
            )

    def cancel_all_orders(self) -> BatchExecutionResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Auth failed")
        
        # cancel_all_orders_api(data, auth)
        # Internal API expects data arg but doesn't seem to use it for basic cancellation?
        # Actually it calls get_order_book(auth) internally.
        try:
            canceled, failed = cancel_all_orders_api({}, jwt_token)

            total = len(canceled) + len(failed)

            # NOTHING TO CANCEL
            if total == 0:
                return BatchExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_status=ExecutionStatus.NO_ACTION,
                    data=[],
                )

            # ALL SUCCESS
            if len(failed) == 0:
                return BatchExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_status=ExecutionStatus.COMPLETED,
                    data=canceled,
                )

            # PARTIAL SUCCESS
            if len(canceled) > 0:
                return BatchExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_status=ExecutionStatus.PARTIAL,
                    data={
                        "cancelled": canceled,
                        "failed": failed,
                    },
                )

            # TOTAL FAILURE
            return BatchExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                data={"failed": failed},
                error=ErrorDetail(
                    code="CANCEL_FAILED",
                    message="Failed to cancel open orders.",
                ),
            )

        except Exception as e:
            return BatchExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                error=ErrorDetail(
                    code="INTERNAL_ERROR",
                    message=str(e),
                ),
            )

    def square_off_all_positions(self) -> BatchExecutionResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication required.")
        # close_all_positions(current_api_key,auth)

        try:
            results = close_all_positions_api(self.api_key, jwt_token)

            if not results:
                return BatchExecutionResponse(
                    success=True,
                    broker="angel",
                    execution_status=ExecutionStatus.NO_ACTION,
                    data=[]
                )

            failed = [r for r in results if r.status != "CLOSED"]

            return BatchExecutionResponse(
                success=len(failed) == 0,
                broker="angel",
                execution_status=ExecutionStatus.PARTIAL if failed else ExecutionStatus.COMPLETED,
                data=results
            )

        except Exception as e:
            return BatchExecutionResponse(
                success=False,
                broker="angel",
                execution_status=ExecutionStatus.FAILED,
                data=[],
                error=ErrorDetail(
                    code="SQUARE_OFF_FAILED",
                    message=str(e)
                )
            )

    # --- GTT Orders ---

    def create_gtt(self, instrument: Equity | Future | Option, transaction_type: TransactionType, 
                  quantity: int, trigger_price: float, price: float, 
                  product_type: ProductType = ProductType.DELIVERY, time_period: int = 365) -> GTTExecutionResponse:
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
        
        resp = create_gtt_rule(payload, jwt_token)
        if resp.get("status"):
            rule_id = resp["data"]["id"]

            return GTTExecutionResponse(
                success=True,
                broker="angel",
                execution_id=rule_id,
                execution_status=GTTRuleStatus.ACTIVE
            )

        return GTTExecutionResponse(
            success=False,
            broker="angel",
            execution_status=GTTRuleStatus.REJECTED,
            error=ErrorDetail(
                code=resp.get("errorcode", "GTT_CREATE_FAILED"),
                message=resp.get("message")
            )
        )

    def modify_gtt(self, id: int, instrument: Equity | Future | Option, 
                  quantity: int, trigger_price: float, price: float) -> GTTExecutionResponse:
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
        
        resp = modify_gtt_rule(payload, jwt_token)
        if resp.get("status"):
            rule_id = resp["data"]["id"]

            return GTTExecutionResponse(
                success=True,
                broker="angel",
                execution_id=rule_id,
                execution_status=GTTRuleStatus.ACTIVE
            )

        return GTTExecutionResponse(
            success=False,
            broker="angel",
            execution_status=GTTRuleStatus.REJECTED,
            error=ErrorDetail(
                code=resp.get("errorcode", "GTT_CREATE_FAILED"),
                message=resp.get("message")
            )
        )

    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> GTTExecutionResponse:
        token_info = self._resolve_instrument(instrument)
        jwt_token = context.get_auth_token()
        
        payload = {
            "id": id,
            "symboltoken": token_info["token"],
            "exchange": token_info["exchange"]
        }
        
        resp = cancel_gtt_rule(payload, jwt_token)
        if resp.get("status"):
            rule_id = resp["data"]["id"]

            return GTTExecutionResponse(
                success=True,
                broker="angel",
                execution_id=rule_id,
                execution_status=GTTRuleStatus.CANCELLED
            )

        return GTTExecutionResponse(
            success=False,
            broker="angel",
            execution_status=GTTRuleStatus.REJECTED,
            error=ErrorDetail(
                code=resp.get("errorcode", "GTT_CANCEL_FAILED"),
                message=resp.get("message")
            )
        )

    def get_gtt_list(self, status: str) -> GTTRuleResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication token is required")
        
        try: 
            payload = {"status": status, "page": 1, "count": 50}
            resp = get_gtt_list_api(payload, jwt_token)
            if not resp.get("status"):
                return GTTRuleResponse(success=False, broker="angel", error=ErrorDetail(code=resp.get("errorcode", "GTT_LIST_FAILED"), message=resp.get("message")))

            rules= []
            for d in resp.get("data", []):
                rules.append(GTTRule(
                    rule_id=str(d["id"]),
                    symbol=d["tradingsymbol"],
                    exchange=d["exchange"],
                    transaction_type=TransactionType(d["transactiontype"]),
                    product_type=reverse_map_product_type(d["producttype"]),
                    quantity=int(d["qty"]),
                    price=float(d["price"]),
                    trigger_price=float(d["triggerprice"]),
                    status=map_gtt_status(d["status"]),
                    created_at=d.get("createddate"),
                    updated_at=d.get("updateddate"),
                    expires_at=d.get("expirydate"),
                ))

            return GTTRulesResponse(success=True, broker="angel", data=rules)

        except Exception as e:
            return GTTRuleResponse(success=False, broker="angel", error=ErrorDetail(code="GTT_LIST_FAILED", message=str(e)))

    def get_gtt_details(self, id: int) -> GTTRuleResponse:
        jwt_token = context.get_auth_token()
        if not jwt_token:
            raise RuntimeError("Authentication token is required")

        try: 
            resp = get_gtt_details_api(id, jwt_token)
            if not resp.get("status"):
                return GTTRuleResponse(success=False, broker="angel", error=ErrorDetail(code=resp.get("errorcode", "GTT_DETAILS_FAILED"), message=resp.get("message")))

            d = resp["data"]

            rule = GTTRule(
                rule_id=str(id),
                symbol=d["tradingsymbol"],
                exchange=d["exchange"],
                transaction_type=TransactionType(d["transactiontype"]),
                product_type=reverse_map_product_type(d["producttype"]),
                quantity=int(d["qty"]),
                price=float(d["price"]),
                trigger_price=float(d["triggerprice"]),
                status=map_gtt_status(d["status"]),
                created_at=d.get("createddate"),
                updated_at=d.get("updateddate"),
                expires_at=d.get("expirydate"),
            )

            return GTTRuleResponse(success=True, broker="angel", data=rule)
            
        except Exception as e:
            return GTTRuleResponse(success=False, broker="angel", error=ErrorDetail(code="GTT_DETAILS_FAILED", message=str(e)))

    # --- WebSocket Streaming ---
    
    # Exchange type mapping for WebSocket
    _WS_EXCHANGE_MAP = {
        "NSE": 1,    # NSE Cash
        "NFO": 2,    # NSE F&O
        "BSE": 3,    # BSE Cash
        "BFO": 4,    # BSE F&O
        "MCX": 5,    # MCX
        "NCX": 7,    # NCDEX
        "CDS": 13,   # Currency
    }
    
    def __init_streaming(self):
        """Lazy initialization of streaming components."""
        if not hasattr(self, '_ws_client'):
            self._ws_client = None
            self._pending_subscriptions: List[tuple] = []  # [(token_list, mode), ...]
            
            # User callbacks
            self.on_tick: Optional[Callable[[Dict[str, Any]], None]] = None
            self.on_error: Optional[Callable[[str, str], None]] = None
            self.on_close: Optional[Callable[[], None]] = None
            self.on_open: Optional[Callable[[], None]] = None
    
    def subscribe(
        self, 
        instruments: List[Equity | Future | Option | Index], 
        mode: StreamMode = StreamMode.QUOTE
    ) -> None:
        """
        Subscribe to real-time market data for given instruments.
        
        Args:
            instruments: List of domain objects (Equity, Future, Option, Index)
            mode: Streaming mode (LTP, QUOTE, SNAP_QUOTE, DEPTH)
        
        Example:
            broker.subscribe([Equity("RELIANCE"), Equity("TCS")], mode=StreamMode.LTP)
        """
        self.__init_streaming()
        
        # Resolve instruments to tokens
        token_list = []
        for inst in instruments:
            resolved = self._resolve_instrument(inst)
            token_list.append({
                "exchange": resolved["exchange"],
                "token": resolved["token"]
            })
        
        # Store for later (if not yet connected)
        self._pending_subscriptions.append((token_list, mode.value))
        
        # If already connected, subscribe immediately
        if self._ws_client and self._ws_client.wsapp:
            self._send_subscription(token_list, mode.value)
    
    def _send_subscription(self, token_list: List[Dict], mode: int) -> None:
        """Send subscription request to WebSocket."""
        # Group by exchange type
        exchange_tokens: Dict[int, List[str]] = {}
        for item in token_list:
            exch_type = self._WS_EXCHANGE_MAP.get(item["exchange"], 1)
            if exch_type not in exchange_tokens:
                exchange_tokens[exch_type] = []
            exchange_tokens[exch_type].append(str(item["token"]))
        
        # Build token list in WebSocket format
        ws_token_list = [
            {"exchangeType": exch, "tokens": tokens}
            for exch, tokens in exchange_tokens.items()
        ]
        
        # Send via SmartWebSocketV2
        self._ws_client.subscribe(
            correlation_id=f"sub_{int(__import__('time').time())}",
            mode=mode,
            token_list=ws_token_list
        )
    
    def start_streaming(self) -> None:
        """
        Connect to the WebSocket server and start streaming.
        
        This is a BLOCKING call - it will run until stop_streaming() is called
        or the connection is closed.
        
        Make sure to:
        1. Call authenticate() first
        2. Set on_tick callback before calling this
        3. Call subscribe() before or after this (subscriptions are buffered)
        
        Example:
            broker.on_tick = lambda tick: print(tick['ltp'])
            broker.subscribe([Equity("RELIANCE")])
            broker.start_streaming()  # Blocks here
        """
        self.__init_streaming()
        
        # Get tokens from context
        jwt_token = context.get_auth_token()
        feed_token = context.get_feed_token()
        
        if not jwt_token or not feed_token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        
        # Import here to avoid circular imports
        from ..internal.angel.streaming import SmartWebSocketV2
        
        # Create WebSocket client
        self._ws_client = SmartWebSocketV2(
            auth_token=jwt_token,
            api_key=self.api_key,
            client_code=self.client_code,
            feed_token=feed_token,
            max_retry_attempt=3,
            retry_delay=5
        )
        
        # Wire up callbacks
        def handle_open(wsapp):
            # Send pending subscriptions
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
        
        self._ws_client.on_open = handle_open
        self._ws_client.on_data = handle_data
        self._ws_client.on_error = handle_error
        self._ws_client.on_close = handle_close
        
        # Connect (blocking)
        self._ws_client.connect()
    
    def stop_streaming(self) -> None:
        """
        Disconnect from the WebSocket server.
        """
        self.__init_streaming()
        if self._ws_client:
            self._ws_client.close_connection()
            self._ws_client = None
