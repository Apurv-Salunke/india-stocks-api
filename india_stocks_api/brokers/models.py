from datetime import datetime, timezone
from typing import List, Optional, Any, TypeVar, Generic, TypeAlias
from ..constants import TransactionType, OrderType, ProductType
from pydantic import BaseModel, Field, ConfigDict


# -------------------------
# Base error + response
# -------------------------

class ErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[Any] = Field(
        default=None,
        description="Optional structured error details"
    )


class BaseResponse(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_strip_whitespace=True
    )

    success: bool = Field(..., description="Indicates if operation succeeded")
    broker: str = Field(..., description="Broker identifier")

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when response was generated"
    )

    error: Optional[ErrorDetail] = Field(
        default=None,
        description="Structured error information if success=False"
    )

    raw: Optional[Any] = Field(
        default=None,
        description="Optional raw broker response (debug only, do not use in production)"
    )


# -------------------------
# Generic responses
# -------------------------

T = TypeVar("T")


class DataResponse(BaseResponse, Generic[T]):
    """
    Generic response wrapper for any method returning single object.
    """

    data: Optional[T] = Field(
        default=None,
        description="Typed response payload"
    )


class ListResponse(BaseResponse, Generic[T]):
    """
    Generic response wrapper for list-based responses.
    """

    data: List[T] = Field(
        default_factory=list,
        description="List response payload"
    )


class ExecutionResponse(BaseResponse, Generic[T]):
    """
    Response wrapper for state-changing operations.
    """

    data: Optional[T] = Field(
        default=None,
        description="Execution result payload"
    )

    execution_id: Optional[str] = Field(
        default=None,
        description="Broker execution reference (order id, gtt id, etc.)"
    )


# -------------------------
# Core primitives
# -------------------------

class DepthLevel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price: float
    quantity: int
    orders: Optional[int] = None


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str
    symbol: str
    exchange: str

    transaction_type: TransactionType
    order_type: OrderType
    product_type: ProductType

    quantity: int
    filled_quantity: int = 0

    price: float
    average_price: Optional[float] = None

    status: str
    timestamp: Optional[datetime] = None


class Position(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str
    quantity: int
    average_price: float
    pnl: float
    product_type: ProductType


class Holding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str
    quantity: int
    average_price: float
    current_price: float
    pnl: float
    product_type: ProductType


class Trade(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_id: str
    order_id: str
    symbol: str

    side: TransactionType
    quantity: int
    price: float

    timestamp: Optional[datetime] = None


class Quote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str

    last_price: float

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    timestamp: Optional[datetime] = None


class MarketDepth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str

    bids: List[DepthLevel]
    asks: List[DepthLevel]

    timestamp: Optional[datetime] = None


class Funds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available: float
    used: float
    total: float


class Profile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    name: str
    broker: str

    email: Optional[str] = None
    phone: Optional[str] = None


class Candle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: datetime

    open: float
    high: float
    low: float
    close: float

    volume: int


# -------------------------
# Typed response aliases
# -------------------------

OrderResponse: TypeAlias = DataResponse[Order]
OrdersResponse: TypeAlias = ListResponse[Order]

PositionResponse: TypeAlias = DataResponse[Position]
PositionsResponse: TypeAlias = ListResponse[Position]

HoldingResponse: TypeAlias = DataResponse[Holding]
HoldingsResponse: TypeAlias = ListResponse[Holding]

TradeResponse: TypeAlias = DataResponse[Trade]
TradesResponse: TypeAlias = ListResponse[Trade]

QuoteResponse: TypeAlias = DataResponse[Quote]
DepthResponse: TypeAlias = DataResponse[MarketDepth]

FundsResponse: TypeAlias = DataResponse[Funds]
ProfileResponse: TypeAlias = DataResponse[Profile]

CandleResponse: TypeAlias = DataResponse[Candle]
CandlesResponse: TypeAlias = ListResponse[Candle]

OrderExecutionResponse: TypeAlias = ExecutionResponse[Order]
GTTExecutionResponse: TypeAlias = ExecutionResponse[Any]
BatchExecutionResponse: TypeAlias = ExecutionResponse[List[Any]]