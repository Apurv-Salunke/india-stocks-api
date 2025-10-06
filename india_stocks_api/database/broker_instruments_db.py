"""
Broker Instrument Database System
Manages all broker tokens and instrument mappings using SQLAlchemy
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Float, Sequence, Index, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_PATH = Path(__file__).parent.parent.parent / "_cache" / "broker_instruments.db"
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# Ensure database directory exists
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)
Base = declarative_base()
Base.query = db_session.query_property()


class BrokerInstrument(Base):
    """
    Broker Instrument mapping table
    Stores all broker-specific tokens and instrument details
    """

    __tablename__ = "broker_instruments"

    id = Column(Integer, Sequence("broker_instruments_id_seq"), primary_key=True)
    standardized_symbol = Column(String, nullable=False, index=True)  # e.g., "RELIANCE"
    broker_symbol = Column(String, nullable=False, index=True)  # e.g., "RELIANCE-EQ"
    instrument_name = Column(String)  # e.g., "Reliance Industries Ltd"
    exchange_code = Column(String, nullable=False, index=True)  # e.g., "NSE", "BSE"
    broker_exchange_code = Column(String)  # Broker-specific exchange code
    broker_token = Column(String, nullable=False, index=True)  # e.g., "2885"
    expiry_date = Column(String)  # For F&O instruments
    strike_price = Column(Float)  # For options
    lot_size = Column(Integer)  # Trading lot size
    instrument_type = Column(String)  # e.g., "EQ", "FUT", "CE", "PE"
    tick_size = Column(Float)  # Minimum price movement
    broker_name = Column(
        String, nullable=False, index=True
    )  # e.g., "angelone", "zerodha"
    is_active = Column(Integer, default=1)  # 1 = active, 0 = inactive
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Composite indexes for performance
    __table_args__ = (
        Index(
            "idx_standardized_symbol_exchange", "standardized_symbol", "exchange_code"
        ),
        Index("idx_broker_symbol_exchange", "broker_symbol", "exchange_code"),
        Index("idx_broker_token_exchange", "broker_token", "exchange_code"),
        Index("idx_broker_name_symbol", "broker_name", "standardized_symbol"),
    )


def initialize_broker_database():
    """Initialize the broker instruments database"""
    logger.info("Initializing Broker Instruments Database")
    Base.metadata.create_all(bind=engine)
    logger.info(f"Database initialized at {DATABASE_PATH}")


def clear_broker_instruments(broker_name: str):
    """Clear all instruments for a specific broker"""
    logger.info(f"Clearing all instruments for broker: {broker_name}")
    BrokerInstrument.query.filter(BrokerInstrument.broker_name == broker_name).delete()
    db_session.commit()


def store_broker_instruments(instruments_data: List[Dict[str, Any]], broker_name: str):
    """
    Store broker instruments in bulk

    Args:
        instruments_data: List of instrument dictionaries
        broker_name: Name of the broker (e.g., 'angelone', 'zerodha')
    """
    if not instruments_data:
        logger.warning(f"No instruments to store for broker {broker_name}")
        return

    logger.info(f"Storing {len(instruments_data)} instruments for broker {broker_name}")

    # Clear existing instruments for this broker
    clear_broker_instruments(broker_name)

    # Prepare data for bulk insert
    insert_data = []
    for instrument in instruments_data:
        insert_data.append(
            {
                "standardized_symbol": instrument.get("symbol", ""),
                "broker_symbol": instrument.get("brsymbol", ""),
                "instrument_name": instrument.get("name", ""),
                "exchange_code": instrument.get("exchange", ""),
                "broker_exchange_code": instrument.get("brexchange", ""),
                "broker_token": instrument.get("token", ""),
                "expiry_date": instrument.get("expiry", ""),
                "strike_price": instrument.get("strike", 0.0),
                "lot_size": instrument.get("lotsize", 1),
                "instrument_type": instrument.get("instrumenttype", ""),
                "tick_size": instrument.get("tick_size", 0.01),
                "broker_name": broker_name,
            }
        )

    # Bulk insert
    try:
        db_session.bulk_insert_mappings(BrokerInstrument, insert_data)
        db_session.commit()
        logger.info(
            f"Successfully stored {len(insert_data)} instruments for broker {broker_name}"
        )
    except Exception as e:
        logger.error(f"Error storing instruments for broker {broker_name}: {e}")
        db_session.rollback()
        raise


def get_broker_token(
    standardized_symbol: str, exchange_code: str, broker_name: str
) -> Optional[str]:
    """
    Get broker token for a standardized symbol

    Args:
        standardized_symbol: Standardized symbol (e.g., "RELIANCE")
        exchange_code: Exchange code (e.g., "NSE", "BSE")
        broker_name: Broker name (e.g., "angelone")

    Returns:
        Broker token as string, or None if not found
    """
    instrument = BrokerInstrument.query.filter(
        BrokerInstrument.standardized_symbol == standardized_symbol,
        BrokerInstrument.exchange_code == exchange_code,
        BrokerInstrument.broker_name == broker_name,
        BrokerInstrument.is_active == 1,
    ).first()

    return instrument.broker_token if instrument else None


def get_broker_symbol(
    standardized_symbol: str, exchange_code: str, broker_name: str
) -> Optional[str]:
    """
    Get broker-specific symbol for a standardized symbol

    Args:
        standardized_symbol: Standardized symbol (e.g., "RELIANCE")
        exchange_code: Exchange code (e.g., "NSE", "BSE")
        broker_name: Broker name (e.g., "angelone")

    Returns:
        Broker-specific symbol, or None if not found
    """
    instrument = BrokerInstrument.query.filter(
        BrokerInstrument.standardized_symbol == standardized_symbol,
        BrokerInstrument.exchange_code == exchange_code,
        BrokerInstrument.broker_name == broker_name,
        BrokerInstrument.is_active == 1,
    ).first()

    return instrument.broker_symbol if instrument else None


def get_standardized_symbol(
    broker_token: str, exchange_code: str, broker_name: str
) -> Optional[str]:
    """
    Get standardized symbol from broker token (reverse lookup)

    Args:
        broker_token: Broker token
        exchange_code: Exchange code (e.g., "NSE", "BSE")
        broker_name: Broker name (e.g., "angelone")

    Returns:
        Standardized symbol, or None if not found
    """
    instrument = BrokerInstrument.query.filter(
        BrokerInstrument.broker_token == broker_token,
        BrokerInstrument.exchange_code == exchange_code,
        BrokerInstrument.broker_name == broker_name,
        BrokerInstrument.is_active == 1,
    ).first()

    return instrument.standardized_symbol if instrument else None


def get_instrument_details(
    standardized_symbol: str, exchange_code: str, broker_name: str
) -> Optional[Dict[str, Any]]:
    """
    Get comprehensive instrument details

    Args:
        standardized_symbol: Standardized symbol (e.g., "RELIANCE")
        exchange_code: Exchange code (e.g., "NSE", "BSE")
        broker_name: Broker name (e.g., "angelone")

    Returns:
        Dictionary with instrument details, or None if not found
    """
    instrument = BrokerInstrument.query.filter(
        BrokerInstrument.standardized_symbol == standardized_symbol,
        BrokerInstrument.exchange_code == exchange_code,
        BrokerInstrument.broker_name == broker_name,
        BrokerInstrument.is_active == 1,
    ).first()

    if instrument:
        return {
            "standardized_symbol": instrument.standardized_symbol,
            "broker_symbol": instrument.broker_symbol,
            "instrument_name": instrument.instrument_name,
            "exchange_code": instrument.exchange_code,
            "broker_exchange_code": instrument.broker_exchange_code,
            "broker_token": instrument.broker_token,
            "expiry_date": instrument.expiry_date,
            "strike_price": instrument.strike_price,
            "lot_size": instrument.lot_size,
            "instrument_type": instrument.instrument_type,
            "tick_size": instrument.tick_size,
            "broker_name": instrument.broker_name,
        }
    return None


def get_database_statistics() -> Dict[str, Any]:
    """Get database statistics"""
    total_instruments = BrokerInstrument.query.filter(
        BrokerInstrument.is_active == 1
    ).count()

    # Count by broker
    broker_counts = {}
    brokers = db_session.query(BrokerInstrument.broker_name).distinct().all()
    for broker in brokers:
        count = BrokerInstrument.query.filter(
            BrokerInstrument.broker_name == broker[0], BrokerInstrument.is_active == 1
        ).count()
        broker_counts[broker[0]] = count

    # Count by exchange
    exchange_counts = {}
    exchanges = db_session.query(BrokerInstrument.exchange_code).distinct().all()
    for exchange in exchanges:
        count = BrokerInstrument.query.filter(
            BrokerInstrument.exchange_code == exchange[0],
            BrokerInstrument.is_active == 1,
        ).count()
        exchange_counts[exchange[0]] = count

    return {
        "total_instruments": total_instruments,
        "broker_counts": broker_counts,
        "exchange_counts": exchange_counts,
        "database_path": str(DATABASE_PATH),
        "database_size_mb": round(DATABASE_PATH.stat().st_size / (1024 * 1024), 2)
        if DATABASE_PATH.exists()
        else 0,
    }


def search_instruments(
    symbol_pattern: str, exchange_code: str, broker_name: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Search for instruments matching a pattern

    Args:
        symbol_pattern: Pattern to search for (e.g., "RELI")
        exchange_code: Exchange code (e.g., "NSE", "BSE")
        broker_name: Broker name (e.g., "angelone")
        limit: Maximum number of results

    Returns:
        List of matching instrument dictionaries
    """
    instruments = (
        BrokerInstrument.query.filter(
            BrokerInstrument.standardized_symbol.like(f"%{symbol_pattern}%"),
            BrokerInstrument.exchange_code == exchange_code,
            BrokerInstrument.broker_name == broker_name,
            BrokerInstrument.is_active == 1,
        )
        .limit(limit)
        .all()
    )

    results = []
    for instrument in instruments:
        results.append(
            {
                "standardized_symbol": instrument.standardized_symbol,
                "broker_symbol": instrument.broker_symbol,
                "broker_token": instrument.broker_token,
                "instrument_name": instrument.instrument_name,
                "lot_size": instrument.lot_size,
                "tick_size": instrument.tick_size,
                "instrument_type": instrument.instrument_type,
            }
        )

    return results


# Initialize database on import
if not DATABASE_PATH.exists():
    initialize_broker_database()
