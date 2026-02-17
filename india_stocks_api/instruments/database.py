"""
Instrument Database (SQLite)
Manages the local storage of tradable instruments using SQLAlchemy.
"""

from datetime import date

from sqlalchemy import Column, Date, Float, Index, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class InstrumentMaster(Base):  # type: ignore[valid-type, misc]
    __tablename__ = "instruments"

    # Primary Key: The unique token used by the broker
    token = Column(String, primary_key=True)

    # Core Identifiers
    symbol = Column(String, index=True)  # OpenAlgo Standard: "NIFTY", "RELIANCE"
    exchange = Column(String, index=True)  # "NSE", "NFO", "BSE"

    # Broker specifics
    tradingsymbol = Column(String)  # Broker's trading symbol: "NIFTY24DECFUT"
    br_symbol = Column(String)  # Raw broker symbol (backup)

    # Financial Details
    expiry = Column(Date, nullable=True, index=True)
    strike = Column(Float, nullable=True, index=True)
    opt_type = Column(String, nullable=True)  # "CE", "PE", "XX" (Futures)
    lot_size = Column(Integer, default=1)
    tick_size = Column(Float, default=0.05)

    # Classification
    instrument_type = Column(String)  # "EQ", "FUT", "OPT", "IDX"

    # Composite Index for fast resolution
    __table_args__ = (Index("idx_resolution", "symbol", "exchange", "expiry", "strike", "opt_type"),)


class InstrumentDB:
    def __init__(self, db_path="instruments.db"):
        import os

        db_path = os.path.abspath(db_path)
        print(f"DEBUG: InstrumentDB initializing at {db_path}")
        print(f"DEBUG: Metadata Tables: {Base.metadata.tables.keys()}")

        self.engine = create_engine(f"sqlite:///{db_path}")
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def get_session(self):
        return self.Session()

    def lookup_token(self, symbol: str, exchange: str, expiry: date = None, strike: float = None, opt_type: str = None):
        """
        O(1) resolution of token from instrument properties.
        """
        session = self.Session()
        try:
            # If no detailed criteria (Shim usage), try matching tradingsymbol first
            if not expiry and not strike:
                # Direct lookup by unique tradingsymbol or symbol
                query = session.query(InstrumentMaster).filter(
                    InstrumentMaster.exchange == exchange,
                    (InstrumentMaster.tradingsymbol == symbol) | (InstrumentMaster.symbol == symbol),
                )
                # If multiple matches (ambiguous), we might return first or handle it
                # For unique tradingsymbol, it returns 1. For 'NIFTY' symbol, returns many.
                return query.first()

            # Detailed Lookup (Resolver usage)
            query = session.query(InstrumentMaster).filter(
                InstrumentMaster.symbol == symbol, InstrumentMaster.exchange == exchange
            )

            if expiry:
                query = query.filter(InstrumentMaster.expiry == expiry)

            if strike is not None:
                query = query.filter(InstrumentMaster.strike == strike)

            if opt_type:
                query = query.filter(InstrumentMaster.opt_type == opt_type)
            else:
                # If identifying a Future, ensure it's not an Option
                # But sometimes FUT doesn't have opt_type=XX in DB?
                if expiry:  # Futures have expiry but no strike/opt_type usually
                    query = query.filter(InstrumentMaster.opt_type.is_(None))

            return query.first()
        finally:
            session.close()

    def raw_bulk_insert(self, records: list[dict], truncate=False):
        """
        Fastest insert using raw sqlite3 executemany.
        Bypasses SQLAlchemy Session overhead.
        """
        import sqlite3

        # Extract DB path from engine URL "sqlite:///path"
        db_path = str(self.engine.url.database)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            if truncate:
                cursor.execute("DELETE FROM instruments")
                conn.commit()

            # Prepare query
            # We assume records keys match table columns.
            # Safe way: explicitly map fields matching the Table definition

            insert_sql = """
            INSERT OR REPLACE INTO instruments (
                token, symbol, exchange, tradingsymbol, br_symbol,
                expiry, strike, opt_type, lot_size, tick_size, instrument_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # Convert list of dicts to list of tuples
            data_tuples = []
            for r in records:
                data_tuples.append(
                    (
                        r["token"],
                        r["symbol"],
                        r["exchange"],
                        r["tradingsymbol"],
                        r["br_symbol"],
                        r["expiry"] or None,
                        r["strike"],
                        r["opt_type"],
                        r["lot_size"],
                        r["tick_size"],
                        r["instrument_type"],
                    )
                )

            cursor.executemany(insert_sql, data_tuples)
            conn.commit()

        except Exception as e:
            print(f"DEBUG: Raw Insert Error: {e}")
            raise
        finally:
            conn.close()
