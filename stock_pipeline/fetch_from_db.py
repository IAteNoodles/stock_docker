import os
import psycopg2
from dotenv import load_dotenv
import logging
from typing import Any, Dict, List, Optional

"""Database helpers for stock data and fetch history.

This module manages PostgreSQL connections, the stock_data table, and a history
table used to track when a symbol was last attempted and the latest known data date.
"""

# Load environment variables from the .env next to this file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

logger = logging.getLogger(__name__)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return a sanitized environment variable value.

    Parameters
    ----------
    name : str
        Name of the environment variable.
    default : Optional[str]
        Default value to return if not present.

    Returns
    -------
    Optional[str]
        Trimmed value or the default if not set.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    # First trim outer whitespace
    v = raw.strip()
    # Remove a single pair of wrapping quotes if present
    if (len(v) >= 2) and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        v = v[1:-1]
    # Trim again to remove inner padding that was inside quotes
    v = v.strip()
    return v if v != "" else default

# Get database connection details from environment variables
DB_NAME = _env("DB_NAME")
DB_USER = _env("DB_USER")
DB_PASS = _env("DB_PASS")
DB_HOST = _env("DB_HOST")
DB_PORT = _env("DB_PORT")


def connect_to_db():
    """Create and return a PostgreSQL connection.

    Returns
    -------
    psycopg2.extensions.connection | None
        The connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("[db] connected to PostgreSQL")
        return conn
    except psycopg2.OperationalError as e:
        logger.error("[db] connection failed: %s", e)
        return None


def create_stock_data_table(cursor) -> None:
    """Ensure the stock_data table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL,
        trade_date DATE NOT NULL,
        open_price NUMERIC(10, 4),
        high_price NUMERIC(10, 4),
        low_price NUMERIC(10, 4),
        close_price NUMERIC(10, 4),
        volume BIGINT,
        UNIQUE (symbol, trade_date)
    );
    """
    try:
        cursor.execute(create_table_query)
        logger.info("[db] ensured table stock_data")
    except psycopg2.Error as e:
        logger.error("[db] error creating table: %s", e)
        

# --- History table helpers ---

def create_history_table(cursor) -> None:
    """Ensure the history table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    """
    query = """
    CREATE TABLE IF NOT EXISTS history (
        symbol VARCHAR(50) PRIMARY KEY,
        latest_data_date DATE,
        last_tried_to_fetch_date TIMESTAMPTZ
    );
    """
    try:
        cursor.execute(query)
        logger.info("[db] ensured table history")
    except psycopg2.Error as e:
        logger.error("[db] error creating history table: %s", e)


def ensure_history_symbol(cursor, symbol: str) -> None:
    """Ensure a history row exists for a symbol.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : str
        Stock symbol to ensure in history.
    """
    try:
        cursor.execute("INSERT INTO history(symbol) VALUES(%s) ON CONFLICT (symbol) DO NOTHING;", (symbol,))
        logger.info("[db] history ensured for symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error ensuring history row: %s", e)


def get_history(cursor, symbol: str) -> Optional[Dict[str, Any]]:
    """Get history entry for a symbol.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : str
        Symbol to query.

    Returns
    -------
    Optional[Dict[str, Any]]
        Dict with latest_data_date and last_tried_to_fetch_date or None if not found.
    """
    try:
        cursor.execute("SELECT latest_data_date, last_tried_to_fetch_date FROM history WHERE symbol=%s;", (symbol,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "latest_data_date": row[0],
            "last_tried_to_fetch_date": row[1],
        }
    except psycopg2.Error as e:
        logger.error("[db] error reading history: %s", e)
        return None


def update_history_latest_date(cursor, symbol: str, latest_date) -> None:
    """Update the latest data date for a symbol in history.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : str
        Symbol to update.
    latest_date : Any
        New latest date value (str YYYY-MM-DD or date).
    """
    try:
        cursor.execute(
            """
            UPDATE history
            SET latest_data_date = GREATEST(COALESCE(latest_data_date, %s), %s)
            WHERE symbol=%s;
            """,
            (latest_date, latest_date, symbol),
        )
        logger.info("[db] history latest_data_date updated symbol=%s date=%s", symbol, latest_date)
    except psycopg2.Error as e:
        logger.error("[db] error updating latest_data_date: %s", e)


def touch_history_last_tried(cursor, symbol: str) -> None:
    """Set the last_tried_to_fetch_date for a symbol to NOW().

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : str
        Symbol to touch.
    """
    try:
        cursor.execute(
            "UPDATE history SET last_tried_to_fetch_date = NOW() WHERE symbol=%s;",
            (symbol,),
        )
        logger.info("[db] history last_tried_to_fetch_date touched symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error updating last_tried_to_fetch_date: %s", e)


def get_max_trade_date_for_symbol(cursor, symbol: str):
    """Return the latest trade_date present in stock_data for a symbol.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : str
        Symbol to query.

    Returns
    -------
    date | None
        Maximum trade_date or None.
    """
    try:
        cursor.execute("SELECT MAX(trade_date) FROM stock_data WHERE symbol=%s;", (symbol,))
        row = cursor.fetchone()
        return row[0] if row else None
    except psycopg2.Error as e:
        logger.error("[db] error getting max trade_date: %s", e)
        return None


def list_history_entries(cursor) -> List[Dict[str, Any]]:
    """List all history entries.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.

    Returns
    -------
    List[Dict[str, Any]]
        Entries with symbol, latest_data_date, last_tried_to_fetch_date.
    """
    try:
        cursor.execute(
            "SELECT symbol, latest_data_date, last_tried_to_fetch_date FROM history ORDER BY symbol;"
        )
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({
                "symbol": row[0],
                "latest_data_date": row[1],
                "last_tried_to_fetch_date": row[2],
            })
        logger.info("[db] history entries count=%d", len(result))
        return result
    except psycopg2.Error as e:
        logger.error("[db] error listing history entries: %s", e)
        return []


# --- Existing stock data helpers ---

def insert_stock_data(cursor, symbol: Optional[str], stock_data: List[Dict[str, Any]]) -> None:
    """Insert or upsert rows into stock_data with per-item validation.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    symbol : Optional[str]
        Symbol to apply to all rows; if None, each item must include 'symbol'.
    stock_data : List[Dict[str, Any]]
        Raw items (e.g., from API) containing price fields and date.

    Notes
    -----
    - If a row lacks a usable 'date' (or 'trade_date') or symbol, it's skipped and an error is logged.
    - Missing numeric fields are inserted as NULLs instead of failing the batch.
    """
    insert_query = """
    INSERT INTO stock_data (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, trade_date) DO UPDATE
    SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume;
    """
    count = 0
    skipped = 0
    try:
        for item in stock_data:
            try:
                row_symbol = symbol or item.get('symbol')
                if not row_symbol:
                    logger.error("[db] insert skip: missing symbol in item=%s", item)
                    skipped += 1
                    continue
                trade_date = (item.get('date') or item.get('trade_date'))
                if isinstance(trade_date, str):
                    trade_date = trade_date[:10]
                if not trade_date:
                    logger.error("[db] insert skip: missing date in item=%s", item)
                    skipped += 1
                    continue
                open_price = item.get('open') or item.get('open_price')
                high_price = item.get('high') or item.get('high_price')
                low_price = item.get('low') or item.get('low_price')
                close_price = item.get('close') or item.get('close_price')
                volume = item.get('volume')
                cursor.execute(insert_query, (row_symbol, trade_date, open_price, high_price, low_price, close_price, volume))
                count += 1
            except Exception as row_err:
                # Log and skip this row; continue with others
                logger.error("[db] insert row error: %s item=%s", row_err, item)
                skipped += 1
        logger.info("[db] upsert symbol=%s count=%d skipped=%d", (symbol or 'per-row'), count, skipped)
    except psycopg2.Error as e:
        logger.error("[db] error inserting data: %s", e)


def get_stock_data(stock_symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Retrieve stock data for a symbol and date range.

    Parameters
    ----------
    stock_symbol : str
        Symbol to query.
    start_date : str
        Inclusive start date (YYYY-MM-DD).
    end_date : str
        Inclusive end date (YYYY-MM-DD).

    Returns
    -------
    List[Dict[str, Any]]
        Rows ordered by trade_date ascending.
    """
    query = """
    SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
    FROM stock_data
    WHERE symbol = %s AND trade_date BETWEEN %s AND %s
    ORDER BY trade_date ASC;
    """
    results: List[Dict[str, Any]] = []
    conn = connect_to_db()
    if conn:
        try:
            with conn.cursor() as cursor:
                logger.info("[db] select symbol=%s range=(%s..%s)", stock_symbol, start_date, end_date)
                cursor.execute(query, (stock_symbol, start_date, end_date))
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        results.append({
                            "symbol": row[0],
                            "date": row[1].strftime("%Y-%m-%d") if row[1] else None,
                            "open": float(row[2]) if row[2] is not None else None,
                            "high": float(row[3]) if row[3] is not None else None,
                            "low": float(row[4]) if row[4] is not None else None,
                            "close": float(row[5]) if row[5] is not None else None,
                            "volume": row[6]
                        })
                    except Exception as row_err:
                        logger.error("[db] map row error: %s row=%s", row_err, row)
                logger.info("[db] rows=%d for symbol=%s", len(results), stock_symbol)
        except psycopg2.Error as e:
            logger.error("[db] error fetching stock data: %s", e)
        finally:
            conn.close()
    return results


def get_existing_dates(cursor, stock_symbol: str, start_date: str, end_date: str):
    """Return a set of date strings present in DB for a symbol and range.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open DB cursor.
    stock_symbol : str
        Symbol to query.
    start_date : str
        Inclusive start date (YYYY-MM-DD).
    end_date : str
        Inclusive end date (YYYY-MM-DD).

    Returns
    -------
    set[str]
        Set of YYYY-MM-DD dates.
    """
    query = """
    SELECT trade_date FROM stock_data
    WHERE symbol = %s AND trade_date BETWEEN %s AND %s;
    """
    cursor.execute(query, (stock_symbol, start_date, end_date))
    rows = cursor.fetchall()
    s = {row[0].strftime("%Y-%m-%d") for row in rows if row and row[0]}
    logger.info("[db] existing_dates symbol=%s count=%d range=(%s..%s)", stock_symbol, len(s), start_date, end_date)
    return s


def add_symbol_to_history(symbol: str) -> bool:
    """Ensure a symbol exists in history.

    Parameters
    ----------
    symbol : str
        Symbol to ensure in history.

    Returns
    -------
    bool
        True on success, False otherwise.
    """
    conn = connect_to_db()
    if not conn:
        return False
    try:
        with conn.cursor() as cursor:
            create_history_table(cursor)
            ensure_history_symbol(cursor, symbol)
        conn.commit()
        logger.info("[db] add_symbol_to_history done symbol=%s", symbol)
        return True
    except Exception as e:
        logger.error("[db] add_symbol_to_history failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass
