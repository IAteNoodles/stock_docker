import os
import psycopg2
import logging
import json
from typing import Any, Dict, List, Optional

from .config import get_db_settings
import psycopg2.extras as extras

"""Database helpers for stock data and fetch history.

This module manages PostgreSQL connections, the stock_data table, and a history
table used to track when a symbol was last attempted and the latest known data date.
Also includes a 'latest' cache table for most recent EOD data per symbol.
"""

logger = logging.getLogger(__name__)

# Centralized DB settings
_DB = get_db_settings()


def connect_to_db():
    """Create and return a PostgreSQL connection.

    Returns
    -------
    psycopg2.extensions.connection | None
        The connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=_DB.name,
            user=_DB.user,
            password=_DB.password,
            host=_DB.host,
            port=_DB.port,
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


def create_latest_table(cursor) -> None:
    """Ensure the latest cache table exists.

    Table schema:
      latest(
        symbol VARCHAR(50) PRIMARY KEY,
        data JSONB,
        last_updated TIMESTAMPTZ
      )
    """
    query = """
    CREATE TABLE IF NOT EXISTS latest (
        symbol VARCHAR(50) PRIMARY KEY,
        data JSONB,
        last_updated TIMESTAMPTZ
    );
    """
    try:
        cursor.execute(query)
        logger.info("[db] ensured table latest")
    except psycopg2.Error as e:
        logger.error("[db] error creating latest table: %s", e)


def get_latest_cache(cursor, symbol: str) -> Optional[Dict[str, Any]]:
    """Return latest cache row for a symbol: {symbol, data, last_updated} or None."""
    try:
        cursor.execute("SELECT symbol, data, last_updated FROM latest WHERE symbol=%s;", (symbol,))
        row = cursor.fetchone()
        if not row:
            return None
        return {"symbol": row[0], "data": row[1], "last_updated": row[2]}
    except psycopg2.Error as e:
        logger.error("[db] error reading latest cache: %s", e)
        return None


def upsert_latest_cache(cursor, symbol: str, data: Dict[str, Any], last_updated: Optional[str] = None) -> None:
    """Upsert symbol into latest cache with JSON data and last_updated.

    If last_updated is None, NOW() will be used.
    """
    if last_updated is None:
        query = (
            "INSERT INTO latest(symbol, data, last_updated) "
            "VALUES(%s, %s, NOW()) "
            "ON CONFLICT (symbol) DO UPDATE SET data=EXCLUDED.data, last_updated=NOW();"
        )
        params = (symbol, extras.Json(data))
    else:
        query = (
            "INSERT INTO latest(symbol, data, last_updated) "
            "VALUES(%s, %s, %s) "
            "ON CONFLICT (symbol) DO UPDATE SET data=EXCLUDED.data, last_updated=EXCLUDED.last_updated;"
        )
        params = (symbol, extras.Json(data), last_updated)
    try:
        cursor.execute(query, params)
        logger.info("[db] latest cache upserted symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error upserting latest cache: %s", e)


# --- API key history helpers (store only index, never the key) ---

def create_api_key_history_table(cursor) -> None:
    """Ensure the api_key_history table exists with a unique idx column.

    Handles legacy schemas by adding missing columns and a unique constraint on idx.
    """
    # Create table if missing (preferred schema)
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_key_history (
                idx INT,
                last_used TIMESTAMPTZ,
                limit_reached TIMESTAMPTZ
            );
            """
        )
    except psycopg2.Error as e:
        logger.error("[db] error ensuring api_key_history table exists: %s", e)

    # Ensure required columns exist
    try:
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS idx INT;")
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS last_used TIMESTAMPTZ;")
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS limit_reached TIMESTAMPTZ;")
    except psycopg2.Error as e:
        logger.error("[db] error ensuring api_key_history columns: %s", e)

    # Ensure unique constraint or unique index on idx (so ON CONFLICT (idx) works)
    try:
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS api_key_history_idx_uq ON api_key_history (idx);"
        )
    except psycopg2.Error as e:
        logger.error("[db] error creating unique index on api_key_history.idx: %s", e)

    logger.info("[db] ensured table api_key_history")


def upsert_api_keys_history(cursor, keys: List[str]) -> None:
    """Ensure rows exist for indices of provided keys (0..len(keys)-1)."""
    if not keys:
        return
    try:
        for i in range(len(keys)):
            cursor.execute(
                """
                INSERT INTO api_key_history(idx)
                VALUES(%s)
                ON CONFLICT (idx) DO NOTHING;
                """,
                (i,),
            )
        logger.info("[db] api_key_history ensured rows for %d indices", len(keys))
    except psycopg2.Error as e:
        logger.error("[db] error upserting api_key_history indices: %s", e)


def select_usable_api_key_idx(cursor, keys_count: int) -> Optional[int]:
    """Select an index for a usable API key (limit not reached in last 30 days).

    Prefer least recently used (oldest last_used), then by idx. Updates last_used.
    Returns the selected idx or None if none available.
    """
    if not keys_count or keys_count <= 0:
        return None
    try:
        cursor.execute(
            """
            SELECT idx
            FROM api_key_history
            WHERE idx >= 0 AND idx < %s
              AND (limit_reached IS NULL OR limit_reached < NOW() - INTERVAL '30 days')
            ORDER BY COALESCE(last_used, TIMESTAMP '1970-01-01'), idx
            LIMIT 1;
            """,
            (keys_count,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        idx = row[0]
        cursor.execute(
            "UPDATE api_key_history SET last_used = NOW() WHERE idx=%s;",
            (idx,),
        )
        logger.info("[db] selected api key idx=%s", idx)
        return idx
    except psycopg2.Error as e:
        logger.error("[db] error selecting usable api key idx: %s", e)
        return None


def mark_api_key_limit_reached_idx(cursor, idx: int) -> None:
    """Mark a key index as having hit the rate limit now (429 observed)."""
    try:
        cursor.execute(
            "UPDATE api_key_history SET limit_reached = NOW() WHERE idx=%s;",
            (idx,),
        )
        logger.info("[db] api key idx=%s marked limit_reached", idx)
    except psycopg2.Error as e:
        logger.error("[db] error marking api key idx limit_reached: %s", e)


def list_api_key_history(cursor) -> List[Dict[str, Any]]:
    """Return list of api_key_history rows: [{idx, last_used, limit_reached}]."""
    try:
        cursor.execute("SELECT idx, last_used, limit_reached FROM api_key_history ORDER BY idx;")
        rows = cursor.fetchall()
        return [{"idx": r[0], "last_used": r[1], "limit_reached": r[2]} for r in rows]
    except psycopg2.Error as e:
        logger.error("[db] error listing api_key_history: %s", e)
        return []


# --- Tracked symbols (special symbols refreshed every 24h) ---

def create_tracked_symbols_table(cursor) -> None:
    """Ensure the tracked_symbols table exists.

    Schema:
      tracked_symbols(
        symbol VARCHAR(50) PRIMARY KEY,
        tracked_at TIMESTAMPTZ DEFAULT NOW(),
        last_fetched TIMESTAMPTZ
      )
    """
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_symbols (
                symbol VARCHAR(50) PRIMARY KEY,
                tracked_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        # Ensure columns exist for forward-compat
        cursor.execute("ALTER TABLE tracked_symbols ADD COLUMN IF NOT EXISTS tracked_at TIMESTAMPTZ DEFAULT NOW();")
        cursor.execute("ALTER TABLE tracked_symbols ADD COLUMN IF NOT EXISTS last_fetched TIMESTAMPTZ;")
        logger.info("[db] ensured table tracked_symbols")
    except psycopg2.Error as e:
        logger.error("[db] error creating tracked_symbols table: %s", e)


def add_tracked_symbol(cursor, symbol: str) -> None:
    """Add a symbol to tracked_symbols (idempotent)."""
    try:
        cursor.execute(
            """
            INSERT INTO tracked_symbols(symbol, tracked_at)
            VALUES(%s, NOW())
            ON CONFLICT (symbol) DO UPDATE SET tracked_at = tracked_symbols.tracked_at;
            """,
            (symbol,),
        )
        logger.info("[db] tracked_symbols add symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error adding tracked symbol: %s", e)


def remove_tracked_symbol(cursor, symbol: str) -> bool:
    """Remove a symbol from tracked_symbols. Returns True if removed."""
    try:
        cursor.execute("DELETE FROM tracked_symbols WHERE symbol=%s;", (symbol,))
        removed = cursor.rowcount > 0
        logger.info("[db] tracked_symbols remove symbol=%s removed=%s", symbol, removed)
        return removed
    except psycopg2.Error as e:
        logger.error("[db] error removing tracked symbol: %s", e)
        return False


def is_tracked_symbol(cursor, symbol: str) -> bool:
    """Return True if the symbol is currently tracked."""
    try:
        cursor.execute("SELECT 1 FROM tracked_symbols WHERE symbol=%s;", (symbol,))
        return cursor.fetchone() is not None
    except psycopg2.Error as e:
        logger.error("[db] error checking tracked symbol: %s", e)
        return False


def list_tracked_symbols(cursor) -> List[str]:
    """List all tracked symbols."""
    try:
        cursor.execute("SELECT symbol FROM tracked_symbols ORDER BY symbol;")
        return [r[0] for r in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error("[db] error listing tracked symbols: %s", e)
        return []


def list_tracked_symbols_with_meta(cursor) -> List[Dict[str, Any]]:
    """List tracked symbols with metadata (last_fetched)."""
    try:
        cursor.execute("SELECT symbol, last_fetched FROM tracked_symbols ORDER BY symbol;")
        rows = cursor.fetchall()
        return [{"symbol": r[0], "last_fetched": r[1]} for r in rows]
    except psycopg2.Error as e:
        logger.error("[db] error listing tracked symbols meta: %s", e)
        return []


def get_tracked_symbol_last_fetched(cursor, symbol: str) -> Optional[str]:
    """Return last_fetched for a tracked symbol (or None)."""
    try:
        cursor.execute("SELECT last_fetched FROM tracked_symbols WHERE symbol=%s;", (symbol,))
        row = cursor.fetchone()
        return row[0] if row else None
    except psycopg2.Error as e:
        logger.error("[db] error reading last_fetched for %s: %s", symbol, e)
        return None


def set_tracked_last_fetched(cursor, symbol: str, when: Optional[str] = None) -> None:
    """Update last_fetched for a tracked symbol to NOW() or a provided timestamp."""
    try:
        if when is None:
            cursor.execute("UPDATE tracked_symbols SET last_fetched = NOW() WHERE symbol=%s;", (symbol,))
        else:
            cursor.execute("UPDATE tracked_symbols SET last_fetched = %s WHERE symbol=%s;", (when, symbol))
        logger.info("[db] tracked_symbols last_fetched updated symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error updating tracked last_fetched: %s", e)
