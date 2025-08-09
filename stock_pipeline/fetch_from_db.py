import psycopg2
import logging
from typing import Any, Dict, List, Optional

from .config import get_db_settings
import psycopg2.extras as extras

"""Database access helpers (data operations only).

This module encapsulates PostgreSQL connectivity and data access/mutation
operations for the stock pipeline. It intentionally excludes any schema
creation concerns (moved to ``stock_pipeline.db_schema``) to keep a clear
separation of responsibilities.

Conventions
- All functions assume required tables already exist.
- Functions accept a live DB cursor when operating within a transaction
  (for better batching and caller-controlled commits/rollbacks) or open
  and close their own connections when necessary.
- Timestamps returned by the DB are passed through (may be naive or
  timezone-aware depending on the server configuration). Callers should
  normalize to UTC as needed.
"""

logger = logging.getLogger(__name__)

# Centralized DB settings
_DB = get_db_settings()


def connect_to_db():
    """Open a new PostgreSQL connection.

    Returns
    -------
    psycopg2.extensions.connection | None
        A new connection if the operation succeeds, otherwise ``None``.

    Notes
    -----
    - The caller is responsible for closing the connection.
    - Connection parameters come from environment variables via
      :func:`stock_pipeline.config.get_db_settings`.
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


# --- History table data helpers ---

def ensure_history_symbol(cursor, symbol: str) -> None:
    """Ensure a row exists for ``symbol`` in the history table.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol (case-insensitive conventionally uppercased by callers).

    Side Effects
    ------------
    Inserts a row if missing; no-op if the row already exists.
    """
    try:
        cursor.execute("INSERT INTO history(symbol) VALUES(%s) ON CONFLICT (symbol) DO NOTHING;", (symbol,))
        logger.info("[db] history ensured for symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error ensuring history row: %s", e)


def get_history(cursor, symbol: str) -> Optional[Dict[str, Any]]:
    """Retrieve the history row for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.

    Returns
    -------
    Optional[Dict[str, Any]]
        Mapping with keys ``latest_data_date`` (date) and
        ``last_tried_to_fetch_date`` (timestamp) if present; ``None`` if not found
        or an error occurs.
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
    """Set/advance ``history.latest_data_date`` for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.
    latest_date : Any
        New latest date (string YYYY-MM-DD or date object). Existing value is
        advanced using ``GREATEST`` to avoid moving backwards.
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
    """Update ``history.last_tried_to_fetch_date`` to NOW() for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.
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
    """Return the maximum ``trade_date`` present in ``stock_data`` for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.

    Returns
    -------
    date | None
        The latest date if rows exist, otherwise ``None``.
    """
    try:
        cursor.execute("SELECT MAX(trade_date) FROM stock_data WHERE symbol=%s;", (symbol,))
        row = cursor.fetchone()
        return row[0] if row else None
    except psycopg2.Error as e:
        logger.error("[db] error getting max trade_date: %s", e)
        return None


def list_history_entries(cursor) -> List[Dict[str, Any]]:
    """List all rows from the ``history`` table.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.

    Returns
    -------
    List[Dict[str, Any]]
        Each dict contains ``symbol``, ``latest_data_date``, and
        ``last_tried_to_fetch_date``.
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


# --- stock_data data helpers ---

def insert_stock_data(cursor, symbol: Optional[str], stock_data: List[Dict[str, Any]]) -> None:
    """Upsert rows into ``stock_data`` with per-item validation.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : Optional[str]
        Default symbol to apply to all items. If ``None``, each item must
        include a ``symbol`` field.
    stock_data : List[Dict[str, Any]]
        Raw item dicts (e.g., provider responses). Expected keys include
        ``symbol``, ``date`` (or ``trade_date``), and price/volume fields.

    Behavior
    --------
    - Rows missing symbol or date are skipped and logged.
    - Numeric fields are inserted as NULL when missing.
    - Conflicts on (symbol, trade_date) update existing values.
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
                logger.error("[db] insert row error: %s item=%s", row_err, item)
                skipped += 1
        logger.info("[db] upsert symbol=%s count=%d skipped=%d", (symbol or 'per-row'), count, skipped)
    except psycopg2.Error as e:
        logger.error("[db] error inserting data: %s", e)


def get_stock_data(stock_symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Fetch rows from ``stock_data`` for a symbol over an inclusive date range.

    Parameters
    ----------
    stock_symbol : str
        Stock ticker symbol to query.
    start_date : str
        Inclusive start date (YYYY-MM-DD).
    end_date : str
        Inclusive end date (YYYY-MM-DD).

    Returns
    -------
    List[Dict[str, Any]]
        Records sorted by ``trade_date`` ascending; price fields mapped to typical
        names (open, high, low, close) and date formatted as YYYY-MM-DD.
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
    """Return a set of present dates for ``stock_symbol`` within a range.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    stock_symbol : str
        Stock ticker symbol to query.
    start_date : str
        Inclusive start date (YYYY-MM-DD).
    end_date : str
        Inclusive end date (YYYY-MM-DD).

    Returns
    -------
    set[str]
        Set of dates formatted as YYYY-MM-DD.
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


# --- latest cache data helpers ---

def get_latest_cache(cursor, symbol: str) -> Optional[Dict[str, Any]]:
    """Read the latest cache row for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.

    Returns
    -------
    Optional[Dict[str, Any]]
        Dict with keys ``symbol``, ``data`` (JSON), and ``last_updated`` (timestamp)
        if present; otherwise ``None``.
    """
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
    """Insert or update the latest cache for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.
    data : Dict[str, Any]
        JSON-serializable payload to persist.
    last_updated : Optional[str]
        Optional timestamp (ISO string). If omitted, ``NOW()`` is used in the DB.
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


# --- API key history data helpers (store only index, never the key) ---

def upsert_api_keys_history(cursor, keys: List[str]) -> None:
    """Ensure rows exist for all API key indices.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    keys : List[str]
        In-memory list of API keys used at runtime. Only the indices are stored
        in the database, keys themselves are never persisted.
    """
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
    """Select a usable API key index using LRU preference.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    keys_count : int
        Number of keys available in memory.

    Returns
    -------
    Optional[int]
        Selected index or ``None`` if none are usable (e.g. all recently limited).

    Notes
    -----
    - Prefers the key with the oldest ``last_used`` timestamp.
    - Skips keys marked ``limit_reached`` within the last 30 days.
    - Updates ``last_used`` for the selected index.
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
            # Fallback: all keys may be recently limited. Choose the least-recently limited
            # to make forward progress (better than returning None and failing hard).
            cursor.execute(
                """
                SELECT idx
                FROM api_key_history
                WHERE idx >= 0 AND idx < %s
                ORDER BY COALESCE(limit_reached, TIMESTAMP '1970-01-01'),
                         COALESCE(last_used, TIMESTAMP '1970-01-01'), idx
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
    """Mark ``idx`` as rate-limited (HTTP 429 observed) at ``NOW()``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    idx : int
        Index of the API key in the runtime list.
    """
    try:
        cursor.execute(
            "UPDATE api_key_history SET limit_reached = NOW() WHERE idx=%s;",
            (idx,),
        )
        logger.info("[db] api key idx=%s marked limit_reached", idx)
    except psycopg2.Error as e:
        logger.error("[db] error marking api key idx limit_reached: %s", e)


def list_api_key_history(cursor) -> List[Dict[str, Any]]:
    """Return all API key index rows with usage metadata.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.

    Returns
    -------
    List[Dict[str, Any]]
        Dicts with ``idx``, ``last_used``, and ``limit_reached``.
    """
    try:
        cursor.execute("SELECT idx, last_used, limit_reached FROM api_key_history ORDER BY idx;")
        rows = cursor.fetchall()
        return [{"idx": r[0], "last_used": r[1], "limit_reached": r[2]} for r in rows]
    except psycopg2.Error as e:
        logger.error("[db] error listing api_key_history: %s", e)
        return []


# --- Tracked symbols data helpers ---

def add_tracked_symbol(cursor, symbol: str) -> None:
    """Add ``symbol`` to the ``tracked_symbols`` set.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol to track.

    Notes
    -----
    Idempotent; existing rows are left unchanged.
    """
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
    """Remove ``symbol`` from ``tracked_symbols``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol to untrack.

    Returns
    -------
    bool
        ``True`` if a row was deleted; ``False`` otherwise.
    """
    try:
        cursor.execute("DELETE FROM tracked_symbols WHERE symbol=%s;", (symbol,))
        removed = cursor.rowcount > 0
        logger.info("[db] tracked_symbols remove symbol=%s removed=%s", symbol, removed)
        return removed
    except psycopg2.Error as e:
        logger.error("[db] error removing tracked symbol: %s", e)
        return False


def list_tracked_symbols(cursor) -> List[str]:
    """List all tracked symbols.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.

    Returns
    -------
    List[str]
        Sorted list of tracked ticker symbols.
    """
    try:
        cursor.execute("SELECT symbol FROM tracked_symbols ORDER BY symbol;")
        return [r[0] for r in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error("[db] error listing tracked symbols: %s", e)
        return []


def list_tracked_symbols_with_meta(cursor) -> List[Dict[str, Any]]:
    """List tracked symbols with metadata.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.

    Returns
    -------
    List[Dict[str, Any]]
        Dicts with ``symbol`` and ``last_fetched`` timestamp for each tracked row.
    """
    try:
        cursor.execute("SELECT symbol, last_fetched FROM tracked_symbols ORDER BY symbol;")
        rows = cursor.fetchall()
        return [{"symbol": r[0], "last_fetched": r[1]} for r in rows]
    except psycopg2.Error as e:
        logger.error("[db] error listing tracked symbols meta: %s", e)
        return []


def set_tracked_last_fetched(cursor, symbol: str, when: Optional[str] = None) -> None:
    """Update ``tracked_symbols.last_fetched`` for ``symbol``.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active transaction.
    symbol : str
        Stock ticker symbol.
    when : Optional[str]
        Optional timestamp to set explicitly; when omitted, ``NOW()`` is used.
    """
    try:
        if when is None:
            cursor.execute("UPDATE tracked_symbols SET last_fetched = NOW() WHERE symbol=%s;", (symbol,))
        else:
            cursor.execute("UPDATE tracked_symbols SET last_fetched = %s WHERE symbol=%s;", (when, symbol))
        logger.info("[db] tracked_symbols last_fetched updated symbol=%s", symbol)
    except psycopg2.Error as e:
        logger.error("[db] error updating tracked last_fetched: %s", e)
