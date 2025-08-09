"""Data parser and database updater utilities.

This module provides thin wrappers around database helpers to make it easy to
update the PostgreSQL database with stock data records prepared elsewhere. It
also includes a convenience function to upsert raw Marketstack-style items.

Note: The main pipeline logic already lives in fetch_data.process_tickers, which
ensures DB-first behavior and minimal API hits. This module exists to satisfy
project requirements that call for a dedicated update_db.py component.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import logging

from .fetch_from_db import (
    connect_to_db,
    create_stock_data_table,
    insert_stock_data,
)

logger = logging.getLogger(__name__)


def upsert_records(symbol: Optional[str], rows: List[Dict[str, Any]]) -> int:
    """Upsert a list of records into stock_data.

    Parameters
    ----------
    symbol : Optional[str]
        Symbol to apply to all rows; if None, rows must carry `symbol`.
    rows : list[dict]
        Items with keys matching Marketstack fields (date, open, high, low, close, volume).

    Returns
    -------
    int
        Number of rows successfully attempted (inserted or updated). Skipped rows are logged.
    """
    conn = connect_to_db()
    if not conn:
        logger.error("[update_db] no DB connection")
        return 0
    try:
        with conn.cursor() as cursor:
            create_stock_data_table(cursor)
            insert_stock_data(cursor, symbol, rows)
        conn.commit()
        logger.info("[update_db] upsert complete symbol=%s count=%d", symbol or "per-row", len(rows))
        return len(rows)
    except Exception as e:
        logger.error("[update_db] upsert failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
