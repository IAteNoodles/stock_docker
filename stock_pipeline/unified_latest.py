"""Unified latest refresh for Dagster.

This module provides a single function `refresh_latest_symbols` that:
- Normalizes input symbols
- Enforces a 24h cooldown using the `history` table (last_tried_to_fetch_date)
- Calls the provider via `fetch_latest_data_marketstack` for symbols that are due
- Upserts returned rows into `stock_data` and updates `history.latest_data_date`
- Updates the `latest` cache table for quick reads
- Returns a mapping of symbol -> latest item (or None)

It reuses existing helpers from `stock_pipeline.fetch_data` and the DB layer.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .fetch_data import (
    fetch_latest_data_marketstack,
)
from .fetch_from_db import (
    connect_to_db,
    ensure_history_symbol,
    get_history,
    insert_stock_data,
    update_history_latest_date,
    get_latest_cache,
    upsert_latest_cache,
)
from .db_schema import (
    create_stock_data_table,
    create_history_table,
    create_latest_table,
)

logger = logging.getLogger(__name__)


def _normalize_symbols(symbols: Optional[List[str]]) -> List[str]:
    return [s.strip().upper() for s in (symbols or []) if isinstance(s, str) and s.strip()]


def refresh_latest_symbols(symbols: List[str]) -> Dict[str, Optional[dict]]:
    """Refresh latest EOD for given symbols with 24h cooldown and DB upsert.

    Parameters
    ----------
    symbols : list[str]
        Input symbols (case-insensitive). Empty/invalid entries are ignored.

    Returns
    -------
    dict[str, Optional[dict]]
        Mapping of symbol to latest item (from provider or cache) or None on failure.
    """
    syms = _normalize_symbols(symbols)
    if not syms:
        return {}

    # Prepare DB and determine which symbols are due to fetch
    conn = connect_to_db()
    if not conn:
        logger.error("[unified-latest] DB connection unavailable")
        return {s: None for s in syms}

    due: List[str] = []
    results: Dict[str, Optional[dict]] = {s: None for s in syms}

    try:
        with conn.cursor() as cursor:
            create_history_table(cursor)
            create_latest_table(cursor)
            create_stock_data_table(cursor)
        conn.commit()

        # Check cooldown and collect due list
        now_utc = datetime.now(timezone.utc)
        for sym in syms:
            try:
                with conn.cursor() as cursor:
                    ensure_history_symbol(cursor, sym)
                    hist = get_history(cursor, sym)
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.error("[unified-latest] history ensure/get failed for %s: %s", sym, e)
                continue

            last_tried = hist.get("last_tried_to_fetch_date") if hist else None
            if last_tried is not None and getattr(last_tried, "tzinfo", None) is None:
                last_tried = last_tried.replace(tzinfo=timezone.utc)
            if last_tried is None or (now_utc - last_tried).total_seconds() >= 86400:
                due.append(sym)
            else:
                # Not due: try to return cached value if present
                try:
                    with conn.cursor() as cursor:
                        cache = get_latest_cache(cursor, sym)
                    if cache:
                        results[sym] = cache.get("data")
                except Exception:
                    # ignore cache read errors
                    pass

        if not due:
            logger.info("[unified-latest] no symbols due; returning cached/None for %d symbols", len(syms))
            return results

        # Touch last_tried_to_fetch_date before attempting (best-effort)
        try:
            from .fetch_from_db import touch_history_last_tried  # local import to avoid cycle at file load
            with conn.cursor() as cursor:
                for sym in due:
                    try:
                        touch_history_last_tried(cursor, sym)
                    except Exception:
                        pass
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        # Fetch latest for due symbols (provider handles key rotation)
        latest_map = fetch_latest_data_marketstack(due)

        # Upsert results and update history/cache
        for sym in due:
            item = (latest_map or {}).get(sym)
            if not item:
                logger.error("[unified-latest] no latest item returned for %s (provider error or empty data)", sym)
                results[sym] = None
                continue
            results[sym] = item
            try:
                with conn.cursor() as cursor:
                    insert_stock_data(cursor, sym, [item])
                    latest_date = (item.get("date") or item.get("trade_date") or "")[:10]
                    if latest_date:
                        update_history_latest_date(cursor, sym, latest_date)
                    try:
                        upsert_latest_cache(cursor, sym, item)
                    except Exception:
                        # cache update is non-fatal
                        pass
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.error("[unified-latest] DB upsert failed for %s: %s", sym, e)

        return results
    finally:
        try:
            conn.close()
        except Exception:
            pass
