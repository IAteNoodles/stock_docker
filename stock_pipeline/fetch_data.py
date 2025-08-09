"""Fetcher and orchestration logic for the stock pipeline.

Responsibilities
- Provider accessors for Marketstack v2 endpoints (/eod and /eod/latest) with
  API-key rotation and robust error handling.
- Orchestration to ensure DB-first behavior and enforce a 24h cooldown based on
  the history table (process_tickers).
- Latest cache helper that prefers cached values within a 24h TTL
  (get_or_refresh_latest).
- Background workers for periodic refreshes based on cooldown and tracked
  symbols (get_latest_value, run_tracked_refresher).

Notes
- Network calls are kept minimal and polite (small sleeps). Tests mock requests.
- API keys are selected by index and persisted only as indices; actual keys are
  never stored in the DB.
"""

import os
import requests
import json
import time
from datetime import datetime, timedelta, date, timezone
import logging
import threading

from .config import _env, get_marketstack_api_keys
from .fetch_from_db import (
    connect_to_db,
    insert_stock_data,
    get_stock_data,
    get_existing_dates,
    ensure_history_symbol,
    get_history,
    update_history_latest_date,
    touch_history_last_tried,
    get_max_trade_date_for_symbol,
    list_history_entries,
    get_latest_cache,
    upsert_latest_cache,
    upsert_api_keys_history,
    select_usable_api_key_idx,
    mark_api_key_limit_reached_idx,
    add_tracked_symbol,
    remove_tracked_symbol,
    list_tracked_symbols_with_meta,
    set_tracked_last_fetched,
)
from .db_schema import (
    create_stock_data_table,
    create_history_table,
    create_latest_table,
    create_api_key_history_table,
    create_tracked_symbols_table,
)

logger = logging.getLogger(__name__)


_selected_api_key_idx: int | None = None  # in-process cached selected key index


def _select_api_key_index_once(keys: list[str]) -> int | None:
    """Select and cache an API key index via DB once; fallback to 0.

    Parameters
    ----------
    keys : list[str]
        In-memory list of API keys. Only the selected index is recorded to DB.

    Returns
    -------
    Optional[int]
        Selected index; ``0`` is used as a safe fallback when DB selection fails.

    Called at first use or when the previously selected key hits rate limit.
    """
    if not keys:
        return None
    conn = connect_to_db()
    if not conn:
        return 0
    try:
        with conn.cursor() as cursor:
            create_api_key_history_table(cursor)
            upsert_api_keys_history(cursor, keys)
            idx = select_usable_api_key_idx(cursor, len(keys))
        conn.commit()
        return idx if idx is not None else 0
    except Exception:
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


def _mark_selected_key_limited(keys: list[str]) -> None:
    """Mark the currently selected API key index as rate-limited.

    Parameters
    ----------
    keys : list[str]
        In-memory list of API keys; if empty or no selection exists, this is a no-op.

    Side Effects
    ------------
    Updates ``api_key_history.limit_reached`` for the selected index and clears
    the in-process cache so the next call reselects a new key.
    """
    global _selected_api_key_idx
    if _selected_api_key_idx is None or not keys:
        return
    conn = connect_to_db()
    if not conn:
        return
    try:
        with conn.cursor() as cursor:
            create_api_key_history_table(cursor)
            mark_api_key_limit_reached_idx(cursor, _selected_api_key_idx)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _current_api_key(keys: list[str]) -> str | None:
    """Return the currently selected API key, selecting once if needed.

    Parameters
    ----------
    keys : list[str]
        In-memory list of API keys.

    Returns
    -------
    Optional[str]
        The selected API key string or ``None`` if no keys are available.
    """
    global _selected_api_key_idx
    if not keys:
        return None
    if _selected_api_key_idx is None:
        _selected_api_key_idx = _select_api_key_index_once(keys)
    if _selected_api_key_idx is None:
        return None
    if _selected_api_key_idx < 0 or _selected_api_key_idx >= len(keys):
        _selected_api_key_idx = 0
    return keys[_selected_api_key_idx]


def fetch_historical_data_marketstack(tickers, start_date, end_date):
    """Fetch historical daily stock data from Marketstack with key rotation.

    IMPORTANT: Never fetch per individual date to avoid exhausting API limits.
    Uses one logical request per ticker (with pagination up to page_size=1000),
    and rotates API keys on HTTP 429.
    """
    keys = get_marketstack_api_keys()
    if not keys:
        logger.warning("MARKETSTACK_API_KEY(S) not found. Proceeding with dummy key for test/mocked runs.")
        keys = [""]

    base_url = "http://api.marketstack.com/v2/eod"
    all_data = {}

    for ticker in tickers:
        logger.info("[api-fetch] ticker=%s window=(%s..%s)", ticker, start_date, end_date)
        collected = []
        offset = 0
        page_size = 1000
        max_rows = 1000

        while len(collected) < max_rows:
            api_key = _current_api_key(keys)
            if api_key is None:
                logger.error("[api-fetch] no usable API key available")
                break
            # perform request with the selected key only
            def do_request_with_key(key: str):
                try:
                    params = {
                        "access_key": key,
                        "symbols": ticker,
                        "date_from": start_date,
                        "date_to": end_date,
                        "limit": page_size,
                        "offset": offset,
                        "sort": "ASC",
                    }
                    logger.warning("[api-fetch] using API key for ticker=%s", ticker)
                    response = requests.get(base_url, params=params, timeout=30)
                    status = getattr(response, "status_code", 200)
                    try:
                        status = int(status)
                    except Exception:
                        status = 200
                    return (response, status)
                except Exception as e:
                    logger.error("[api-fetch] request error ticker=%s err=%s", ticker, e)
                    return (None, 599)

            response, status = do_request_with_key(api_key)

            if status == 429:
                logger.warning("[api-fetch] 429 for current key; marking limited and reselecting")
                _mark_selected_key_limited(keys)
                # force reselect on next loop
                _selected_api_key_idx = None
                time.sleep(0.2)
                continue

            if not response:
                logger.error("[api-fetch] no response (request failure) ticker=%s", ticker)
                break

            status = getattr(response, "status_code", 200)
            try:
                status = int(status)
            except Exception:
                status = 200
            if status >= 400:
                try:
                    payload = response.text
                except Exception:
                    payload = "<no body>"
                logger.error("[api-fetch] HTTP error %s ticker=%s body=%s", status, ticker, payload)
                break

            try:
                data = response.json()
            except Exception as e:
                logger.error("[api-fetch] json decode error ticker=%s err=%s", ticker, e)
                break

            if isinstance(data, dict) and data.get("error"):
                logger.error("[api-fetch] provider error ticker=%s err=%s", ticker, data.get("error"))
                break

            items = data.get("data", []) if isinstance(data, dict) else []
            if not isinstance(items, list):
                logger.error("[api-fetch] unexpected payload format for ticker=%s", ticker)
                break
            if not items:
                break
            valid_items = [it for it in items if isinstance(it, dict)]
            for it in items:
                if not isinstance(it, dict):
                    logger.error("[api-fetch] non-dict item ignored ticker=%s item=%s", ticker, it)
            collected.extend(valid_items)
            if len(items) < page_size:
                break
            offset += page_size
            time.sleep(0.25)

        logger.info("[api-fetch] ticker=%s collected=%d", ticker, len(collected))
        all_data[ticker] = collected[:max_rows]
        time.sleep(0.5)

    return all_data


def fetch_latest_data_marketstack(tickers):
    """Fetch the latest EOD data using /v2/eod/latest with key rotation.

    Returns a dict mapping symbol -> latest item dict (or None).
    """
    keys = get_marketstack_api_keys()
    if not keys:
        logger.warning("MARKETSTACK_API_KEY(S) not found. Proceeding with dummy key for test/mocked runs.")
        keys = [""]

    base_url = "http://api.marketstack.com/v2/eod/latest"
    all_latest = {}

    for ticker in tickers:
        api_key = _current_api_key(keys)
        if api_key is None:
            all_latest[ticker] = None
            continue

        def do_request_with_key(key: str):
            try:
                params = {"access_key": key, "symbols": ticker}
                logger.warning("[api-latest] using API key for ticker=%s", ticker)
                resp = requests.get(base_url, params=params, timeout=20)
                status = getattr(resp, "status_code", 200)
                try:
                    status = int(status)
                except Exception:
                    status = 200
                return (resp, status)
            except Exception as e:
                logger.error("[api-latest] request error ticker=%s err=%s", ticker, e)
                return (None, 599)

        resp, status = do_request_with_key(api_key)
        if status == 429:
            logger.warning("[api-latest] 429 for current key; marking limited and reselecting")
            _mark_selected_key_limited(keys)
            _selected_api_key_idx = None
            # try once with newly selected key
            api_key2 = _current_api_key(keys)
            if api_key2 is not None and api_key2 != api_key:
                resp, status = do_request_with_key(api_key2)
            else:
                resp = None

        if not resp:
            all_latest[ticker] = None
            continue

        status = getattr(resp, "status_code", 200)
        try:
            status = int(status)
        except Exception:
            status = 200
        if status >= 400:
            logger.error("[api-latest] HTTP %s ticker=%s", status, ticker)
            all_latest[ticker] = None
            continue
        try:
            data = resp.json()
        except Exception as e:
            logger.error("[api-latest] json decode error ticker=%s err=%s", ticker, e)
            all_latest[ticker] = None
            continue

        if isinstance(data, dict) and data.get("error"):
            logger.error("[api-latest] provider error ticker=%s err=%s", ticker, data.get("error"))
            all_latest[ticker] = None
            continue

        items = data.get("data", []) if isinstance(data, dict) else []
        item = None
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict):
                item = first
        elif isinstance(data, dict) and "symbol" in data and "date" in data:
            item = data
        all_latest[ticker] = item
        time.sleep(0.2)

    return all_latest


def _dates_set(start_date: str, end_date: str) -> set[str]:
    """Return the set of YYYY-MM-DD dates in the inclusive range.

    Parameters
    ----------
    start_date : str
        Inclusive start date.
    end_date : str
        Inclusive end date.
    """
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = (e - s).days
    return {(s + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)}


def _last_1000_window() -> tuple[str, str]:
    """Return a tuple (start, end) covering the last 1000 days including today."""
    today = date.today()
    start = today - timedelta(days=999)
    return (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))


def _range_within_last_1000(start_date: str, end_date: str) -> bool:
    """Whether a date range is fully inside the last 1000 days.

    Parameters
    ----------
    start_date : str
        Inclusive start date.
    end_date : str
        Inclusive end date.

    Returns
    -------
    bool
        True if within last 1000 days.
    """
    win_start, win_end = _last_1000_window()
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()
    ws = datetime.strptime(win_start, "%Y-%m-%d").date()
    we = datetime.strptime(win_end, "%Y-%m-%d").date()
    return s >= ws and e <= we


def process_tickers(tickers, start_date, end_date):
    """Ensure data exists for tickers in the requested range with minimal API calls.

    IMPORTANT: Do not split requests into per-day API calls. If fetching is needed,
    perform only one logical fetch per ticker (broad window when applicable).
    """
    logger.info("[process] start=%s end=%s tickers=%s", start_date, end_date, tickers)

    succeeded, failed = set(), set()
    api_calls = 0  # per-request API call counter (one per ticker fetch)

    conn = connect_to_db()
    if not conn:
        logger.error("[process] Cannot connect to DB; aborting.")
        return succeeded, set(tickers)

    try:
        with conn.cursor() as cursor:
            logger.info("[process] ensuring table exists...")
            create_stock_data_table(cursor)
            create_history_table(cursor)
            conn.commit()

        for ticker in tickers:
            try:
                # Ensure history row and get history
                with conn.cursor() as cursor:
                    ensure_history_symbol(cursor, ticker)
                    hist = get_history(cursor, ticker)
                conn.commit()

                # Compute requested coverage and existing coverage
                need = _dates_set(start_date, end_date)
                with conn.cursor() as cursor:
                    existing = get_existing_dates(cursor, ticker, start_date, end_date)
                    max_db_date = get_max_trade_date_for_symbol(cursor, ticker)
                conn.commit()

                # If fully covered, update latest date and skip fetch
                if existing and existing.issuperset(need):
                    if max_db_date:
                        try:
                            with conn.cursor() as cursor:
                                update_history_latest_date(cursor, ticker, max_db_date)
                            conn.commit()
                        except Exception as e:
                            logger.warning("[process] ticker=%s unable to update history from DB: %s", ticker, e)
                    logger.info("[process] ticker=%s fully covered by DB; skip fetch", ticker)
                    succeeded.add(ticker)
                    continue

                # Missing coverage -> need one fetch (never per-date)
                missing = need - (existing or set())
                if not missing:
                    # Edge case: no explicit missing computed (empty need?), mark success
                    logger.info("[process] ticker=%s nothing missing; mark success", ticker)
                    succeeded.add(ticker)
                    continue

                # Enforce 24h backoff using history
                now_utc = datetime.now(timezone.utc)
                last_tried = hist.get("last_tried_to_fetch_date") if hist else None
                
                if last_tried is not None and last_tried.tzinfo is None:
                    last_tried = last_tried.replace(tzinfo=timezone.utc)
                if last_tried is not None:
                    elapsed = (now_utc - last_tried).total_seconds()
                    if elapsed < 86400:
                        wait_hours = round((86400 - elapsed) / 3600, 2)
                        logger.info("[process] ticker=%s skip fetch (cooldown %.2f hours remaining)", ticker, wait_hours)
                        failed.add(ticker)
                        continue

                # Touch last_tried_to_fetch_date before attempting
                with conn.cursor() as cursor:
                    touch_history_last_tried(cursor, ticker)
                conn.commit()

                # Single fetch window is the requested range (never per-date)
                fetch_from, fetch_to = start_date, end_date

                api_calls += 1
                logger.warning(
                    "[process] API_CALL #%d (this request): ticker=%s window=(%s..%s)",
                    api_calls,
                    ticker,
                    fetch_from,
                    fetch_to,
                )
                api_result = fetch_historical_data_marketstack([ticker], fetch_from, fetch_to)
                records = api_result.get(ticker, [])
                logger.info("[process] ticker=%s fetched_records=%d", ticker, len(records))
                if not records:
                    logger.warning("[process] ticker=%s no API data", ticker)
                    failed.add(ticker)
                    continue

                with conn.cursor() as cursor:
                    insert_stock_data(cursor, ticker, records)
                conn.commit()
                logger.info("[process] ticker=%s upserted=%d", ticker, len(records))

                # Update history.latest_data_date using fetched max or DB max
                try:
                    max_rec_date = max(
                        ((r.get("date") or r.get("trade_date") or "")[:10] for r in records),
                        default=None,
                    )
                    final_latest = max_rec_date
                    if max_db_date and (final_latest is None or str(max_db_date) > final_latest):
                        final_latest = str(max_db_date)
                    if final_latest:
                        with conn.cursor() as cursor:
                            update_history_latest_date(cursor, ticker, final_latest)
                        conn.commit()
                except Exception as e:
                    logger.warning("[process] ticker=%s unable to compute/update max fetched date: %s", ticker, e)

                # Read-back (optional, helps callers expecting data immediately from DB)
                rows = get_stock_data(ticker, start_date, end_date)
                logger.info("[process] ticker=%s rows_after_upsert=%d", ticker, len(rows))
                succeeded.add(ticker)
            except Exception as e:
                logger.error("[process] ticker=%s exception: %s", ticker, e)
                failed.add(ticker)
    finally:
        try:
            conn.close()
            logger.info("[process] DB connection closed")
        except Exception:
            pass

    logger.warning("[process] TOTAL_API_CALLS this request: %d", api_calls)
    return succeeded, failed


def get_or_refresh_latest(symbol: str) -> dict | None:
    """Return latest for symbol from cache if fresh; else fetch and update.

    Freshness: if last_updated is within 24h, return cached. Otherwise fetch
    via provider (key-rotating), update cache, upsert into stock_data, and
    update history.latest_data_date.
    """
    conn = connect_to_db()
    if not conn:
        logger.error("[latest] DB connection unavailable")
        return None
    try:
        with conn.cursor() as cursor:
            create_latest_table(cursor)
            conn.commit()
        with conn.cursor() as cursor:
            cache = get_latest_cache(cursor, symbol)
        if cache and cache.get("last_updated") is not None:
            last_upd = cache["last_updated"]
            if last_upd.tzinfo is None:
                last_upd = last_upd.replace(tzinfo=timezone.utc)
            age_sec = (datetime.now(timezone.utc) - last_upd).total_seconds()
            if age_sec < 86400:
                logger.info("[latest] cache hit symbol=%s age_hours=%.2f", symbol, age_sec/3600)
                return cache.get("data")

        # Need to fetch
        logger.info("[latest] cache miss/stale for %s; fetching", symbol)
        latest_map = fetch_latest_data_marketstack([symbol])
        item = (latest_map or {}).get(symbol)
        if not item:
            logger.warning("[latest] provider returned no latest for %s", symbol)
            return cache.get("data") if cache else None

        # Upsert into DB: stock_data and history, and update latest cache
        with conn.cursor() as cursor:
            create_stock_data_table(cursor)
            insert_stock_data(cursor, symbol, [item])
            latest_date = (item.get("date") or item.get("trade_date") or "")[:10]
            if latest_date:
                update_history_latest_date(cursor, symbol, latest_date)
            # write to latest cache
            upsert_latest_cache(cursor, symbol, item)
        conn.commit()
        logger.info("[latest] updated cache and tables for %s", symbol)
        return item
    except Exception as e:
        logger.error("[latest] error for symbol=%s: %s", symbol, e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_latest_value(stop_event: 'threading.Event | None' = None, default_idle_seconds: int = 3600):
    """Background worker that refreshes symbols when they are due.

    Uses the provider's /eod/latest endpoint to fetch only the latest day per
    symbol, honoring the 24h cooldown. This keeps the worker simple and avoids
    complex window logic. Gaps spanning multiple days are expected to be filled
    by on-demand calls to process_tickers when the API is queried for a range.
    """
    logger.info("[worker] get_latest_value started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            entries = []
            conn = connect_to_db()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        create_history_table(cursor)
                        conn.commit()
                        entries = list_history_entries(cursor)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

            if not entries:
                logger.info("[worker] no history entries; sleeping %ds", default_idle_seconds)
                if stop_event and stop_event.wait(default_idle_seconds):
                    logger.info("[worker] stop requested; exiting")
                    return
                continue

            due_symbols = []
            earliest_next_due: datetime | None = None
            for row in entries:
                sym = row.get("symbol")
                last_tried = row.get("last_trried_to_fetch_date") or row.get("last_tried_to_fetch_date")
                if last_tried is not None and getattr(last_tried, "tzinfo", None) is None:
                    last_tried = last_tried.replace(tzinfo=timezone.utc)
                if last_tried is None or (now_utc - last_tried).total_seconds() >= 86400:
                    due_symbols.append(sym)
                else:
                    next_due = last_tried + timedelta(seconds=86400)
                    if earliest_next_due is None or next_due < earliest_next_due:
                        earliest_next_due = next_due

            if due_symbols:
                logger.info("[worker] due_symbols=%s", due_symbols)
                for sym in due_symbols:
                    # Touch last_tried before attempting
                    try:
                        c = connect_to_db()
                        if c:
                            try:
                                with c.cursor() as cursor:
                                    touch_history_last_tried(cursor, sym)
                                c.commit()
                            finally:
                                try:
                                    c.close()
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.error("[worker] error touching last_tried for %s: %s", sym, e)

                    # Fetch latest via provider and upsert minimal state
                    try:
                        latest_map = fetch_latest_data_marketstack([sym])
                        item = (latest_map or {}).get(sym)
                        if item:
                            c2 = connect_to_db()
                            if c2:
                                try:
                                    with c2.cursor() as cursor:
                                        create_stock_data_table(cursor)
                                        insert_stock_data(cursor, sym, [item])
                                        latest_date = (item.get("date") or item.get("trade_date") or "")[:10]
                                        if latest_date:
                                            update_history_latest_date(cursor, sym, latest_date)
                                    c2.commit()
                                finally:
                                    try:
                                        c2.close()
                                    except Exception:
                                        pass
                    except Exception as e:
                        logger.error("[worker] symbol=%s exception during latest refresh: %s", sym, e)
                    # small delay to avoid hammering
                    time.sleep(0.2)
                # After processing due items, continue to recompute schedule immediately
                continue

            # If nothing is due, compute sleep until earliest next due
            sleep_s = default_idle_seconds if earliest_next_due is None else max(1, int((earliest_next_due - now_utc).total_seconds()))
            logger.info("[worker] nothing due; next wake in %ds", sleep_s)
            if stop_event and stop_event.wait(sleep_s):
                logger.info("[worker] stop requested; exiting")
                return
        except Exception as e:
            logger.error("[worker] loop exception: %s", e)
            # backoff a bit on unexpected error
            if stop_event and stop_event.wait(5):
                logger.info("[worker] stop requested during error backoff; exiting")
                return


def refresh_tracked_symbol(symbol: str) -> dict | None:
    """For a tracked symbol, check 24h since last request; if due, fetch latest and upsert.

    - Ensures history exists.
    - Uses latest cache policy to avoid redundant provider calls.
    - If last_tried_to_fetch_date < 24h, returns cached/latest without provider call.
    """
    sym = symbol.strip().upper()
    if not sym:
        return None
    conn = connect_to_db()
    if not conn:
        logger.error("[track] DB unavailable")
        return None
    try:
        with conn.cursor() as cursor:
            create_history_table(cursor)
            create_latest_table(cursor)
            create_tracked_symbols_table(cursor)
            ensure_history_symbol(cursor, sym)
        conn.commit()

        # Check cooldown
        with conn.cursor() as cursor:
            hist = get_history(cursor, sym)
            cache = get_latest_cache(cursor, sym)
        last_tried = hist.get("last_tried_to_fetch_date") if hist else None
        if last_tried is not None and getattr(last_tried, "tzinfo", None) is None:
            last_tried = last_tried.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if last_tried and (now_utc - last_tried).total_seconds() < 86400:
            logger.info("[track] cooldown active for %s; returning cached if available", sym)
            return cache.get("data") if cache else None

        # Touch last_tried and fetch latest (cache-aware helper)
        with conn.cursor() as cursor:
            touch_history_last_tried(cursor, sym)
        conn.commit()
        item = get_or_refresh_latest(sym)
        # Update tracked last_fetched to this time since we performed a refresh
        try:
            with conn.cursor() as cursor:
                set_tracked_last_fetched(cursor, sym)
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.warning("[track] failed to update last_fetched for %s: %s", sym, e)
        return item
    except Exception as e:
        logger.error("[track] error refreshing %s: %s", sym, e)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def track_symbol(symbol: str) -> bool:
    """Mark a symbol as tracked (special symbol). Idempotent."""
    sym = symbol.strip().upper()
    if not sym:
        return False
    conn = connect_to_db()
    if not conn:
        return False
    try:
        with conn.cursor() as cursor:
            create_tracked_symbols_table(cursor)
            add_tracked_symbol(cursor, sym)
            create_history_table(cursor)
            ensure_history_symbol(cursor, sym)
        conn.commit()
        return True
    except Exception as e:
        logger.error("[track] track_symbol failed for %s: %s", sym, e)
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


def untrack_symbol(symbol: str) -> bool:
    """Unmark a symbol from tracked list."""
    sym = symbol.strip().upper()
    if not sym:
        return False
    conn = connect_to_db()
    if not conn:
        return False
    try:
        with conn.cursor() as cursor:
            create_tracked_symbols_table(cursor)
            removed = remove_tracked_symbol(cursor, sym)
        conn.commit()
        return removed
    except Exception as e:
        logger.error("[track] untrack_symbol failed for %s: %s", sym, e)
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


def run_tracked_refresher(stop_event: 'threading.Event | None' = None, default_idle_seconds: int = 900):
    """Background loop to refresh tracked symbols using last_fetched scheduling.

    - Reads tracked symbols and their last_fetched timestamps.
    - If last_fetched is None or older than 24h, refresh the symbol via cache-aware latest fetch.
    - After processing a due symbol, updates tracked_symbols.last_fetched to NOW().
    - Sleeps until the earliest next due time across all tracked symbols (or default idle).
    """
    logger.info("[track-worker] started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            tracked: list[dict] = []
            conn = connect_to_db()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        create_tracked_symbols_table(cursor)
                        conn.commit()
                        tracked = list_tracked_symbols_with_meta(cursor)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

            if not tracked:
                sleep_s = default_idle_seconds
                logger.info("[track-worker] no tracked symbols; sleeping %ds", sleep_s)
                if stop_event and stop_event.wait(sleep_s):
                    logger.info("[track-worker] stop requested; exiting")
                    return
                continue

            due_syms: list[str] = []
            earliest_next_due: datetime | None = None
            for row in tracked:
                sym = row.get("symbol")
                lf = row.get("last_fetched")
                if lf is not None and getattr(lf, "tzinfo", None) is None:
                    lf = lf.replace(tzinfo=timezone.utc)
                if lf is None or (now_utc - lf).total_seconds() >= 86400:
                    due_syms.append(sym)
                else:
                    next_due = lf + timedelta(seconds=86400)
                    if earliest_next_due is None or next_due < earliest_next_due:
                        earliest_next_due = next_due

            if due_syms:
                logger.info("[track-worker] due=%s", due_syms)
                for sym in due_syms:
                    try:
                        # Use cache-aware latest retrieval; this may or may not hit provider
                        _ = get_or_refresh_latest(sym)
                        # Update last_fetched to schedule next check
                        c2 = connect_to_db()
                        if c2:
                            try:
                                with c2.cursor() as cursor:
                                    set_tracked_last_fetched(cursor, sym)
                                c2.commit()
                            except Exception as e2:
                                try:
                                    c2.rollback()
                                except Exception:
                                    pass
                                logger.error("[track-worker] failed updating last_fetched for %s: %s", sym, e2)
                            finally:
                                try:
                                    c2.close()
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.error("[track-worker] error refreshing %s: %s", sym, e)
                    time.sleep(0.2)
                # After processing due items, continue to recompute schedule immediately
                continue

            # If nothing is due, compute sleep until earliest next due
            sleep_s = default_idle_seconds if earliest_next_due is None else max(1, int((earliest_next_due - now_utc).total_seconds()))
            logger.info("[track-worker] nothing due; next wake in %ds", sleep_s)
            if stop_event and stop_event.wait(sleep_s):
                logger.info("[track-worker] stop requested; exiting")
                return
        except Exception as e:
            logger.error("[track-worker] loop exception: %s", e)
            # backoff a bit on unexpected error
            if stop_event and stop_event.wait(5):
                logger.info("[track-worker] stop requested during error backoff; exiting")
                return


