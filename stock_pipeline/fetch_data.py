import os
import requests
import json
import time
from datetime import datetime, timedelta, date, timezone
import logging
import threading

from dotenv import load_dotenv
from .fetch_from_db import (
    connect_to_db,
    create_stock_data_table,
    insert_stock_data,
    get_stock_data,
    get_existing_dates,
    create_history_table,
    ensure_history_symbol,
    get_history,
    update_history_latest_date,
    touch_history_last_tried,
    get_max_trade_date_for_symbol,
    list_history_entries,
)

# Load .env from this package directory to work regardless of CWD
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

logger = logging.getLogger(__name__)


def _env(name: str, default: str | None = None) -> str | None:
    """Return a sanitized environment variable value.

    Parameters
    ----------
    name : str
        Name of the environment variable.
    default : str | None
        Default value if the variable is missing.

    Returns
    -------
    str | None
        Trimmed value or the default when absent.
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


API_KEY = _env("MARKETSTACK_API_KEY")


def fetch_historical_data_marketstack(tickers, start_date, end_date):
    """Fetch historical daily stock data from Marketstack.

    Parameters
    ----------
    tickers : list[str]
        List of ticker symbols, e.g. ["AAPL", "MSFT"].
    start_date : str
        Inclusive start date in YYYY-MM-DD.
    end_date : str
        Inclusive end date in YYYY-MM-DD.

    Returns
    -------
    dict[str, list[dict]]
        Mapping from ticker to raw item dicts as returned by the API. Missing
        fields are left to downstream handling; network/parse errors are logged
        and that ticker returns an empty list.
    """
    if not API_KEY:
        logger.warning("MARKETSTACK_API_KEY not found. Skipping API fetch.")
        return {t: [] for t in tickers}

    base_url = "http://api.marketstack.com/v2/eod"
    all_data = {}

    for ticker in tickers:
        logger.info("[api-fetch] ticker=%s window=(%s..%s)", ticker, start_date, end_date)
        collected = []
        offset = 0
        page_size = 1000  # fetch up to 1000 per HTTP request to minimize calls
        max_rows = 1000

        while len(collected) < max_rows:
            params = {
                "access_key": API_KEY,
                "symbols": ticker,
                "date_from": start_date,
                "date_to": end_date,
                "limit": page_size,
                "offset": offset,
                "sort": "ASC",
            }
            try:
                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and data.get("error"):
                    logger.error("[api-fetch] provider error ticker=%s err=%s", ticker, data.get("error"))
                    break
                items = data.get("data", []) if isinstance(data, dict) else []
                if not isinstance(items, list):
                    logger.error("[api-fetch] unexpected payload format for ticker=%s", ticker)
                    break
                if not items:
                    break
                # Only keep dict items; non-dicts are ignored with a warning
                valid_items = []
                for it in items:
                    if isinstance(it, dict):
                        valid_items.append(it)
                    else:
                        logger.error("[api-fetch] non-dict item ignored ticker=%s item=%s", ticker, it)
                collected.extend(valid_items)
                # Stop if fewer than a full page returned
                if len(items) < page_size:
                    break
                offset += page_size
                # small delay to be polite to the API
                time.sleep(0.25)
            except requests.exceptions.RequestException as e:
                logger.error("[api-fetch] request error ticker=%s err=%s", ticker, e)
                break
            except json.JSONDecodeError as e:
                logger.error("[api-fetch] json decode error ticker=%s err=%s", ticker, e)
                break
            except Exception as e:
                logger.error("[api-fetch] unexpected error ticker=%s err=%s", ticker, e)
                break

        logger.info("[api-fetch] ticker=%s collected=%d", ticker, len(collected))
        all_data[ticker] = collected[:max_rows]

        # polite delay between tickers
        time.sleep(0.5)

    return all_data


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

    Parameters
    ----------
    tickers : list[str]
        Symbols to process.
    start_date : str
        Inclusive start date (YYYY-MM-DD).
    end_date : str
        Inclusive end date (YYYY-MM-DD).

    Returns
    -------
    tuple[set[str], set[str]]
        (succeeded_symbols, failed_symbols)
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

        # Decide windowing strategy once per request (okay for our use-case)
        need = _dates_set(start_date, end_date)
        use_broad_window = len(need) < 1000 and _range_within_last_1000(start_date, end_date)
        broad_from, broad_to = _last_1000_window() if use_broad_window else (start_date, end_date)
        logger.info("[process] use_broad_window=%s window=(%s..%s)", use_broad_window, broad_from, broad_to)

        for ticker in tickers:
            try:
                # Ensure history row exists for this symbol
                with conn.cursor() as cursor:
                    ensure_history_symbol(cursor, ticker)
                conn.commit()

                # 0) Check DB first
                rows = get_stock_data(ticker, start_date, end_date)
                logger.info("[process] ticker=%s existing_rows=%d", ticker, len(rows))
                if rows:
                    # Update latest_data_date from DB max(trade_date)
                    with conn.cursor() as cursor:
                        max_db_date = get_max_trade_date_for_symbol(cursor, ticker)
                        if max_db_date:
                            update_history_latest_date(cursor, ticker, max_db_date)
                    conn.commit()
                    logger.info("[process] ticker=%s data present in DB; skip fetch", ticker)
                    succeeded.add(ticker)
                    continue

                # No rows in requested range; enforce 24h backoff using history
                with conn.cursor() as cursor:
                    hist = get_history(cursor, ticker)
                now_utc = datetime.now(timezone.utc)
                last_tried = hist.get("last_tried_to_fetch_date") if hist else None
                if last_tried is not None:
                    # ensure aware comparison
                    if last_tried.tzinfo is None:
                        last_tried = last_tried.replace(tzinfo=timezone.utc)
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

                # 1) Fetch once (broad or exact) -> count as one API call per ticker
                api_calls += 1
                logger.warning(
                    "[process] API_CALL #%d (this request): ticker=%s window=(%s..%s)",
                    api_calls,
                    ticker,
                    broad_from,
                    broad_to,
                )
                api_result = fetch_historical_data_marketstack([ticker], broad_from, broad_to)
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

                # Update history.latest_data_date to max date from fetched records
                try:
                    max_rec_date = max(
                        ( (r.get("date") or r.get("trade_date") or "")[:10] for r in records ),
                        default=None,
                    )
                    if max_rec_date:
                        with conn.cursor() as cursor:
                            update_history_latest_date(cursor, ticker, max_rec_date)
                        conn.commit()
                except Exception as e:
                    logger.warning("[process] ticker=%s unable to compute/update max fetched date: %s", ticker, e)

                # 2) Read-back to return/print
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


def get_latest_value(stop_event: 'threading.Event | None' = None, default_idle_seconds: int = 3600):
    """Background worker that refreshes symbols when they are due.

    Parameters
    ----------
    stop_event : threading.Event | None
        When set, the worker exits promptly after the current sleep/iteration.
    default_idle_seconds : int
        Fallback sleep time when no scheduling information is available.
    """
    logger.info("[worker] get_latest_value started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            # Read history entries
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

            # Partition into due and not-due
            due_symbols = []
            earliest_next_due: datetime | None = None
            for row in entries:
                sym = row.get("symbol")
                last_tried = row.get("last_tried_to_fetch_date")
                # normalize tz-aware for comparison
                if last_tried is not None and last_tried.tzinfo is None:
                    last_tried = last_tried.replace(tzinfo=timezone.utc)
                # If never tried, it's due now
                if last_tried is None or (now_utc - last_tried).total_seconds() >= 86400:
                    due_symbols.append((sym, row))
                else:
                    next_due = last_tried + timedelta(seconds=86400)
                    if earliest_next_due is None or next_due < earliest_next_due:
                        earliest_next_due = next_due

            if due_symbols:
                logger.info("[worker] due_symbols=%s", [s for s, _ in due_symbols])
                # Process one by one
                for sym, row in due_symbols:
                    # Compute fetch window
                    latest_date = row.get("latest_data_date")
                    if latest_date is not None:
                        start_dt = latest_date + timedelta(days=1)
                        start_date = start_dt.strftime("%Y-%m-%d")
                    else:
                        start_date, _ = _last_1000_window()
                    end_date = date.today().strftime("%Y-%m-%d")

                    try:
                        logger.info("[worker] processing symbol=%s window=(%s..%s)", sym, start_date, end_date)
                        process_tickers([sym], start_date, end_date)
                    except Exception as e:
                        logger.error("[worker] symbol=%s exception: %s", sym, e)
                        # on error, just continue; process_tickers touches last_tried itself
                    # small delay to avoid hammering
                    time.sleep(0.2)
                # After processing due items, continue loop to recompute schedule
                continue

            # If nothing is due, compute sleep until earliest next due
            if earliest_next_due is None:
                sleep_s = default_idle_seconds
            else:
                sleep_s = max(1, int((earliest_next_due - now_utc).total_seconds()))
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


def start_latest_value_worker() -> tuple[threading.Thread, threading.Event]:
    """Start the background refresher thread.

    Returns
    -------
    tuple[threading.Thread, threading.Event]
        The started thread and a stop_event to signal shutdown.
    """
    stop_event = threading.Event()
    t = threading.Thread(target=get_latest_value, args=(stop_event,), name="latest-value-worker", daemon=True)
    t.start()
    logger.info("[worker] started background thread name=%s", t.name)
    return t, stop_event


