"""FastAPI application exposing stock data and tracking endpoints.

Endpoints
- GET /stocks: Ensure data for symbols and date range, then return DB rows.
- GET /latest: Return latest EOD for a symbol (cache-first, 24h TTL).
- GET /api-key-history: Show API key index usage and limit timestamps.
- POST /track, POST /untrack: Manage tracked symbols.
- GET /track/status, GET /track/list: Tracking utilities.

Startup/shutdown use FastAPI lifespan to initialize schema and run a background
refresher for tracked symbols.
"""

from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel
import time
import logging
import sys
from datetime import datetime, timezone
import threading
from contextlib import asynccontextmanager

from .fetch_from_db import (
    get_stock_data,
    list_api_key_history,
    upsert_api_keys_history,
)
from .fetch_data import process_tickers, get_or_refresh_latest, fetch_latest_data_marketstack, track_symbol, untrack_symbol, refresh_tracked_symbol, run_tracked_refresher
from .fetch_from_db import (
    connect_to_db,
    insert_stock_data,
    update_history_latest_date,
    get_latest_cache,
    upsert_latest_cache,
    list_tracked_symbols,
    list_tracked_symbols_with_meta,
)
from .db_schema import create_api_key_history_table, create_latest_table, create_stock_data_table, create_tracked_symbols_table
from .config import get_marketstack_api_keys
from .db_init import init_database

logger = logging.getLogger(__name__)


def setup_app_logging():
    """Configure root and package loggers for the API process."""
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Ensure a stream handler to stdout exists
    has_stream = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
    if not has_stream:
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    # Ensure our modules log at INFO
    for name in (
        "stock_pipeline",
        "stock_pipeline.fetch_data",
        "stock_pipeline.fetch_from_db",
        __name__,
    ):
        logging.getLogger(name).setLevel(logging.INFO)


def _get_client_id(request: Request) -> str:
    """Return a client identifier for logs.

    Prefers X-Client-Id or X-Request-Id header, else falls back to client IP.
    """
    # Prefer explicit header, else fall back to client IP
    try:
        hdr = request.headers.get("X-Client-Id") or request.headers.get("X-Request-Id")
    except Exception:
        hdr = None
    if hdr:
        return hdr
    try:
        return getattr(request.client, "host", None) or "unknown"
    except Exception:
        return "unknown"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown manager to configure logging, init DB, and run worker."""
    # Startup
    setup_app_logging()
    logger.info("[api] logging configured")
    try:
        ok = init_database()
        logger.info("[api] database init ok=%s", ok)
    except Exception as e:
        logger.error("[api] database init error: %s", e)
    try:
        stop_flag_attr = "_tracked_worker_stop_event"
        if not hasattr(app.state, stop_flag_attr):
            app.state._tracked_worker_stop_event = threading.Event()
        t = threading.Thread(
            target=run_tracked_refresher,
            kwargs={"stop_event": app.state._tracked_worker_stop_event},
            daemon=True,
        )
        t.start()
        app.state._tracked_worker_thread = t
        logger.info("[api] tracked refresher started")
    except Exception as e:
        logger.error("[api] failed to start tracked refresher: %s", e)
    # Yield control to the app
    try:
        yield
    finally:
        # Shutdown
        try:
            ev = getattr(app.state, "_tracked_worker_stop_event", None)
            th = getattr(app.state, "_tracked_worker_thread", None)
            if ev:
                ev.set()
            if th and th.is_alive():
                th.join(timeout=5)
                logger.info("[api] tracked refresher stopped")
        except Exception as e:
            logger.error("[api] error stopping tracked refresher: %s", e)


app = FastAPI(title="Stock Data API", lifespan=lifespan)


class StockQueryResponse(BaseModel):
    """Response model for /stocks rows."""
    symbol: str
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None


@app.get("/stocks", response_model=Dict[str, List[StockQueryResponse]])
async def get_stocks(
    request: Request,
    symbols: str = Query(..., description="Comma separated symbols"),
    start_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
):
    """Ensure data exists for the requested symbols and return rows from DB.

    Parameters
    ----------
    symbols : str
        Comma-separated list of tickers.
    start_date, end_date : str
        Inclusive YYYY-MM-DD dates.
    """
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] GET /stocks start cid=%s symbols=%s start_date=%s end_date=%s", cid, symbols, start_date, end_date)
    # Normalize parameter names
    s_date = start_date
    e_date = end_date
    if not s_date or not e_date:
        raise HTTPException(status_code=400, detail="Missing start_date/end_date")

    logger.info("[api] request symbols=%s start_date=%s end_date=%s", symbols, s_date, e_date)
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    logger.info("[api] parsed_symbols=%s", syms)
    if not syms:
        raise HTTPException(status_code=400, detail="No symbols provided")

    # Ensure data exists (fetch only if missing)
    try:
        logger.info("[api] calling process_tickers() to ensure data...")
        _succeeded, _failed = process_tickers(syms, s_date, e_date)
        logger.info("[api] process_tickers done succeeded=%s failed=%s", sorted(list(_succeeded)), sorted(list(_failed)))
    except Exception as e:
        logger.exception("[api] ensure/process error: %s", e)

    result: Dict[str, List[dict]] = {}
    for sym in syms:
        rows = get_stock_data(sym, s_date, e_date)
        logger.info("[api] DB rows for %s: %d", sym, len(rows))
        result[sym] = rows  # empty list if none
    logger.info("[api] total_duration_sec=%.3f", (time.time()-t0))
    logger.info("[api] GET /stocks end cid=%s duration=%.3fs symbols=%d", cid, (time.time()-t0), len(syms))
    return result


@app.get("/")
async def health(request: Request):
    """Health endpoint that also logs a client id for traceability."""
    logger.info("[api] GET / healthcheck cid=%s", request.headers.get("X-Client-Id") or (getattr(request.client, 'host', None) or 'unknown'))
    return {"status": "ok"}


class LatestResponse(BaseModel):
    """Response model for /latest."""
    symbol: str
    data: dict | None
    cached: bool


@app.get("/latest")
async def get_latest(
    request: Request,
    symbol: str = Query(..., description="Single symbol, e.g. AAPL"),
):
    """Return latest EOD (cache-first; fetch and upsert if stale)."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] GET /latest start cid=%s symbol=%s", cid, symbol)
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    # Cache-first: return cached if last_updated < 24h
    conn = connect_to_db()
    cache: Optional[dict] = None
    last_upd = None
    if conn:
        try:
            with conn.cursor() as cursor:
                create_latest_table(cursor)
                row = get_latest_cache(cursor, sym)
                if row:
                    cache = row.get("data")
                    last_upd = row.get("last_updated")
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

    if last_upd is not None:
        if getattr(last_upd, "tzinfo", None) is None:
            last_upd = last_upd.replace(tzinfo=timezone.utc)
        age_sec = (datetime.now(timezone.utc) - last_upd).total_seconds()
        if age_sec < 86400:
            logger.info("[api] GET /latest cache-hit cid=%s symbol=%s age=%.1fs duration=%.3fs", cid, sym, age_sec, (time.time()-t0))
            return {"symbol": sym, "data": cache, "cached": True}

    # Stale or no cache: fetch from provider and upsert
    try:
        latest_map = fetch_latest_data_marketstack([sym])
        item = (latest_map or {}).get(sym)
    except Exception as e:
        logger.error("[api] latest fetch error for %s: %s", sym, e)
        item = None

    if item is None:
        # nothing from provider; return (stale) cache if present
        logger.info("[api] GET /latest provider-miss cid=%s symbol=%s used_cache=%s duration=%.3fs", request.headers.get("X-Client-Id") or (getattr(request.client, 'host', None) or 'unknown'), sym, bool(cache), (time.time()-t0))
        return {"symbol": sym, "data": cache, "cached": bool(cache)}

    # Upsert into DB (stock_data/history/latest cache)
    conn = connect_to_db()
    if not conn:
        logger.info("[api] GET /latest no-db cid=%s symbol=%s duration=%.3fs", cid, sym, (time.time()-t0))
        return {"symbol": sym, "data": item, "cached": False}
    try:
        with conn.cursor() as cursor:
            create_stock_data_table(cursor)
            insert_stock_data(cursor, sym, [item])
            latest_date = (item.get("date") or item.get("trade_date") or "")[:10]
            if latest_date:
                update_history_latest_date(cursor, sym, latest_date)
            create_latest_table(cursor)
            upsert_latest_cache(cursor, sym, item)
        conn.commit()
    except Exception as e:
        logger.error("[api] latest DB upsert error for %s: %s", sym, e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    logger.info("[api] GET /latest provider-hit cid=%s symbol=%s duration=%.3fs", request.headers.get("X-Client-Id") or (getattr(request.client, 'host', None) or 'unknown'), sym, (time.time()-t0))
    return {"symbol": sym, "data": item, "cached": False}


@app.get("/api-key-history")
async def get_api_key_history(request: Request):
    """Return usage metadata for API key indices (no actual keys)."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] GET /api-key-history start cid=%s", cid)
    keys = get_marketstack_api_keys()
    conn = connect_to_db()
    if not conn:
        logger.error("[api] GET /api-key-history DB unavailable cid=%s", cid)
        raise HTTPException(status_code=500, detail="DB unavailable")
    try:
        with conn.cursor() as cursor:
            create_api_key_history_table(cursor)
            upsert_api_keys_history(cursor, keys)
            rows = list_api_key_history(cursor)
        conn.commit()
        logger.info("[api] GET /api-key-history end cid=%s duration=%.3fs keys=%d rows=%d", cid, (time.time()-t0), len(keys), len(rows))
        return {"count": len(keys), "history": rows}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error reading api_key_history: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/track")
async def track(request: Request, symbol: str = Query(..., description="Symbol to start tracking")):
    """Add a symbol to the tracked list and optionally refresh immediately."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] POST /track start cid=%s symbol=%s", cid, symbol)
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    ok = track_symbol(sym)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to track symbol")
    # Optionally refresh immediately (respects cooldown)
    data = refresh_tracked_symbol(sym)
    logger.info("[api] POST /track end cid=%s symbol=%s fetched=%s duration=%.3fs", cid, sym, bool(data), (time.time()-t0))
    # last_fetched is already updated inside refresh_tracked_symbol when it fetches
    return {"symbol": sym, "tracked": True, "data": data}


@app.post("/untrack")
async def untrack(request: Request, symbol: str = Query(..., description="Symbol to stop tracking")):
    """Remove a symbol from the tracked list (idempotent)."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] POST /untrack start cid=%s symbol=%s", cid, symbol)
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    conn = connect_to_db()
    current = False
    if conn:
        try:
            with conn.cursor() as cursor:
                create_tracked_symbols_table(cursor)
                current = sym in list_tracked_symbols(cursor)
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
    removed = untrack_symbol(sym)
    logger.info("[api] POST /untrack end cid=%s symbol=%s removed=%s was_tracked=%s duration=%.3fs", cid, sym, bool(removed), bool(current), (time.time()-t0))
    if not removed and not current:
        # was not tracked; treat as success (idempotent)
        return {"symbol": sym, "tracked": False, "removed": False}
    return {"symbol": sym, "tracked": False, "removed": bool(removed)}


@app.get("/track/status")
async def track_status(request: Request, symbol: str = Query(..., description="Tracked symbol to check/refresh")):
    """Show last_fetched and whether cooldown/cached state applies for a symbol."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] GET /track/status start cid=%s symbol=%s", cid, symbol)
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    conn = connect_to_db()
    cached = False
    data = None
    if conn:
        try:
            with conn.cursor() as cursor:
                create_tracked_symbols_table(cursor)
                row = next((r for r in list_tracked_symbols_with_meta(cursor) if r["symbol"] == sym), None)
                if row:
                    data = row.get("last_fetched")
                    if data is not None and getattr(data, "tzinfo", None) is None:
                        data = data.replace(tzinfo=timezone.utc)
                    now_utc = datetime.now(timezone.utc)
                    delta = 86400 - (now_utc - data).total_seconds() if data else 0
                    cached = delta > 0
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
    logger.info("[api] GET /track/status end cid=%s symbol=%s cached_or_cooldown=%s duration=%.3fs", cid, sym, cached, (time.time()-t0))
    return {"symbol": sym, "data": data, "cached_or_cooldown": cached}


@app.get("/track/list")
async def track_list(request: Request):
    """List all tracked symbols with last_fetched and next due time in seconds."""
    t0 = time.time()
    logger.info("[api] GET /track/list start cid=%s", request.headers.get("X-Client-Id") or (getattr(request.client, 'host', None) or 'unknown'))
    conn = connect_to_db()
    if not conn:
        logger.info("[api] GET /track/list no-db duration=%.3fs", (time.time()-t0))
        return {"tracked": []}
    try:
        with conn.cursor() as cursor:
            create_tracked_symbols_table(cursor)
            rows = list_tracked_symbols_with_meta(cursor)
        conn.commit()
        # compute next_due for each row
        out = []
        now_utc = datetime.now(timezone.utc)
        for r in rows:
            lf = r.get("last_fetched")
            if lf is not None and getattr(lf, "tzinfo", None) is None:
                lf = lf.replace(tzinfo=timezone.utc)
            if lf is None:
                next_due = "now"
                seconds = 0
            else:
                delta = 86400 - (now_utc - lf).total_seconds()
                seconds = int(delta) if delta > 0 else 0
                next_due = seconds
            out.append({"symbol": r["symbol"], "last_fetched": r.get("last_fetched"), "seconds_until_next_fetch": next_due})
        logger.info("[api] GET /track/list end cid=%s duration=%.3fs count=%d", request.headers.get("X-Client-Id") or (getattr(request.client, 'host', None) or 'unknown'), (time.time()-t0), len(out))
        return {"tracked": out}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error listing tracked symbols: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
