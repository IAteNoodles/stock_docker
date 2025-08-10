"""FastAPI application exposing stock data and tracking endpoints.

Endpoints
- GET /stocks: Ensure data for symbols and date range, then return DB rows.
- GET /latest: Return latest EOD for a symbol (cache-first, 24h TTL).
- GET /api-key-history: Show API key index usage and limit timestamps.

Startup/shutdown use FastAPI lifespan to initialize schema.
"""

from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel
import time
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import asyncio

from .fetch_from_db import (
    get_stock_data,
    list_api_key_history,
    upsert_api_keys_history,
)
from .fetch_data import process_tickers, get_or_refresh_latest
from .fetch_from_db import (
    connect_to_db,
    get_latest_cache,
)
from .db_schema import create_api_key_history_table, create_latest_table
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
    """Startup/shutdown manager to configure logging and init DB (no tracking worker)."""
    # Startup
    setup_app_logging()
    logger.info("[api] logging configured")
    try:
        ok = init_database()
        logger.info("[api] database init ok=%s", ok)
    except Exception as e:
        logger.error("[api] database init error: %s", e)
    # Yield control to the app
    try:
        yield
    finally:
        # No tracking worker to stop anymore
        pass


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


# Global executor for background tasks
background_executor = ThreadPoolExecutor(max_workers=8)
_in_progress_refreshes = set()
_in_progress_lock = threading.Lock()


def schedule_refresh(symbol: str):
    sym = symbol.strip().upper()
    if not sym:
        return
    with _in_progress_lock:
        if sym in _in_progress_refreshes:
            return
        _in_progress_refreshes.add(sym)

    def refresh_task():
        try:
            get_or_refresh_latest(sym)
        finally:
            with _in_progress_lock:
                _in_progress_refreshes.discard(sym)

    background_executor.submit(refresh_task)


async def run_in_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


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
        _succeeded, _failed = await run_in_thread(process_tickers, syms, s_date, e_date)
        logger.info("[api] process_tickers done succeeded=%s failed=%s", sorted(list(_succeeded)), sorted(list(_failed)))
    except Exception as e:
        logger.exception("[api] ensure/process error: %s", e)

    result: Dict[str, List[dict]] = {}
    for sym in syms:
        rows = await run_in_thread(get_stock_data, sym, s_date, e_date)
        if not rows:
            schedule_refresh(sym)
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
    conn = await run_in_thread(connect_to_db)
    cache: Optional[dict] = None
    last_upd = None
    if conn:
        try:
            def get_cache():
                with conn.cursor() as cursor:
                    create_latest_table(cursor)
                    row = get_latest_cache(cursor, sym)
                    if row:
                        return row.get("data"), row.get("last_updated")
                conn.commit()
                return None, None
            cache, last_upd = await run_in_thread(get_cache)
        except Exception:
            try:
                await run_in_thread(conn.rollback)
            except Exception:
                pass
        finally:
            try:
                await run_in_thread(conn.close)
            except Exception:
                pass
    age_sec = None
    if last_upd is not None:
        if getattr(last_upd, "tzinfo", None) is None:
            last_upd = last_upd.replace(tzinfo=timezone.utc)
        age_sec = (datetime.now(timezone.utc) - last_upd).total_seconds()
        if age_sec < 86400:
            logger.info("[api] GET /latest cache-hit cid=%s symbol=%s age=%.1fs duration=%.3fs", cid, sym, age_sec, (time.time()-t0))
            return {"symbol": sym, "data": cache, "cached": True}

    # If we reach here, cache is missing or stale: fetch synchronously
    try:
        data = await run_in_thread(get_or_refresh_latest, sym)
        if data is not None:
            logger.info("[api] GET /latest fetched-now cid=%s symbol=%s duration=%.3fs", cid, sym, (time.time()-t0))
            return {"symbol": sym, "data": data, "cached": False}
        else:
            logger.info("[api] GET /latest no-data-after-fetch cid=%s symbol=%s duration=%.3fs", cid, sym, (time.time()-t0))
            return {"symbol": sym, "data": cache, "cached": bool(cache)}
    except Exception as e:
        logger.exception("[api] GET /latest error cid=%s symbol=%s err=%s", cid, sym, e)
        # Fall back to any cache we had
        return {"symbol": sym, "data": cache, "cached": bool(cache)}


@app.get("/api-key-history")
async def get_api_key_history(request: Request):
    """Return usage metadata for API key indices (no actual keys)."""
    t0 = time.time()
    cid = _get_client_id(request)
    logger.info("[api] GET /api-key-history start cid=%s", cid)
    keys = get_marketstack_api_keys()
    conn = await run_in_thread(connect_to_db)
    if not conn:
        logger.error("[api] GET /api-key-history DB unavailable cid=%s", cid)
        raise HTTPException(status_code=500, detail="DB unavailable")
    try:
        def get_history():
            with conn.cursor() as cursor:
                create_api_key_history_table(cursor)
                upsert_api_keys_history(cursor, keys)
                rows = list_api_key_history(cursor)
            conn.commit()
            return rows
        rows = await run_in_thread(get_history)
        logger.info("[api] GET /api-key-history end cid=%s duration=%.3fs keys=%d rows=%d", cid, (time.time()-t0), len(keys), len(rows))
        return {"count": len(keys), "history": rows}
    except Exception as e:
        try:
            await run_in_thread(conn.rollback)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error reading api_key_history: {e}")
    finally:
        try:
            await run_in_thread(conn.close)
        except Exception:
            pass