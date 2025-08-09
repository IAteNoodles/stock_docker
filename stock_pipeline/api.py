from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import time
import logging
import sys
from datetime import datetime, timezone
import threading

from .fetch_from_db import (
    get_stock_data,
    list_api_key_history,
    create_api_key_history_table,
    upsert_api_keys_history,
)
from .fetch_data import process_tickers, get_or_refresh_latest, fetch_latest_data_marketstack, track_symbol, untrack_symbol, refresh_tracked_symbol, run_tracked_refresher
from .fetch_from_db import (
    connect_to_db,
    create_stock_data_table,
    insert_stock_data,
    update_history_latest_date,
    create_latest_table,
    get_latest_cache,
    upsert_latest_cache,
    create_tracked_symbols_table,
    list_tracked_symbols,
    list_tracked_symbols_with_meta,
)
from .config import get_marketstack_api_keys

app = FastAPI(title="Stock Data API")

logger = logging.getLogger(__name__)


def setup_app_logging():
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


@app.on_event("startup")
async def on_startup():
    setup_app_logging()
    logger.info("[api] logging configured")
    # Start tracked refresher in a background thread
    try:
        stop_flag_attr = "_tracked_worker_stop_event"
        if not hasattr(app.state, stop_flag_attr):
            app.state._tracked_worker_stop_event = threading.Event()
        t = threading.Thread(target=run_tracked_refresher, kwargs={"stop_event": app.state._tracked_worker_stop_event}, daemon=True)
        t.start()
        app.state._tracked_worker_thread = t
        logger.info("[api] tracked refresher started")
    except Exception as e:
        logger.error("[api] failed to start tracked refresher: %s", e)


class StockQueryResponse(BaseModel):
    symbol: str
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None


@app.get("/stocks", response_model=Dict[str, List[StockQueryResponse]])
async def get_stocks(
    symbols: str = Query(..., description="Comma separated symbols"),
    start_date: str | None = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
):
    t0 = time.time()
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
    return result


@app.get("/")
async def health():
    return {"status": "ok"}


class LatestResponse(BaseModel):
    symbol: str
    data: dict | None
    cached: bool


@app.get("/latest")
async def get_latest(
    symbol: str = Query(..., description="Single symbol, e.g. AAPL"),
):
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
        return {"symbol": sym, "data": cache, "cached": bool(cache)}

    # Upsert into DB (stock_data/history/latest cache)
    conn = connect_to_db()
    if not conn:
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
    return {"symbol": sym, "data": item, "cached": False}


@app.get("/api-key-history")
async def get_api_key_history():
    keys = get_marketstack_api_keys()
    conn = connect_to_db()
    if not conn:
        raise HTTPException(status_code=500, detail="DB unavailable")
    try:
        with conn.cursor() as cursor:
            create_api_key_history_table(cursor)
            upsert_api_keys_history(cursor, keys)
            rows = list_api_key_history(cursor)
        conn.commit()
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
async def track(symbol: str = Query(..., description="Symbol to start tracking")):
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    ok = track_symbol(sym)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to track symbol")
    # Optionally refresh immediately (respects cooldown)
    data = refresh_tracked_symbol(sym)
    # last_fetched is already updated inside refresh_tracked_symbol when it fetches
    return {"symbol": sym, "tracked": True, "data": data}


@app.post("/untrack")
async def untrack(symbol: str = Query(..., description="Symbol to stop tracking")):
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
    if not removed and not current:
        # was not tracked; treat as success (idempotent)
        return {"symbol": sym, "tracked": False, "removed": False}
    return {"symbol": sym, "tracked": False, "removed": bool(removed)}


@app.get("/track/status")
async def track_status(symbol: str = Query(..., description="Tracked symbol to check/refresh")):
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    # Ensure it's tracked
    conn = connect_to_db()
    is_tracked = False
    if conn:
        try:
            with conn.cursor() as cursor:
                create_tracked_symbols_table(cursor)
                is_tracked = sym in list_tracked_symbols(cursor)
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
    if not is_tracked:
        raise HTTPException(status_code=404, detail="Symbol not tracked")

    data = refresh_tracked_symbol(sym)
    cached = data is None
    return {"symbol": sym, "data": data, "cached_or_cooldown": cached}


@app.get("/track/list")
async def track_list():
    conn = connect_to_db()
    if not conn:
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
