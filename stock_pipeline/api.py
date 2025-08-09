from typing import Dict, List
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import time
import logging
import sys

from .fetch_from_db import get_stock_data
from .fetch_data import process_tickers

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


class StockQueryResponse(BaseModel):
    symbol: str
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None


@app.get("/stocks", response_model=Dict[str, List[StockQueryResponse]])
async def get_stocks(symbols: str = Query(..., description="Comma separated symbols"),
                     start: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
                     end: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$")):
    t0 = time.time()
    logger.info("[api] request symbols=%s start=%s end=%s", symbols, start, end)
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    logger.info("[api] parsed_symbols=%s", syms)
    if not syms:
        raise HTTPException(status_code=400, detail="No symbols provided")

    # Ensure data exists (fetch only if missing)
    try:
        logger.info("[api] calling process_tickers() to ensure data...")
        _succeeded, _failed = process_tickers(syms, start, end)
        logger.info("[api] process_tickers done succeeded=%s failed=%s", sorted(list(_succeeded)), sorted(list(_failed)))
    except Exception as e:
        logger.exception("[api] ensure/process error: %s", e)

    result: Dict[str, List[dict]] = {}
    for sym in syms:
        rows = get_stock_data(sym, start, end)
        logger.info("[api] DB rows for %s: %d", sym, len(rows))
        result[sym] = rows  # empty list if none
    logger.info("[api] total_duration_sec=%.3f", (time.time()-t0))
    return result


@app.get("/")
async def health():
    return {"status": "ok"}
