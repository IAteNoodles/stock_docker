"""
A minimal, self-contained latest-price client for Marketstack /eod/latest.

- Uses v2 (HTTPS) by default.
- Can fall back to v1 (HTTP) if HTTPS fails or when prefer_https=False.
- No DB or other project dependencies. Safe to use directly from Dagster assets.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests

# Use centralized config for API keys
from .config import get_marketstack_api_keys

logger = logging.getLogger(__name__)


def _request_latest(symbols: List[str], base_url: str, api_key: str, timeout: int = 20) -> Dict[str, Optional[dict]]:
    # Normalize symbols
    symbols = [s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()]
    if not symbols:
        return {}

    params = {
        "access_key": api_key,
        "symbols": ",".join(symbols),
    }
    try:
        resp = requests.get(base_url, params=params, timeout=timeout)
    except Exception as e:
        logger.error("[simple-latest] request error url=%s err=%s", base_url, e)
        return {s: None for s in symbols}

    try:
        status = int(getattr(resp, "status_code", 0))
    except Exception:
        status = 0

    if status >= 400 or status == 0:
        body = None
        try:
            body = resp.text[:300]
        except Exception:
            body = None
        logger.error("[simple-latest] HTTP %s url=%s body=%s", status, base_url, body)
        return {s: None for s in symbols}

    try:
        data = resp.json()
    except Exception as e:
        logger.error("[simple-latest] json decode error: %s", e)
        return {s: None for s in symbols}

    if isinstance(data, dict) and data.get("error"):
        logger.error("[simple-latest] provider error: %s", data.get("error"))
        return {s: None for s in symbols}

    items = data.get("data", []) if isinstance(data, dict) else []
    result: Dict[str, Optional[dict]] = {s: None for s in symbols}

    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                sym = it.get("symbol") or it.get("ticker")
                if sym and isinstance(sym, str):
                    result[sym.upper()] = it
    else:
        # Some responses might be object-shaped with symbol/date directly
        if isinstance(data, dict) and "symbol" in data and "date" in data:
            result[data["symbol"].upper()] = data

    return result


def fetch_latest_for_symbols(symbols: List[str], prefer_https: bool = True, timeout: int = 20) -> Dict[str, Optional[dict]]:
    """
    Fetch latest EOD data for the given symbols using Marketstack /eod/latest.

    - When prefer_https=True, use v2 (HTTPS) endpoint first, then fall back to v1 (HTTP) on failure.
    - When prefer_https=False, use v1 (HTTP) only.
    - API keys are loaded from stock_pipeline.config (MARKETSTACK_API_KEYS or MARKETSTACK_API_KEY).
    """
    symbols = [s.strip().upper() for s in (symbols or []) if isinstance(s, str) and s.strip()]
    if not symbols:
        return {}

    keys = get_marketstack_api_keys()
    if not keys:
        logger.error("[simple-latest] No MARKETSTACK API keys found in env via config.")
        return {s: None for s in symbols}

    v2_url = "https://api.marketstack.com/v2/eod/latest"
    v1_url = "http://api.marketstack.com/v1/eod/latest"

    last_result: Dict[str, Optional[dict]] = {s: None for s in symbols}
    logger.warning("[simple-latest] trying keys: %s", keys)
    for idx, key in enumerate(keys):
        # Try v2 (HTTPS) first when preferred
        if prefer_https:
            res_v2 = _request_latest(symbols, v2_url, api_key=key, timeout=timeout)
            if any(res_v2.values()):
                return res_v2
            logger.warning("[simple-latest] no results with key #%d on v2; falling back to v1 (HTTP)", idx + 1)
            last_result = res_v2

        # Try v1 (HTTP)
        res_v1 = _request_latest(symbols, v1_url, api_key=key, timeout=timeout)
        if any(res_v1.values()):
            return res_v1
        last_result = res_v1
        logger.warning("[simple-latest] no results with key #%d on v1; trying next key if available", idx + 1)

    # If all keys failed, return the last observed empty mapping
    return last_result


def get_latest_stock_data(symbol: str, prefer_https: bool = True, timeout: int = 20) -> Optional[dict]:
    """Convenience wrapper for a single symbol."""
    out = fetch_latest_for_symbols([symbol], prefer_https=prefer_https, timeout=timeout)
    return out.get(symbol.upper())
