from dagster import asset, Field, String
import logging
import requests

from stock_pipeline.unified_latest import refresh_latest_symbols
from stock_pipeline.config import get_marketstack_api_keys


@asset(
    config_schema={
        "symbols": Field(
            [String],
            description="List of stock symbols to update.",
            default_value=[],
        )
    }
)
def update_stock_data(context):
    """
    Fetch latest EOD data for provided symbols and upsert into DB (24h cooldown).

    Uses the unified refresher which enforces cooldown via history table,
    rotates API keys in the provider layer, and updates stock_data + latest cache.
    Returns a mapping of symbol -> latest item (or None when unavailable).
    """
    # Ensure our library logs are visible in Dagster
    logging.getLogger("stock_pipeline").setLevel(logging.DEBUG)
    logging.getLogger("stock_pipeline.fetch_data").setLevel(logging.DEBUG)
    logging.getLogger("stock_pipeline.unified_latest").setLevel(logging.DEBUG)

    symbols = context.op_config["symbols"]
    context.log.info(f"Starting latest refresh for symbols: {symbols}")

    if not symbols:
        context.log.warning("No symbols provided in the configuration. Skipping run.")
        return {}

    try:
        results = refresh_latest_symbols(symbols)
        success = {k: v for k, v in results.items() if v}
        failed = [k for k, v in results.items() if not v]
        context.add_output_metadata({
            "success_count": len(success),
            "failed": failed,
        })
        context.log.info(f"Succeeded: {set(success.keys())}, Failed: {set(failed)}")

        # Diagnostics: if any failures, make one direct provider call and log status/body
        if failed:
            try:
                keys = get_marketstack_api_keys()
                key = (keys[0] if keys else "").strip()
                if key:
                    sym = failed[0]
                    url = "https://api.marketstack.com/v2/eod/latest"
                    params = {"access_key": key, "symbols": sym}
                    resp = requests.get(url, params=params, timeout=20)
                    body = resp.text[:800] if hasattr(resp, "text") else "<no body>"
                    context.log.error(
                        f"[diagnostic] provider call for {sym} -> status={resp.status_code}, body={body}"
                    )
                else:
                    context.log.error("[diagnostic] No MARKETSTACK_API_KEYS available to test provider call.")
            except Exception as diag_err:
                context.log.error(f"[diagnostic] error while probing provider: {diag_err}")

        return results
    except Exception as e:
        context.log.error(f"Error during latest refresh asset execution: {e}")
        raise



