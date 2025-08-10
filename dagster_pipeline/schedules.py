from dagster import define_asset_job, schedule, DefaultScheduleStatus
import importlib

from . import assets


def _read_symbols_from_tracklist():
    try:
        mod = importlib.import_module("stock_pipeline.tracklist")
        try:
            mod = importlib.reload(mod)
        except Exception:
            pass
        syms = getattr(mod, "track", getattr(mod, "symbols", []))
        if not isinstance(syms, list):
            return []
        return [s.strip().upper() for s in syms if isinstance(s, str) and s.strip()]
    except Exception:
        return []


# Asset job with a default config sourced from the current tracklist
stock_data_update_daily_job = define_asset_job(
    name="stock_data_update_daily_job",
    selection=[assets.update_stock_data],
    config={
        "ops": {
            "update_stock_data": {
                "config": {
                    "symbols": _read_symbols_from_tracklist(),
                }
            }
        }
    },
)


@schedule(
    cron_schedule="0 0 * * *",
    job=stock_data_update_daily_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_update_schedule(context):
    symbols = _read_symbols_from_tracklist()
    if not symbols:
        context.log.warning("tracklist has no symbols; running with empty list")
    return {
        "ops": {
            "update_stock_data": {
                "config": {
                    "symbols": symbols,
                }
            }
        }
    }
