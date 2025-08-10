from dagster import sensor, RunRequest
import os
import time
from typing import List
import importlib
import json

TRACK_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "stock_pipeline", "tracklist.py")
print(f"[sensors] using track file: {TRACK_FILE}")


def _read_symbols_from_trackfile() -> List[str]:
    # Import and reload the module to pick up edits
    try:
        mod = importlib.import_module("stock_pipeline.tracklist")
        try:
            mod = importlib.reload(mod)
        except Exception:
            pass
        syms = getattr(mod, "track", [])
    except Exception:
        return []
    if not isinstance(syms, list):
        return []
    # normalize
    return [s.strip().upper() for s in syms if isinstance(s, str) and s.strip()]


# This sensor triggers immediately on first load, on tracklist changes,
# and then enforces a 24h interval since the last run.
@sensor(name="trackfile_change_sensor", minimum_interval_seconds=30, job_name="stock_data_update_daily_job")
def trackfile_change_sensor(context):
    # Load cursor state
    state = {"last_mtime": None, "last_run": None}
    if context.cursor:
        try:
            state.update(json.loads(context.cursor))
        except Exception:
            pass

    # Current file mtime
    try:
        mtime = os.path.getmtime(TRACK_FILE)
    except FileNotFoundError:
        context.log.warning(f"Track file not found: {TRACK_FILE}")
        return

    symbols = _read_symbols_from_trackfile()
    if not symbols:
        context.log.warning("Track file has no valid symbols; skipping run request")
        # still update mtime in cursor so we don't retrigger on same missing list
        state["last_mtime"] = mtime
        context.update_cursor(json.dumps(state))
        return

    now = time.time()
    last_mtime = state.get("last_mtime")
    last_run = state.get("last_run")

    changed = (last_mtime is None) or (mtime != last_mtime)
    due_24h = (last_run is None) or (now - float(last_run) >= 86400)

    if not changed and not due_24h:
        return

    # Build run config to pass symbols to the asset
    run_config = {
        "ops": {
            "update_stock_data": {
                "config": {
                    "symbols": symbols,
                }
            }
        }
    }

    # Update cursor and emit run
    state["last_mtime"] = mtime
    state["last_run"] = now
    context.update_cursor(json.dumps(state))
    yield RunRequest(run_key=str(int(now)), run_config=run_config, tags={"trigger": "trackfile_or_interval"})
