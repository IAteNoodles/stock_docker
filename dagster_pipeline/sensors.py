from dagster import sensor, RunRequest
import os
import time
from typing import List
import importlib
import json
import hashlib

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
    state = {"last_mtime": None, "last_run": None, "last_hash": None}
    if context.cursor:
        try:
            state.update(json.loads(context.cursor))
        except Exception:
            pass

    # Current file mtime and content hash
    try:
        mtime = os.path.getmtime(TRACK_FILE)
        with open(TRACK_FILE, "rb") as f:
            contents = f.read()
        file_hash = hashlib.sha256(contents).hexdigest()
    except FileNotFoundError:
        context.log.warning(f"Track file not found: {TRACK_FILE}")
        return

    symbols = _read_symbols_from_trackfile()
    if not symbols:
        context.log.warning("Track file has no valid symbols; skipping run request")
        # still update cursor so we don't retrigger on same empty list
        state["last_mtime"] = mtime
        state["last_hash"] = file_hash
        context.update_cursor(json.dumps(state))
        return

    now = time.time()
    last_mtime = state.get("last_mtime")
    last_run = state.get("last_run")
    last_hash = state.get("last_hash")

    changed = (last_mtime is None) or (mtime != last_mtime) or (file_hash != last_hash)
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
    state["last_hash"] = file_hash
    state["last_run"] = now
    context.update_cursor(json.dumps(state))
    yield RunRequest(run_key=f"{int(now)}:{file_hash[:8]}", run_config=run_config, tags={"trigger": "trackfile_or_interval"})
