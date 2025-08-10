# dagster_pipeline (Dagster orchestration)

This package defines Dagster jobs and sensors for updating stock data.

Highlights:
- `trackfile_change_sensor` — watches `stock_pipeline/tracklist.py` for changes (mtime + content hash)
- Triggers `stock_data_update_daily_job` with a run config containing the tracked symbols
- Also emits a run every 24 hours even without changes

Entrypoints:
- Dev server: `dagster dev -m dagster_pipeline -d . -h 0.0.0.0 -p 3000`
- Web UI served on port 3000 in Docker (mapped to 33000 on host)

Environment:
- `PYTHONPATH` should include the repo root so `stock_pipeline` and `dagster_pipeline` can be imported

Operational notes:
- The container bind-mounts `stock_pipeline/` and `dagster_pipeline/` in compose
- The sensor reloads `tracklist.py` on each tick and normalizes symbols
- Jobs write to Postgres tables `latest` and `history`
