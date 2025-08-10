# stock_pipeline (FastAPI service)

The `stock_pipeline` package implements the HTTP API and data access to Postgres.

Main endpoints:
- `GET /` — healthcheck
- `GET /stocks` — process and return stock list
- `GET /latest?symbol=XYZ` — returns latest cached payload for a symbol and schedules a background refresh
- `GET /api-key-history` — returns API key usage metadata

Key modules:
- `api.py` — FastAPI app factory, logging, background refresh scheduler
- `fetch_data.py` — integrates with Marketstack to fetch/update data
- `fetch_from_db.py` — DB read/write helpers for `latest` and `history`
- `db_init.py` — optional DB/table initialization pieces used by the app
- `config.py` — centralized configuration from environment/.env

Environment variables:
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`
- `MARKETSTACK_API_KEYS` — comma-separated list of API keys

How to get Marketstack API keys:
- Create an account at https://marketstack.com
- Create an API access key (free tier works for testing)
- Copy the key(s) and set them in `.env.deploy` as:
  - `MARKETSTACK_API_KEYS=key1,key2` (recommended, supports rotation)
  - or legacy single key: `MARKETSTACK_API_KEY=key1`
- The service automatically rotates keys on HTTP 429 (rate limit) and records usage metadata in `api_key_history` (only key indices, never the key string).

How `config.py` works:
- Loads `.env` located next to `stock_pipeline/config.py` using python-dotenv without overriding variables already set by the OS/Docker.
- Exposes helpers:
  - `_env(name, default)` — reads and sanitizes env values (trims and unwraps quotes).
  - `DatabaseSettings` — dataclass with name, user, password, host, port.
  - `get_db_settings()` — returns `DatabaseSettings` from `DB_*` variables.
  - `get_marketstack_api_keys()` — returns a list of keys from `MARKETSTACK_API_KEYS` (CSV) and/or `MARKETSTACK_API_KEY` (single). Duplicates removed, preserves order.

Function overview (most-used):
- fetch_data.py
  - `process_tickers(tickers, start_date, end_date)` — ensures requested range exists in DB with minimal API calls (never per-day calls; one window per ticker), with 24h cooldown via `history`.
  - `get_or_refresh_latest(symbol)` — returns cached latest within 24h, else fetches `/v2/eod/latest`, upserts into DB and cache.
  - `fetch_historical_data_marketstack(tickers, start_date, end_date)` — provider client with API-key rotation and pagination.
  - `fetch_latest_data_marketstack(tickers)` — provider latest endpoint with rate-limit handling.
  - `get_latest_value(stop_event, default_idle_seconds)` — background worker to refresh due symbols based on cooldown.
  - `refresh_tracked_symbol(symbol)` — refresh a tracked symbol if due, update cache/history.
  - `track_symbol(symbol)` / `untrack_symbol(symbol)` — manage tracked set (idempotent).
  - `run_tracked_refresher(stop_event, default_idle_seconds)` — scheduler for tracked symbols.
- fetch_from_db.py
  - `connect_to_db()` — returns a new psycopg2 connection using `config.get_db_settings()`.
  - History helpers: `ensure_history_symbol`, `get_history`, `update_history_latest_date`, `touch_history_last_tried`, `get_max_trade_date_for_symbol`, `list_history_entries`.
  - Stock data: `insert_stock_data`, `get_stock_data`, `get_existing_dates`.
  - Latest cache: `get_latest_cache`, `upsert_latest_cache`.
  - API key usage: `upsert_api_keys_history`, `select_usable_api_key_idx`, `mark_api_key_limit_reached_idx`, `list_api_key_history`.
  - Tracked symbols: `add_tracked_symbol`, `remove_tracked_symbol`, `list_tracked_symbols`, `list_tracked_symbols_with_meta`, `set_tracked_last_fetched`.
- db_schema.py
  - `create_stock_data_table`, `create_history_table`, `create_latest_table`, `create_api_key_history_table`, `create_tracked_symbols_table` — idempotent creators used by the app and deploy script.
- db_init.py
  - Optional initialization hooks used by the API to ensure tables on startup when DB is reachable.
- latest.py / simple_latest.py / unified_latest.py / update_db.py
  - Helpers and alternatives for fetching/updating latest data and coordinating DB updates; kept for compatibility/experiments.

DB schema (tables created by deploy script or on first use):
- `latest(symbol TEXT PRIMARY KEY, data JSONB, last_updated TIMESTAMPTZ)`
- `history(symbol TEXT PRIMARY KEY, latest_data_date DATE)`
- `stock_data(symbol TEXT, trade_date DATE, open_price NUMERIC, high_price NUMERIC, low_price NUMERIC, close_price NUMERIC, volume NUMERIC, PRIMARY KEY(symbol, trade_date))`
- `api_key_history(idx INT PRIMARY KEY, last_used TIMESTAMPTZ, limit_reached TIMESTAMPTZ)`
- `tracked_symbols(symbol TEXT PRIMARY KEY, tracked_at TIMESTAMPTZ, last_fetched TIMESTAMPTZ)`

Development:
- `uvicorn stock_pipeline.api:app --reload --port 8000`
- Run tests with `pytest -q`
