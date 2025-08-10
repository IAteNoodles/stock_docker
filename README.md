# Stock Docker: FastAPI + Dagster + Postgres

This repo packages a stock data API (FastAPI), an orchestration layer (Dagster), and a Postgres database into a single Docker Compose deployment.

- API: serves latest quotes and symbols data
- Dagster: watches a tracklist file and runs jobs to fetch/update data
- Postgres: stores the latest payloads and history metadata

See module docs:
- `stock_pipeline/README.md`
- `dagster_pipeline/README.md`

## Repository layout

- `stock_pipeline/` — FastAPI app and data/DB helpers
- `dagster_pipeline/` — Dagster jobs, sensors
- `docker-compose.deploy.yml` — production-ish compose
- `deploy.sh` — one-command deploy tool (Linux/macOS)
- `deploy.ps1` — one-command deploy tool (Windows PowerShell)
- `deploy.bat` — Windows CMD convenience wrapper (PowerShell is recommended)
- `.env.example` — example environment file for reference

## Quick start (recommended)

1) Configure environment
- Windows (PowerShell): `./deploy.ps1 init` and follow prompts
- Linux/macOS: `./deploy.sh init` and follow prompts
- Alternatively, copy `.env.example` to `.env.deploy` and edit values

2) Start services
- Windows (PowerShell): `./deploy.ps1 up` (uses Docker build cache)
  - Or: `./deploy.ps1 up --no-cache` (forces clean image rebuild)
- Linux/macOS: `./deploy.sh up`
  - Or: `./deploy.sh up --no-cache`

3) Watch logs
- Windows: `./deploy.ps1 logs` (all services) or `./deploy.ps1 logs api` / `dagster-webserver` / `postgres`
- Linux/macOS: `./deploy.sh logs` (or `./deploy.sh logs api`)

Service URLs:
- Dagster UI: http://localhost:33000
- API (FastAPI): http://localhost:8000 (docs at /docs)
- Postgres: localhost:65432 (host port)

The deploy scripts will:
- Pass `--env-file .env.deploy` to all compose commands
- Wait for Postgres to be ready
- Create the database named in `DB_NAME` if missing
- Create tables `latest` and `history` if missing

Notes for Windows:
- Use Windows PowerShell (or PowerShell 7) to run `deploy.ps1` from the repo root.
- If script execution is restricted, you may need to enable script execution for your session: `Set-ExecutionPolicy -Scope Process Bypass -Force`.

## Configuration (.env.deploy)

Required variables:
- `DB_NAME` — database name inside Postgres (e.g., `stocks_db`)
- `DB_USER` — application DB username (used by API/Dagster to connect)
- `DB_PASS` — application DB password
- `MARKETSTACK_API_KEYS` — comma-separated Marketstack API keys used by the data fetcher
- Optional build args: `GIT_REPO`, `GIT_REF` (used only when building from `Dockerfile.git`)

Notes:
- Keep `.env` (local/dev tools) separate from `.env.deploy` (compose runtime). The deploy scripts (`deploy.sh` / `deploy.ps1`) use only `.env.deploy`.
- Ensure DB name is consistent across services; the deploy scripts create `DB_NAME` inside the Postgres container.

### Example .env.deploy

```
DB_NAME=stocks_db
DB_USER=stock_user
DB_PASS=stock_pass

# Comma-separated keys; order matters (leftmost preferred)
MARKETSTACK_API_KEYS=key1,key2,key3

# Optional image build from Git
GIT_REPO=https://github.com/IAteNoodles/stock_docker.git
GIT_REF=master
```

### API key rotation

The fetcher rotates Marketstack keys to respect rate limits and maximize uptime:
- Keys are loaded from `MARKETSTACK_API_KEYS` (CSV). Duplicates removed, order preserved.
- Each request selects a usable key index based on recent usage stored in `api_key_history`.
- On HTTP 429 (rate limit), the current key is marked as limited with a cooldown and the next key is tried.
- Metadata stored in DB is only the key index and timestamps; actual key strings are never persisted.
- When limits reset, keys become eligible again automatically.

## Cache control (builds)

- Use cache (default): `./deploy.sh up` or `./deploy.ps1 up` (and `restart`)
- No cache: `./deploy.sh up --no-cache` or `./deploy.ps1 up --no-cache` (and `restart --no-cache`)

Under the hood, `--no-cache` runs `docker compose build --no-cache` followed by `docker compose up -d`.

## Updating code

- If using bind mounts (compose), code changes reflect immediately in containers without rebuilds
- If building images (no mounts), rebuild:
  - Linux/macOS: `./deploy.sh restart` (with cache) or `./deploy.sh restart --no-cache`
  - Windows: `./deploy.ps1 restart` (with cache) or `./deploy.ps1 restart --no-cache`

## Docker Hub (optional)

Build, tag, and push:
- `docker build -t <youruser>/stock-docker:latest .`
- `docker push <youruser>/stock-docker:latest`

Update `docker-compose.deploy.yml` to use:
- `image: <youruser>/stock-docker:latest` (instead of `build:`)

## Local development (without Docker)

- Python 3.11+
- `pip install -r requirements.txt`
- API dev: `uvicorn stock_pipeline.api:app --reload --port 8000`
- Dagster dev: `dagster dev -m dagster_pipeline -d . -h 0.0.0.0 -p 3000`

Ensure Postgres is available and `DB_*` env vars are set accordingly.

## Testing

- Run tests: `pytest -q`

## Troubleshooting

- Database does not exist: Ensure `DB_NAME` is correct in `.env.deploy`. `deploy.sh`/`deploy.ps1 up` will auto-create it.
- Relation "history" does not exist: The deploy scripts create tables idempotently; run `./deploy.sh restart` or `./deploy.ps1 restart`.
- Healthcheck/pg_isready errors: Compose is configured to use safe defaults. Ensure `DB_USER`, `DB_NAME` match runtime values.
- Sensor not triggering: Verify bind mounts for `stock_pipeline/` and `dagster_pipeline/` are active and `PYTHONPATH` is set in compose; adjust the tracklist file and watch Dagster UI.

## Contributing guidelines

- Keep secrets out of git: never commit `.env` files; use `.env.example` for templates
- Use clear, consistent naming for env vars and DB names across services
- Prefer idempotent setup scripts (as in `deploy.sh` / `deploy.ps1`) so cold starts succeed
- Write tests for non-trivial changes in data processing
