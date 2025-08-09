"""Centralized configuration loaded from .env.

This module provides a single place to read environment variables needed by the
application: database credentials and API keys. It uses python-dotenv to load a
`.env` file colocated with the package, and exposes simple accessors.

Environment Variables
- DB_NAME, DB_USER, DB_PASS/DB_PASSWORD, DB_HOST, DB_PORT
- MARKETSTACK_API_KEYS: comma-separated keys (preferred)
- MARKETSTACK_API_KEY: single key (legacy/fallback)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, List
from dotenv import load_dotenv

# Load .env next to this file so it works regardless of CWD
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return a sanitized environment variable value.

    Parameters
    ----------
    name : str
        Variable name to read from the environment.
    default : Optional[str]
        Default value to use if the variable is missing or empty after sanitation.

    Returns
    -------
    Optional[str]
        Trimmed value with one level of wrapping quotes removed, or ``default``.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip()
    if (len(v) >= 2) and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        v = v[1:-1]
    v = v.strip()
    return v if v != "" else default


@dataclass(frozen=True)
class DatabaseSettings:
    """Typed container for database connection settings."""
    name: Optional[str]
    user: Optional[str]
    password: Optional[str]
    host: Optional[str]
    port: Optional[str]


def get_db_settings() -> DatabaseSettings:
    """Return database settings from environment.

    Returns
    -------
    DatabaseSettings
        Dataclass with fields name, user, password, host, port (all optional strings).
    """
    return DatabaseSettings(
        name=_env("DB_NAME"),
        user=_env("DB_USER"),
        password=_env("DB_PASS") or _env("DB_PASSWORD"),  # support both names
        host=_env("DB_HOST"),
        port=_env("DB_PORT"),
    )


def get_marketstack_api_keys() -> List[str]:
    """Return a list of Marketstack API keys from env.

    Supports either:
    - MARKETSTACK_API_KEYS: comma-separated string of keys
    - MARKETSTACK_API_KEY: single key (fallback)

    Returns
    -------
    list[str]
        Ordered list of API keys; duplicates removed with preference for the
        explicit list when provided.
    """
    keys_csv = _env("MARKETSTACK_API_KEYS")
    keys: List[str] = []
    if keys_csv:
        keys = [k.strip() for k in keys_csv.split(",") if k and k.strip()]
    single = _env("MARKETSTACK_API_KEY")
    if single:
        # ensure first key is the legacy single key if no list provided
        if not keys:
            keys = [single]
        elif single not in keys:
            keys.insert(0, single)
    return keys
