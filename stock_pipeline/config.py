"""Centralized configuration loaded from .env.

This module provides a single place to read environment variables needed by the
application: database credentials and API keys. It uses python-dotenv to load a
`.env` file colocated with the package, and exposes simple accessors.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load .env next to this file so it works regardless of CWD
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return a sanitized environment variable value.

    Trims whitespace, removes one pair of wrapping quotes if present, then
    trims again. Returns default if the final result is empty.
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
    name: Optional[str]
    user: Optional[str]
    password: Optional[str]
    host: Optional[str]
    port: Optional[str]


def get_db_settings() -> DatabaseSettings:
    """Return database settings from environment."""
    return DatabaseSettings(
        name=_env("DB_NAME"),
        user=_env("DB_USER"),
        password=_env("DB_PASS") or _env("DB_PASSWORD"),  # support both names
        host=_env("DB_HOST"),
        port=_env("DB_PORT"),
    )


def get_marketstack_api_key() -> Optional[str]:
    """Return the Marketstack API key from environment (or None)."""
    return _env("MARKETSTACK_API_KEY")
