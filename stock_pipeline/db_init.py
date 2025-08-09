"""Database initialization entry point.

Ensures all required tables exist and performs any one-time bootstrapping for
runtime features (e.g., api_key_history table presence). Intended to be called
at service startup.
"""

import logging
from .fetch_from_db import connect_to_db
from .db_schema import (
    create_stock_data_table,
    create_history_table,
    create_latest_table,
    create_api_key_history_table,
    create_tracked_symbols_table,
)
from .config import get_marketstack_api_keys

logger = logging.getLogger(__name__)


def init_database() -> bool:
    """Initialize database schema and seed api_key_history indices.

    Returns
    -------
    bool
        True if initialization ran without fatal errors, else False.

    Notes
    -----
    - Safe to call multiple times (DDL is idempotent).
    - Requires a working PostgreSQL connection.
    """
    conn = connect_to_db()
    if not conn:
        logger.warning("[db_init] DB connection unavailable; skipping initialization")
        return False
    try:
        with conn.cursor() as cursor:
            # Ensure core tables exist
            create_stock_data_table(cursor)
            create_history_table(cursor)
            create_latest_table(cursor)
            create_api_key_history_table(cursor)
            create_tracked_symbols_table(cursor)
        conn.commit()
        logger.info("[db_init] schema ensured")
        return True
    except Exception as e:
        logger.error("[db_init] initialization error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass
