"""Database schema creation helpers.

This module contains idempotent functions that ensure required tables and
indexes exist. These functions accept a live psycopg2 cursor and perform DDL
statements. They are safe to call repeatedly and on every startup.
"""

import logging
import psycopg2

logger = logging.getLogger(__name__)


def create_stock_data_table(cursor) -> None:
    """Ensure the ``stock_data`` table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active connection/transaction.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL,
        trade_date DATE NOT NULL,
        open_price NUMERIC(10, 4),
        high_price NUMERIC(10, 4),
        low_price NUMERIC(10, 4),
        close_price NUMERIC(10, 4),
        volume BIGINT,
        UNIQUE (symbol, trade_date)
    );
    """
    try:
        cursor.execute(create_table_query)
        logger.info("[db-schema] ensured table stock_data")
    except psycopg2.Error as e:
        logger.error("[db-schema] error creating stock_data table: %s", e)


def create_history_table(cursor) -> None:
    """Ensure the ``history`` table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active connection/transaction.
    """
    query = """
    CREATE TABLE IF NOT EXISTS history (
        symbol VARCHAR(50) PRIMARY KEY,
        latest_data_date DATE,
        last_tried_to_fetch_date TIMESTAMPTZ
    );
    """
    try:
        cursor.execute(query)
        logger.info("[db-schema] ensured table history")
    except psycopg2.Error as e:
        logger.error("[db-schema] error creating history table: %s", e)


def create_latest_table(cursor) -> None:
    """Ensure the ``latest`` cache table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active connection/transaction.
    """
    query = """
    CREATE TABLE IF NOT EXISTS latest (
        symbol VARCHAR(50) PRIMARY KEY,
        data JSONB,
        last_updated TIMESTAMPTZ
    );
    """
    try:
        cursor.execute(query)
        logger.info("[db-schema] ensured table latest")
    except psycopg2.Error as e:
        logger.error("[db-schema] error creating latest table: %s", e)


def create_api_key_history_table(cursor) -> None:
    """Ensure the ``api_key_history`` table and its unique index exist.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active connection/transaction.
    """
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_key_history (
                idx INT,
                last_used TIMESTAMPTZ,
                limit_reached TIMESTAMPTZ
            );
            """
        )
    except psycopg2.Error as e:
        logger.error("[db-schema] error ensuring api_key_history table exists: %s", e)

    try:
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS idx INT;")
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS last_used TIMESTAMPTZ;")
        cursor.execute("ALTER TABLE api_key_history ADD COLUMN IF NOT EXISTS limit_reached TIMESTAMPTZ;")
    except psycopg2.Error as e:
        logger.error("[db-schema] error ensuring api_key_history columns: %s", e)

    try:
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS api_key_history_idx_uq ON api_key_history (idx);"
        )
    except psycopg2.Error as e:
        logger.error("[db-schema] error creating unique index on api_key_history.idx: %s", e)

    logger.info("[db-schema] ensured table api_key_history")


def create_tracked_symbols_table(cursor) -> None:
    """Ensure the ``tracked_symbols`` table exists.

    Parameters
    ----------
    cursor : psycopg2.extensions.cursor
        Open cursor bound to an active connection/transaction.
    """
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_symbols (
                symbol VARCHAR(50) PRIMARY KEY,
                tracked_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cursor.execute("ALTER TABLE tracked_symbols ADD COLUMN IF NOT EXISTS tracked_at TIMESTAMPTZ DEFAULT NOW();")
        cursor.execute("ALTER TABLE tracked_symbols ADD COLUMN IF NOT EXISTS last_fetched TIMESTAMPTZ;")
        logger.info("[db-schema] ensured table tracked_symbols")
    except psycopg2.Error as e:
        logger.error("[db-schema] error creating tracked_symbols table: %s", e)
