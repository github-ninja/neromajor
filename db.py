"""Database layer: connection pool, schema initialization, async helpers."""

import asyncio
import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.pool
from psycopg2.extras import DictCursor

import config

logger = logging.getLogger(__name__)

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool() -> None:
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=config.DB_POOL_MIN,
        maxconn=config.DB_POOL_MAX,
        dsn=config.DB_URL,
    )
    logger.info("Пул соединений создан (min=%d, max=%d).", config.DB_POOL_MIN, config.DB_POOL_MAX)


def close_pool() -> None:
    if _pool is not None:
        _pool.closeall()
        logger.info("Пул соединений закрыт.")


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    assert _pool is not None, "Пул соединений не инициализирован."
    conn = _pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


async def run_in_thread(func, *args):
    return await asyncio.to_thread(func, *args)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def _create_schema() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id      BIGINT PRIMARY KEY,
                    username     TEXT,
                    first_name   TEXT,
                    last_name    TEXT,
                    display_name TEXT NOT NULL,
                    updated_at   TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id         SERIAL PRIMARY KEY,
                    chat_id    BIGINT NOT NULL,
                    user_id    BIGINT NOT NULL,
                    content    TEXT,
                    is_bot     BOOLEAN NOT NULL DEFAULT FALSE,
                    timestamp  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Migration: add is_bot column if not present (existing installs)
            cur.execute(
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_bot BOOLEAN NOT NULL DEFAULT FALSE"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_chat_ts "
                "ON messages (chat_id, timestamp)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_user_id "
                "ON messages (user_id)"
            )

            cur.execute("""
                CREATE TABLE IF NOT EXISTS violation_logs (
                    id              SERIAL PRIMARY KEY,
                    chat_id         BIGINT NOT NULL,
                    user_id         BIGINT NOT NULL,
                    content         TEXT,
                    article         TEXT,
                    fines           BIGINT DEFAULT 0,
                    days            BIGINT DEFAULT 0,
                    violation_count BIGINT DEFAULT 1,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_violations_chat_user_created "
                "ON violation_logs (chat_id, user_id, created_at)"
            )

            cur.execute("""
                CREATE TABLE IF NOT EXISTS stats_checkpoint (
                    chat_id    BIGINT PRIMARY KEY,
                    last_check TIMESTAMPTZ NOT NULL
                )
            """)
        conn.commit()


async def init_db() -> None:
    init_pool()
    try:
        await run_in_thread(_create_schema)
        logger.info("Схема базы данных проверена/создана.")
    except Exception as exc:
        logger.critical("Не удалось инициализировать БД: %s", exc)
        raise
