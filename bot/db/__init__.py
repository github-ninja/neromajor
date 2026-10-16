"""Database: asyncpg pool and versioned SQL migrations."""

import logging
import re
from pathlib import Path

import asyncpg

logger = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_MIGRATION_RE = re.compile(r"^(\d{4})_[\w-]+\.sql$")
# Arbitrary constant: serialises concurrent migrators (e.g. two containers).
_MIGRATION_LOCK_ID = 7_004_221


async def create_pool(dsn: str, min_size: int, max_size: int) -> asyncpg.Pool:
    # Every timestamp is stored and compared in UTC; local time only exists
    # at the presentation layer (Settings.timezone).
    pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        server_settings={"timezone": "UTC", "application_name": "neuromajor"},
    )
    logger.info("Пул соединений создан (min=%d, max=%d).", min_size, max_size)
    return pool


def _discover() -> list[tuple[int, Path]]:
    found = []
    for path in sorted(_MIGRATIONS_DIR.iterdir()):
        match = _MIGRATION_RE.match(path.name)
        if match:
            found.append((int(match.group(1)), path))
    return found


async def migrate(pool: asyncpg.Pool) -> list[int]:
    """Apply pending migrations, each in its own transaction. Returns applied versions."""
    applied_now: list[int] = []
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        for version, path in _discover():
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock($1)", _MIGRATION_LOCK_ID)
                done = await conn.fetchval(
                    "SELECT 1 FROM schema_migrations WHERE version = $1", version
                )
                if done:
                    continue
                logger.info("Применяю миграцию %s.", path.name)
                await conn.execute(path.read_text(encoding="utf-8"))
                await conn.execute(
                    "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
                    version, path.name,
                )
                applied_now.append(version)
    if applied_now:
        logger.info("Миграции применены: %s.", applied_now)
    return applied_now
