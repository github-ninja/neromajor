"""
Integration tests against a throwaway PostgreSQL. Skipped unless TEST_DB_URL
points to an empty database, e.g.:

    docker run --rm -d -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=t postgres:16-alpine
    TEST_DB_URL=postgresql://postgres:t@127.0.0.1:55432/postgres pytest
"""

import os
from datetime import date, datetime, timezone

import asyncpg
import pytest
import pytest_asyncio

from bot import db
from bot.db.repo import NewViolation, Repository

TEST_DB_URL = os.getenv("TEST_DB_URL")
pytestmark = [
    pytest.mark.skipif(not TEST_DB_URL, reason="TEST_DB_URL not set"),
    pytest.mark.asyncio,
]

BASELINE = (db._MIGRATIONS_DIR / "0001_baseline.sql").read_text()


@pytest_asyncio.fixture
async def pool():
    pool = await db.create_pool(TEST_DB_URL, 1, 3)
    async with pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    yield pool
    await pool.close()


async def seed_legacy(conn: asyncpg.Connection) -> None:
    """Data shaped like the production database before the refactor."""
    await conn.execute(BASELINE)
    await conn.execute(
        """
        INSERT INTO users (user_id, username, display_name, updated_at)
        VALUES (1, 'ivan', 'Иван', '2026-03-01 10:00'), (2, NULL, 'Пётр', NULL);
        INSERT INTO messages (chat_id, user_id, content, is_bot, "timestamp") VALUES
            (-100, 1, 'Слава кому-то', FALSE, '2026-03-01 10:00'),   -- id 1
            (-100, 2, 'обычный текст', FALSE, '2026-03-01 11:00'),   -- id 2
            (-100, 999, 'реплика бота', TRUE, '2026-03-01 11:30'),  -- id 3
            (-100, 1, 'после проверки', FALSE, '2026-03-05 09:00'), -- id 4
            (-200, 2, 'другой чат', FALSE, '2026-03-02 09:00');     -- id 5
        INSERT INTO violation_logs (chat_id, user_id, content, article, fines, days, created_at) VALUES
            (-100, 1, 'Слава кому-то', 'ст. 280', 300000, 0, '2026-03-03 12:00'),
            (-100, 2, 'перефразировано ИИ', 'ст. 282', 10000, 0, '2026-03-03 12:00');
        INSERT INTO stats_checkpoint (chat_id, last_check) VALUES (-100, '2026-03-03 12:00');
        """
    )


async def test_migration_converts_legacy_data(pool):
    async with pool.acquire() as conn:
        await seed_legacy(conn)
    assert await db.migrate(pool) == [1, 2]
    assert await db.migrate(pool) == []  # idempotent

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, message_id, committed_at, detected_at FROM violations ORDER BY user_id"
        )
        # Matched violation gets its source message and real date (UTC preserved).
        assert rows[0]["message_id"] == 1
        assert rows[0]["committed_at"] == datetime(2026, 3, 1, 10, tzinfo=timezone.utc)
        # Paraphrased content can't be matched: falls back to the audit time.
        assert rows[1]["message_id"] is None
        assert rows[1]["committed_at"] == datetime(2026, 3, 3, 12, tzinfo=timezone.utc)

        chats = {r["chat_id"]: r["audit_cursor"] for r in await conn.fetch("SELECT * FROM chats")}
        assert chats == {-100: 3, -200: 0}  # checkpoint -> last message id before it
        assert await conn.fetchval("SELECT updated_at FROM users WHERE user_id = 2") is not None
        for table in ("violation_logs", "stats_checkpoint"):
            assert await conn.fetchval("SELECT to_regclass($1)", table) is None


async def test_audit_cycle_and_amnesty(pool):
    await db.migrate(pool)
    repo = Repository(pool)
    ids = [
        await repo.save_user_message(-1, 10, "petya", "Пётр", None, "Пётр", f"msg {i}", i)
        for i in range(3)
    ]
    await repo.save_bot_message(-1, 999, "бот", 100)

    pending = await repo.pending_messages(-1, 10)
    assert [m.id for m in pending] == ids  # bot message excluded
    assert await repo.chats_with_pending() == [-1]

    v = NewViolation(ids[1], 10, "msg 1", "ст. 280", 300_000, 0, pending[1].created_at)
    assert await repo.commit_audit(-1, ids[1], [v]) == 1
    assert await repo.commit_audit(-1, ids[1], [v]) == 0  # duplicate ignored
    assert [m.id for m in await repo.pending_messages(-1, 10)] == [ids[2]]

    [stats] = await repo.offender_stats(-1)
    assert (stats.total_count, stats.total_fines, stats.day_count) == (1, 300_000, 1)
    assert (await repo.find_chat_user(-1, "@PETYA".lstrip("@"))).user_id == 10
    assert await repo.find_chat_user(-2, "petya") is None

    await repo.reset_violations(-1)
    assert await repo.offender_stats(-1) == []
    assert await repo.count_pending(-1) == 0


async def test_proactive_bookkeeping(pool):
    await db.migrate(pool)
    repo = Repository(pool)
    await repo.save_user_message(-1, 10, None, "A", None, "A", "hi", 1)
    today = date(2026, 10, 16)
    assert await repo.proactive_candidates(24, today) == [-1]
    await repo.mark_proactive(-1, today)
    assert await repo.proactive_candidates(24, today) == []
    assert await repo.activity(-1, 10) == (1, 1)
