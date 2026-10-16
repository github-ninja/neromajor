"""
Integration tests against a throwaway PostgreSQL. Skipped unless TEST_DB_URL
points to an empty database, e.g.:

    docker run --rm -d -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=t postgres:16-alpine
    TEST_DB_URL=postgresql://postgres:t@127.0.0.1:55432/postgres pytest
"""

import os
from datetime import date, datetime, timezone
from pathlib import Path

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

ROOT = Path(__file__).resolve().parent.parent
LEGACY_SCHEMA = (ROOT / "tests" / "legacy_schema.sql").read_text()
IMPORT_SCRIPT = (ROOT / "scripts" / "import_legacy.sql").read_text()
BOT_ID = 999


@pytest_asyncio.fixture
async def pool():
    pool = await db.create_pool(TEST_DB_URL, 1, 3)
    async with pool.acquire() as conn:
        await conn.execute(
            "DROP SCHEMA IF EXISTS legacy CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
        )
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def repo(pool):
    await db.migrate(pool)
    repo = Repository(pool)
    await repo.register_bot(BOT_ID, "neuromajor_bot", "Товарищ Майор")
    return repo


async def test_migrations_are_idempotent(pool):
    assert await db.migrate(pool) == [1]
    assert await db.migrate(pool) == []


async def test_audit_cycle_and_amnesty(repo):
    ids = [
        await repo.save_user_message(-1, 10, "petya", "Пётр", None, "Пётр", f"msg {i}", i)
        for i in range(3)
    ]
    await repo.save_bot_message(-1, BOT_ID, "бот", 100)

    pending = await repo.pending_messages(-1, 10)
    assert [m.id for m in pending] == ids  # bot message excluded
    assert await repo.chats_with_pending() == [-1]

    v = NewViolation(ids[1], "ст. 280", 300_000, 0)
    assert await repo.commit_audit(-1, ids[1], [v]) == 1
    assert await repo.commit_audit(-1, ids[1], [v]) == 0  # duplicate ignored
    assert [m.id for m in await repo.pending_messages(-1, 10)] == [ids[2]]

    [stats] = await repo.offender_stats(-1)
    assert (stats.total_count, stats.total_fines, stats.day_count) == (1, 300_000, 1)
    assert stats.last_violation == pending[1].created_at  # dated by the message
    [row] = await repo.user_violations(-1, 10)
    assert row.content == "msg 1"

    assert (await repo.find_chat_user(-1, "PETYA")).user_id == 10
    assert await repo.find_chat_user(-2, "petya") is None
    assert await repo.find_chat_user(-1, "neuromajor_bot") is None

    citizen = await repo.random_citizen(-1)
    assert citizen.total_fines == 300_000 and citizen.last_violation is not None

    await repo.reset_violations(-1)
    assert await repo.offender_stats(-1) == []
    assert await repo.count_pending(-1) == 0


async def test_dialogue_marks_bot_lines(repo):
    await repo.save_user_message(-1, 10, None, "A", None, "A", "привет", 1)
    await repo.save_bot_message(-1, BOT_ID, "слежу", 2)
    lines = await repo.recent_dialogue(-1, 10)
    assert [(l.author, l.content) for l in lines] == [("A", "привет"), (None, "слежу")]
    assert await repo.last_bot_message_at(-1) is not None


async def test_proactive_bookkeeping(repo):
    await repo.save_user_message(-1, 10, None, "A", None, "A", "hi", 1)
    today = date(2026, 10, 16)
    assert await repo.proactive_candidates(24, today) == [-1]
    await repo.mark_proactive(-1, today)
    assert await repo.proactive_candidates(24, today) == []
    assert await repo.activity(-1, 10) == (1, 1)


async def test_violation_requires_sanction(repo):
    mid = await repo.save_user_message(-1, 10, None, "A", None, "A", "x", 1)
    with pytest.raises(asyncpg.CheckViolationError):
        await repo.commit_audit(-1, mid, [NewViolation(mid, "ст. 1", 0, 0)])


async def test_legacy_import(pool):
    async with pool.acquire() as conn:
        await conn.execute("CREATE SCHEMA legacy; SET search_path = legacy;")
        await conn.execute(LEGACY_SCHEMA)
        await conn.execute(
            """
            INSERT INTO users (user_id, username, display_name, updated_at)
            VALUES (1, 'ivan', 'Иван', '2026-03-01 10:00'), (2, NULL, 'Пётр', NULL);
            INSERT INTO messages (chat_id, user_id, content, is_bot, "timestamp") VALUES
                (-100, 1, 'первое', FALSE, '2026-03-01 10:00'),
                (-100, 999, 'реплика бота', TRUE, '2026-03-01 11:30'),
                (-100, 2, NULL, FALSE, '2026-03-05 09:00'),
                (-200, 2, 'другой чат', FALSE, '2026-03-02 09:00');
            INSERT INTO violation_logs (chat_id, user_id, content, fines) VALUES (-100, 1, 'первое', 1000);
            SET search_path = public;
            """
        )
    await db.migrate(pool)
    async with pool.acquire() as conn:
        await conn.execute(IMPORT_SCRIPT)
        msgs = await conn.fetch("SELECT * FROM messages ORDER BY id")
        assert [m["content"] for m in msgs] == ["первое", "реплика бота", "", "другой чат"]
        assert msgs[0]["created_at"] == datetime(2026, 3, 1, 10, tzinfo=timezone.utc)
        bot = await conn.fetchrow("SELECT * FROM users WHERE user_id = 999")
        assert bot["is_bot"] and bot["display_name"] == "Товарищ Майор"
        assert await conn.fetchval("SELECT count(*) FROM violations") == 0  # amnesty

    repo = Repository(pool)
    assert await repo.chats_with_pending() == []  # history counts as audited
    await repo.save_user_message(-100, 1, "ivan", "Иван", None, "Иван", "новое", 5)
    assert [m.content for m in await repo.pending_messages(-100, 10)] == ["новое"]
