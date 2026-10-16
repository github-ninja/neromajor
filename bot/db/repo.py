"""All SQL lives here. Handlers and services talk to the database only through Repository."""

from dataclasses import dataclass
from datetime import date, datetime

import asyncpg


@dataclass(frozen=True, slots=True)
class UserRef:
    user_id: int
    display_name: str
    username: str | None


@dataclass(frozen=True, slots=True)
class PendingMessage:
    id: int
    user_id: int
    display_name: str
    content: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class NewViolation:
    message_id: int
    article: str
    fines: int
    days: int


@dataclass(frozen=True, slots=True)
class OffenderStats:
    user_id: int
    display_name: str
    total_count: int
    total_fines: int
    total_days: int
    last_violation: datetime
    day_count: int
    day_fines: int
    day_days: int


@dataclass(frozen=True, slots=True)
class ViolationRow:
    committed_at: datetime
    content: str
    article: str
    fines: int
    days: int


@dataclass(frozen=True, slots=True)
class DialogueLine:
    author: str | None  # None for the bot's own messages
    content: str


@dataclass(frozen=True, slots=True)
class Citizen:
    display_name: str
    total_fines: int
    last_violation: datetime | None


_UPSERT_USER = """
    INSERT INTO users (user_id, username, first_name, last_name, display_name, is_bot, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, now())
    ON CONFLICT (user_id) DO UPDATE SET
        username     = EXCLUDED.username,
        first_name   = EXCLUDED.first_name,
        last_name    = EXCLUDED.last_name,
        display_name = EXCLUDED.display_name,
        updated_at   = now()
    WHERE (users.username, users.first_name, users.last_name, users.display_name)
          IS DISTINCT FROM
          (EXCLUDED.username, EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.display_name)
"""

_ENSURE_CHAT = "INSERT INTO chats (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING"


class Repository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -- writes from the message stream -------------------------------------

    async def register_bot(self, bot_id: int, username: str | None, name: str) -> None:
        """The bot is a user too: its replies are messages with a real author row."""
        await self._pool.execute(_UPSERT_USER, bot_id, username, name, None, name, True)

    async def save_user_message(
        self,
        chat_id: int,
        user_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        display_name: str,
        content: str,
        tg_message_id: int | None,
    ) -> int:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                _UPSERT_USER, user_id, username, first_name, last_name, display_name, False
            )
            await conn.execute(_ENSURE_CHAT, chat_id)
            return await conn.fetchval(
                """
                INSERT INTO messages (chat_id, user_id, content, is_bot, tg_message_id)
                VALUES ($1, $2, $3, FALSE, $4)
                RETURNING id
                """,
                chat_id, user_id, content, tg_message_id,
            )

    async def save_bot_message(
        self, chat_id: int, bot_id: int, content: str, tg_message_id: int | None
    ) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(_ENSURE_CHAT, chat_id)
            await conn.execute(
                """
                INSERT INTO messages (chat_id, user_id, content, is_bot, tg_message_id)
                VALUES ($1, $2, $3, TRUE, $4)
                """,
                chat_id, bot_id, content, tg_message_id,
            )

    # -- reads for conversation context -------------------------------------

    async def recent_dialogue(self, chat_id: int, limit: int) -> list[DialogueLine]:
        """Last *limit* messages, oldest first, including the bot's own."""
        rows = await self._pool.fetch(
            """
            SELECT CASE WHEN m.is_bot THEN NULL ELSE u.display_name END AS author, m.content
            FROM messages m
            JOIN users u ON u.user_id = m.user_id
            WHERE m.chat_id = $1
            ORDER BY m.id DESC
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [DialogueLine(r["author"], r["content"]) for r in reversed(rows)]

    async def recent_human_messages(self, chat_id: int, limit: int) -> list[tuple[str, str]]:
        """Last *limit* human messages as (display_name, content), oldest first."""
        rows = await self._pool.fetch(
            """
            SELECT u.display_name, m.content
            FROM messages m
            JOIN users u ON u.user_id = m.user_id
            WHERE m.chat_id = $1 AND NOT m.is_bot
            ORDER BY m.id DESC
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [(r["display_name"], r["content"]) for r in reversed(rows)]

    async def recent_bot_messages(self, chat_id: int, limit: int) -> list[str]:
        rows = await self._pool.fetch(
            """
            SELECT content FROM messages
            WHERE chat_id = $1 AND is_bot
            ORDER BY id DESC
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [r["content"] for r in rows]

    async def user_messages(self, chat_id: int, user_id: int, limit: int) -> list[str]:
        rows = await self._pool.fetch(
            """
            SELECT content FROM messages
            WHERE chat_id = $1 AND user_id = $2 AND NOT is_bot
            ORDER BY id DESC
            LIMIT $3
            """,
            chat_id, user_id, limit,
        )
        return [r["content"] for r in reversed(rows)]

    # -- users --------------------------------------------------------------

    async def find_chat_user(self, chat_id: int, username: str) -> UserRef | None:
        """Resolve @username to a human who has written in this chat."""
        row = await self._pool.fetchrow(
            """
            SELECT u.user_id, u.display_name, u.username
            FROM users u
            WHERE lower(u.username) = lower($2) AND NOT u.is_bot
              AND EXISTS (SELECT 1 FROM messages m WHERE m.chat_id = $1 AND m.user_id = u.user_id)
            ORDER BY u.updated_at DESC
            LIMIT 1
            """,
            chat_id, username,
        )
        return UserRef(row["user_id"], row["display_name"], row["username"]) if row else None

    async def get_user(self, user_id: int) -> UserRef | None:
        row = await self._pool.fetchrow(
            "SELECT user_id, display_name, username FROM users WHERE user_id = $1", user_id
        )
        return UserRef(row["user_id"], row["display_name"], row["username"]) if row else None

    # -- audit --------------------------------------------------------------

    async def pending_messages(self, chat_id: int, limit: int) -> list[PendingMessage]:
        """Human messages after the chat's audit cursor, oldest first."""
        rows = await self._pool.fetch(
            """
            SELECT m.id, m.user_id, u.display_name, m.content, m.created_at
            FROM messages m
            JOIN chats c ON c.chat_id = m.chat_id
            JOIN users u ON u.user_id = m.user_id
            WHERE m.chat_id = $1 AND m.id > c.audit_cursor AND NOT m.is_bot
            ORDER BY m.id
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [
            PendingMessage(r["id"], r["user_id"], r["display_name"], r["content"], r["created_at"])
            for r in rows
        ]

    async def count_pending(self, chat_id: int) -> int:
        return await self._pool.fetchval(
            """
            SELECT count(*)
            FROM messages m
            JOIN chats c ON c.chat_id = m.chat_id
            WHERE m.chat_id = $1 AND m.id > c.audit_cursor AND NOT m.is_bot
            """,
            chat_id,
        )

    async def chats_with_pending(self) -> list[int]:
        rows = await self._pool.fetch(
            """
            SELECT c.chat_id
            FROM chats c
            WHERE EXISTS (
                SELECT 1 FROM messages m
                WHERE m.chat_id = c.chat_id AND m.id > c.audit_cursor AND NOT m.is_bot
            )
            """
        )
        return [r["chat_id"] for r in rows]

    async def commit_audit(self, chat_id: int, cursor: int, violations: list[NewViolation]) -> int:
        """Store findings and advance the cursor atomically. Returns rows inserted."""
        async with self._pool.acquire() as conn, conn.transaction():
            inserted = 0
            for v in violations:
                status = await conn.execute(
                    """
                    INSERT INTO violations (message_id, article, fines, days)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    v.message_id, v.article, v.fines, v.days,
                )
                inserted += int(status.rsplit(" ", 1)[-1])
            await conn.execute(
                """
                UPDATE chats
                SET audit_cursor = GREATEST(audit_cursor, $2), last_audit_at = now()
                WHERE chat_id = $1
                """,
                chat_id, cursor,
            )
            return inserted

    async def last_audit_at(self, chat_id: int) -> datetime | None:
        return await self._pool.fetchval(
            "SELECT last_audit_at FROM chats WHERE chat_id = $1", chat_id
        )

    # -- violations ---------------------------------------------------------

    async def offender_stats(self, chat_id: int) -> list[OffenderStats]:
        rows = await self._pool.fetch(
            """
            SELECT
                m.user_id,
                u.display_name,
                count(*)                     AS total_count,
                sum(v.fines)                 AS total_fines,
                sum(v.days)                  AS total_days,
                max(m.created_at)            AS last_violation,
                count(*)     FILTER (WHERE m.created_at > now() - interval '24 hours') AS day_count,
                sum(v.fines) FILTER (WHERE m.created_at > now() - interval '24 hours') AS day_fines,
                sum(v.days)  FILTER (WHERE m.created_at > now() - interval '24 hours') AS day_days
            FROM violations v
            JOIN messages m ON m.id = v.message_id
            JOIN users u ON u.user_id = m.user_id
            WHERE m.chat_id = $1
            GROUP BY m.user_id, u.display_name
            """,
            chat_id,
        )
        return [
            OffenderStats(
                user_id=r["user_id"],
                display_name=r["display_name"],
                total_count=r["total_count"],
                total_fines=int(r["total_fines"] or 0),
                total_days=int(r["total_days"] or 0),
                last_violation=r["last_violation"],
                day_count=r["day_count"],
                day_fines=int(r["day_fines"] or 0),
                day_days=int(r["day_days"] or 0),
            )
            for r in rows
        ]

    async def user_violations(self, chat_id: int, user_id: int) -> list[ViolationRow]:
        rows = await self._pool.fetch(
            """
            SELECT m.created_at, m.content, v.article, v.fines, v.days
            FROM violations v
            JOIN messages m ON m.id = v.message_id
            WHERE m.chat_id = $1 AND m.user_id = $2
            ORDER BY m.created_at DESC
            """,
            chat_id, user_id,
        )
        return [
            ViolationRow(r["created_at"], r["content"], r["article"], r["fines"], r["days"])
            for r in rows
        ]

    async def reset_violations(self, chat_id: int) -> None:
        """Amnesty: forget all violations and treat every existing message as audited."""
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """
                DELETE FROM violations v
                USING messages m
                WHERE m.id = v.message_id AND m.chat_id = $1
                """,
                chat_id,
            )
            await conn.execute(_ENSURE_CHAT, chat_id)
            await conn.execute(
                """
                UPDATE chats
                SET audit_cursor  = COALESCE((SELECT max(id) FROM messages WHERE chat_id = $1), 0),
                    last_audit_at = now()
                WHERE chat_id = $1
                """,
                chat_id,
            )

    # -- scheduler ----------------------------------------------------------

    async def activity(self, chat_id: int, window_minutes: int) -> tuple[int, int]:
        """(human messages, distinct authors) in the last *window_minutes*."""
        row = await self._pool.fetchrow(
            """
            SELECT count(*) AS messages, count(DISTINCT user_id) AS users
            FROM messages
            WHERE chat_id = $1 AND NOT is_bot
              AND created_at > now() - make_interval(mins => $2)
            """,
            chat_id, window_minutes,
        )
        return row["messages"], row["users"]

    async def last_bot_message_at(self, chat_id: int) -> datetime | None:
        return await self._pool.fetchval(
            """
            SELECT created_at FROM messages
            WHERE chat_id = $1 AND is_bot
            ORDER BY id DESC
            LIMIT 1
            """,
            chat_id,
        )

    async def proactive_candidates(self, inactive_hours: int, today: date) -> list[int]:
        """Chats with recent human activity that haven't had today's remark yet."""
        rows = await self._pool.fetch(
            """
            SELECT c.chat_id
            FROM chats c
            WHERE (c.last_proactive_on IS NULL OR c.last_proactive_on < $2)
              AND EXISTS (
                  SELECT 1 FROM messages m
                  WHERE m.chat_id = c.chat_id AND NOT m.is_bot
                    AND m.created_at > now() - make_interval(hours => $1)
              )
            """,
            inactive_hours, today,
        )
        return [r["chat_id"] for r in rows]

    async def mark_proactive(self, chat_id: int, today: date) -> None:
        await self._pool.execute(
            "UPDATE chats SET last_proactive_on = $2 WHERE chat_id = $1", chat_id, today
        )

    async def random_citizen(self, chat_id: int) -> Citizen | None:
        """A random human active in the chat within 30 days, with their record."""
        row = await self._pool.fetchrow(
            """
            WITH active AS (
                SELECT DISTINCT user_id FROM messages
                WHERE chat_id = $1 AND NOT is_bot AND created_at > now() - interval '30 days'
            ), picked AS (
                SELECT user_id FROM active ORDER BY random() LIMIT 1
            )
            SELECT u.display_name,
                   COALESCE(sum(v.fines), 0) AS total_fines,
                   max(m.created_at) FILTER (WHERE v.id IS NOT NULL) AS last_violation
            FROM picked p
            JOIN users u ON u.user_id = p.user_id
            LEFT JOIN messages m ON m.chat_id = $1 AND m.user_id = p.user_id
            LEFT JOIN violations v ON v.message_id = m.id
            GROUP BY u.display_name
            """,
            chat_id,
        )
        if not row:
            return None
        return Citizen(row["display_name"], int(row["total_fines"]), row["last_violation"])
