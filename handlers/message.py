"""Handler: persist every non-command message to the DB."""

import logging

from aiogram import Bot, types

import db

logger = logging.getLogger(__name__)

# Bot instance injected at startup — set from main.py
_bot: Bot | None = None


def set_bot(bot: Bot) -> None:
    global _bot
    _bot = bot


def _build_display_name(user: types.User) -> str:
    full_name = " ".join(filter(None, [user.first_name, user.last_name])).strip()
    return full_name or user.username or f"ID{user.id}"


def _store(chat_id: int, user: types.User, text: str) -> None:
    display_name = _build_display_name(user)
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, display_name, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    username     = EXCLUDED.username,
                    first_name   = EXCLUDED.first_name,
                    last_name    = EXCLUDED.last_name,
                    display_name = EXCLUDED.display_name,
                    updated_at   = NOW()
                WHERE users.username    IS DISTINCT FROM EXCLUDED.username
                   OR users.first_name  IS DISTINCT FROM EXCLUDED.first_name
                   OR users.last_name   IS DISTINCT FROM EXCLUDED.last_name
                """,
                (user.id, user.username, user.first_name, user.last_name, display_name),
            )
            cur.execute(
                "INSERT INTO messages (chat_id, user_id, content) VALUES (%s, %s, %s)",
                (chat_id, user.id, text),
            )
        conn.commit()


async def store_message(message: types.Message) -> None:
    if not message.text or message.text.startswith("/"):
        return
    user = message.from_user
    if not user:
        return

    try:
        await db.run_in_thread(_store, message.chat.id, user, message.text)
    except Exception:
        logger.exception(
            "Ошибка при сохранении сообщения (chat=%d, user=%d).",
            message.chat.id, user.id,
        )
        return

    # Reactive response: try to have the Major join the conversation
    if _bot is not None:
        from scheduler import maybe_respond
        await maybe_respond(_bot, message.chat.id)
