"""Handler: persist every non-command message to the DB."""

import logging

from aiogram import Bot, types

import db

logger = logging.getLogger(__name__)

# Bot instance and id — injected at startup via set_bot()
_bot: Bot | None = None
_bot_id: int | None = None
_bot_username: str | None = None


def set_bot(bot: Bot, bot_id: int, bot_username: str) -> None:
    global _bot, _bot_id, _bot_username
    _bot = bot
    _bot_id = bot_id
    _bot_username = bot_username.lower()


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


def _is_mention(message: types.Message) -> bool:
    """Return True if the message mentions the bot via @username."""
    if not _bot_username or not message.entities:
        return False
    for entity in message.entities:
        if entity.type == "mention":
            mentioned = message.text[entity.offset: entity.offset + entity.length].lstrip("@").lower()
            if mentioned == _bot_username:
                return True
    return False


def _is_reply_to_bot(message: types.Message) -> bool:
    """Return True if the message is a reply to one of the bot's messages."""
    reply = message.reply_to_message
    return bool(reply and reply.from_user and reply.from_user.id == _bot_id)


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

    if _bot is None:
        return

    # Priority 1: direct mention or reply to bot — always respond
    if _is_mention(message) or _is_reply_to_bot(message):
        from handlers.mention import handle_mention
        await handle_mention(message, _bot_id)
        return

    # Priority 2: reactive scheduler response during active conversations
    from scheduler import maybe_respond
    await maybe_respond(_bot, message.chat.id)
