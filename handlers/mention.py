"""Handler: reply when the bot is mentioned or its message is quoted."""

import logging

from aiogram import types
from aiogram.types import LinkPreviewOptions
from psycopg2.extras import DictCursor

import config
import db
from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

_PROMPT_REPLY = (
    "Ты — майор ФСБ, ведущий наблюдение за чатом. "
    "Вот последние сообщения беседы для контекста:\n{context}\n\n"
    "Гражданин {name} процитировал твоё сообщение: «{quoted}» "
    "и написал в ответ: «{reply}».\n\n"
    "Отреагируй в контексте этого разговора настолько развернуто, насколько требует контекст — "
    "с пассивной агрессией и надменным тоном сотрудника ФСБ. "
    "Без звёздочек и markdown-разметки."
)

_PROMPT_MENTION = (
    "Ты — майор ФСБ, ведущий наблюдение за чатом. "
    "Вот последние сообщения беседы для контекста:\n{context}\n\n"
    "Гражданин {name} обратился к тебе лично: «{text}».\n\n"
    "Ответь одной короткой фразой в контексте этого разговора — "
    "с пассивной агрессией и надменным тоном сотрудника ФСБ. "
    "Можешь обратиться к гражданину по имени. "
    "Без звёздочек и markdown-разметки."
)


def _fetch_context(chat_id: int) -> str:
    """Return last MENTION_CONTEXT_MESSAGES messages as a formatted string."""
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content
                FROM messages m
                JOIN users u ON u.user_id = m.user_id
                WHERE m.chat_id = %s
                ORDER BY m.timestamp DESC
                LIMIT %s
                """,
                (chat_id, config.MENTION_CONTEXT_MESSAGES),
            )
            rows = cur.fetchall()
    if not rows:
        return ""
    return "\n".join(f"{r['display_name']}: {r['content']}" for r in reversed(rows))


async def handle_mention(message: types.Message, bot_id: int) -> None:
    """
    Called from store_message when the bot is quoted or mentioned.
    Fetches conversation context and generates a contextual reply.
    """
    user = message.from_user
    if not user:
        return

    name = (
        " ".join(filter(None, [user.first_name, user.last_name])).strip()
        or user.username
        or f"ID{user.id}"
    )

    context = await db.run_in_thread(_fetch_context, message.chat.id)

    reply = message.reply_to_message
    if reply and reply.from_user and reply.from_user.id == bot_id:
        quoted = (reply.text or "").strip()
        user_text = (message.text or "").strip()
        prompt = _PROMPT_REPLY.format(
            context=context,
            name=name,
            quoted=quoted[:200],
            reply=user_text[:300],
        )
    else:
        prompt = _PROMPT_MENTION.format(
            context=context,
            name=name,
            text=(message.text or "").strip()[:300],
        )

    result = await safe_generate_content(prompt)
    if result.get("status") != "ok":
        logger.warning("handle_mention: AI недоступен, пропускаем ответ.")
        return

    text = result["text"].strip()
    try:
        await message.reply(
            f"🕵️ {escape_html(text)}",
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except Exception:
        logger.exception("handle_mention: ошибка отправки ответа (chat=%d).", message.chat.id)
