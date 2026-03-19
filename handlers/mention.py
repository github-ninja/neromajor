"""Handler: reply when the bot is mentioned or its message is quoted."""

import logging

from aiogram import Bot, types
from aiogram.types import LinkPreviewOptions
from psycopg2.extras import DictCursor

import config
import db
from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

# Bot name shown to AI so it can refer to itself correctly
_BOT_NAME = "Товарищ Майор"

_PROMPT_REPLY = (
    "Ты — майор ФСБ по имени «Товарищ Майор», ведущий наблюдение за чатом. "
    "Ниже приведена история беседы. Твои собственные реплики помечены как «{bot_name}».\n\n"
    "История беседы:\n{context}\n\n"
    "Гражданин {name} процитировал твоё сообщение: «{quoted}» "
    "и написал в ответ: «{reply}».\n\n"
    "Ответь в контексте этого разговора — саркастично, с пассивной агрессией, "
    "в стиле сотрудника ФСБ. Можешь развить мысль или задать встречный вопрос. "
    "Не повторяй то, что уже говорил. Без звёздочек и markdown-разметки."
    "Отвечай только своими словами, без префиксов вроде 'Товарищ Майор:' или эмодзи в начале. "
)

_PROMPT_MENTION = (
    "Ты — майор ФСБ по имени «Товарищ Майор», ведущий наблюдение за чатом. "
    "Ниже приведена история беседы. Твои собственные реплики помечены как «{bot_name}».\n\n"
    "История беседы:\n{context}\n\n"
    "Гражданин {name} обратился к тебе лично: «{text}».\n\n"
    "Ответь в контексте этого разговора — с пассивной агрессией и надменным тоном сотрудника ФСБ. "
    "Можешь обратиться к гражданину по имени, развить тему или задать встречный вопрос. "
    "Не повторяй то, что уже говорил. Без звёздочек и markdown-разметки."
    "Отвечай только своими словами, без префиксов вроде 'Товарищ Майор:' или эмодзи в начале. "
)


def _fetch_context(chat_id: int, bot_id: int, bot_display_name: str) -> str:
    """
    Return last MENTION_CONTEXT_MESSAGES messages as formatted dialogue,
    with bot messages labeled as bot_display_name.
    """
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content, m.is_bot
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
    lines = []
    for r in reversed(rows):
        speaker = bot_display_name if r["is_bot"] else r["display_name"]
        lines.append(f"{speaker}: {r['content']}")
    return "\n".join(lines)


async def handle_mention(message: types.Message, bot: Bot, bot_id: int) -> None:
    """
    Called from store_message when the bot is quoted or mentioned.
    Fetches conversation context including bot's own messages and replies.
    """
    user = message.from_user
    if not user:
        return

    name = (
        " ".join(filter(None, [user.first_name, user.last_name])).strip()
        or user.username
        or f"ID{user.id}"
    )

    context = await db.run_in_thread(_fetch_context, message.chat.id, bot_id, _BOT_NAME)

    reply = message.reply_to_message
    if reply and reply.from_user and reply.from_user.id == bot_id:
        quoted = (reply.text or "").strip()
        user_text = (message.text or "").strip()
        prompt = _PROMPT_REPLY.format(
            bot_name=_BOT_NAME,
            context=context,
            name=name,
            quoted=quoted[:200],
            reply=user_text[:300],
        )
    else:
        prompt = _PROMPT_MENTION.format(
            bot_name=_BOT_NAME,
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
        # Save bot reply to DB so it appears in future context
        from handlers.message import save_bot_message
        await db.run_in_thread(save_bot_message, message.chat.id, bot_id, text)
    except Exception:
        logger.exception("handle_mention: ошибка отправки ответа (chat=%d).", message.chat.id)
