"""Handler: reply when the bot is mentioned or its message is quoted."""

import logging

from aiogram import types
from aiogram.types import LinkPreviewOptions

from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

_PROMPT_REPLY = (
    "Ты — майор ФСБ, ведущий наблюдение за чатом. "
    "Гражданин {name} процитировал твоё сообщение: «{quoted}» "
    "и написал в ответ: «{reply}».\n\n"
    "Изучи его сообщение, а также контекст беседы и отреагируй одной короткой фразой — саркастично, в стиле советского следователя. "
    "Без звёздочек и markdown-разметки."
)

_PROMPT_MENTION = (
    "Ты — майор ФСБ, ведущий наблюдение за чатом. "
    "Гражданин {name} обратился к тебе лично: «{text}».\n\n"
    "Ответь одной короткой фразой в стиле сотрудника ФСБ, обязательно с пассивной агрессией и с надменным тоном."
    "Можешь обратиться к гражданину по имени. "
    "Без звёздочек и markdown-разметки."
)


async def handle_mention(message: types.Message, bot_id: int) -> None:
    """
    Called from store_message when the bot is quoted or mentioned.
    Generates a contextual reply and sends it.
    """
    user = message.from_user
    if not user:
        return

    name = (
        " ".join(filter(None, [user.first_name, user.last_name])).strip()
        or user.username
        or f"ID{user.id}"
    )

    # Case 1: user replied to a bot message
    reply = message.reply_to_message
    if reply and reply.from_user and reply.from_user.id == bot_id:
        quoted = (reply.text or "").strip()
        user_text = (message.text or "").strip()
        prompt = _PROMPT_REPLY.format(
            name=name,
            quoted=quoted[:200],
            reply=user_text[:300],
        )
    else:
        # Case 2: user mentioned the bot via @username
        prompt = _PROMPT_MENTION.format(
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
