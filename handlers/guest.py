"""Handler: reply to guest_message updates (bot mentioned outside its chats)."""

import logging

from aiogram.types import InlineQueryResultArticle, InputTextMessageContent, LinkPreviewOptions, Message

from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

_BOT_NAME = "Товарищ Майор"

# Guest mode is stateless: no DB history available, one reply per invocation.
# The prompt is self-contained — we pass the summoning message and, when
# present, the message it was replying to as context.
_PROMPT = (
    "Ты — майор ФСБ по имени «Товарищ Майор», ведущий наблюдение за гражданами. "
    "Тебя вызвали в чужой чат — ты туда не приглашён, но явился по долгу службы. "
    "{reply_context}"
    "Гражданин {name} обратился к тебе: «{text}».\n\n"
    "Ответь саркастично, с пассивной агрессией, в стиле советского следователя. "
    "Можешь отметить, что тебя вырвали из оперативной работы по пустякам. "
    "Не задавай вопросов. "
    "Отвечай только своими словами, без префиксов вроде 'Товарищ Майор:' или эмодзи в начале. "
    "Без звёздочек и markdown-разметки. "
    "Ответ — не более двух-трёх коротких предложений, не растекайся мыслью."
)

_REPLY_CONTEXT_BLOCK = (
    "Контекст: гражданин процитировал сообщение «{quoted}» и добавил своё. "
)


async def handle_guest_message(message: Message) -> None:
    """
    Called for guest_message updates — bot was @mentioned in a chat it doesn't belong to.
    Telegram allows exactly one reply via answerGuestQuery; no DB access available.
    """
    logger.info(
        "guest_message получен: chat=%s, user=%s, text=%r, guest_query_id=%s",
        getattr(message.chat, "id", None),
        getattr(message.from_user, "id", None),
        (message.text or "")[:50],
        message.guest_query_id,
    )
    if not message.guest_query_id:
        logger.warning("handle_guest_message: guest_query_id отсутствует, пропускаем.")
        return

    user = message.guest_bot_caller_user or message.from_user
    if not user:
        return

    name = (
        " ".join(filter(None, [user.first_name, user.last_name])).strip()
        or user.username
        or f"ID{user.id}"
    )

    text = (message.text or "").strip()

    # If the guest mention was itself a reply, include that context
    reply_context = ""
    if message.reply_to_message and message.reply_to_message.text:
        quoted = message.reply_to_message.text.strip()[:200]
        reply_context = _REPLY_CONTEXT_BLOCK.format(quoted=quoted)

    prompt = _PROMPT.format(
        name=name,
        text=text[:300],
        reply_context=reply_context,
    )

    result = await safe_generate_content(prompt)
    if result.get("status") != "ok":
        logger.warning("handle_guest_message: AI недоступен, пропускаем ответ.")
        return

    reply_text = result["text"].strip()
    try:
        await message.answer_guest_query(
            result=InlineQueryResultArticle(
                id="reply",
                title=_BOT_NAME,
                input_message_content=InputTextMessageContent(
                    message_text=escape_html(reply_text),
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                ),
            ),
        )
        logger.info(
            "Майор ответил на гостевой вызов от %s (guest_query_id=%s).",
            name,
            message.guest_query_id,
        )
    except Exception:
        logger.exception(
            "handle_guest_message: ошибка при ответе на guest_query_id=%s.",
            message.guest_query_id,
        )
