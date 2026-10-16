"""Guest mode: the bot is @mentioned in a chat it isn't a member of."""

import logging

from aiogram import Router
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent, Message

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.tg import display_name, escape

logger = logging.getLogger(__name__)
router = Router(name="guest")


@router.guest_message()
async def handle_guest_message(message: Message, ai: Gemini) -> None:
    """
    Stateless: no history is stored for foreign chats, and Telegram accepts
    exactly one answer per guest_query_id.
    """
    if not message.guest_query_id:
        return
    user = message.guest_bot_caller_user or message.from_user
    if not user:
        return

    reply_context = ""
    reply = message.reply_to_message
    if reply and (reply.text or reply.caption):
        quoted = (reply.text or reply.caption).strip()[:200]
        reply_context = prompts.GUEST_REPLY_CONTEXT.format(quoted=quoted)

    prompt = prompts.GUEST.format(
        name=display_name(user),
        text=(message.text or message.caption or "").strip()[:300],
        reply_context=reply_context,
    )
    try:
        answer = await ai.text(prompt, system=voice.PERSONA)
    except AIError as exc:
        logger.warning("Гостевой вызов без ответа (%s): %s", message.guest_query_id, exc.user_message)
        return

    await message.answer_guest_query(
        result=InlineQueryResultArticle(
            id="reply",
            title=voice.BOT_NAME,
            input_message_content=InputTextMessageContent(message_text=escape(answer)),
        ),
    )
    logger.info("Майор ответил на гостевой вызов (guest_query_id=%s).", message.guest_query_id)
