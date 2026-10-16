"""Guest mode: the bot is @mentioned in a chat it isn't a member of."""

import logging
from datetime import datetime

from aiogram import Bot, Router
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent, Message

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.config import Settings
from bot.handlers.common import TYPING, replace_placeholder, typing
from bot.tg import display_name, escape

logger = logging.getLogger(__name__)
router = Router(name="guest")


@router.guest_message()
async def handle_guest_message(
    message: Message, bot: Bot, ai: Gemini, settings: Settings
) -> None:
    """
    Stateless: no history is stored for foreign chats. Telegram accepts
    exactly one answer per guest_query_id, so the answer is the typing
    placeholder, sent at once and then edited as an inline message.
    """
    if not message.guest_query_id:
        return
    user = message.guest_bot_caller_user or message.from_user
    if not user:
        return
    logger.info("Гостевой вызов (guest_query_id=%s, chat=%s, user=%d).",
                message.guest_query_id, message.chat.id, user.id)

    reply_context = ""
    reply = message.reply_to_message
    if reply and (reply.text or reply.caption):
        quoted = (reply.text or reply.caption).strip()[:500]
        reply_context = prompts.GUEST_REPLY_CONTEXT.format(quoted=quoted)

    prompt = prompts.GUEST.format(
        today=f"{datetime.now(settings.timezone):%d.%m.%Y}",
        name=display_name(user),
        text=(message.text or message.caption or "").strip()[:1000],
        reply_context=reply_context,
    )
    sent = await message.answer_guest_query(
        result=InlineQueryResultArticle(
            id="reply",
            title=voice.BOT_NAME,
            input_message_content=InputTextMessageContent(message_text=TYPING),
        ),
    )

    async def edit(text: str) -> object:
        return await bot.edit_message_text(text, inline_message_id=sent.inline_message_id)

    async with typing(edit):
        try:
            answer = await ai.text(
                prompt, system=voice.CONVERSATION_PERSONA,
                deadline=settings.gemini_reply_deadline,
            )
        except AIError as exc:
            logger.warning("ИИ недоступен для гостевого вызова (%s): %s",
                           message.guest_query_id, exc.user_message)
            answer = voice.fallback_reply(exc)
    await replace_placeholder(edit, escape(answer))
    logger.info("Майор ответил на гостевой вызов (guest_query_id=%s).", message.guest_query_id)
