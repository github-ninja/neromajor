"""
Catch-all for ordinary chat messages: persist them, answer when addressed,
otherwise let the Major decide whether to chime in. Must be the last router.
"""

import logging
import random
import time

from aiogram import Bot, F, Router, types
from aiogram.enums import ChatType

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.config import Settings
from bot.db.repo import Repository
from bot.services.major import Major
from bot.tg import display_name, escape

logger = logging.getLogger(__name__)
router = Router(name="messages")

_QUOTE_CHARS = 500
_TEXT_CHARS = 1000


class MentionLimiter:
    """At most one mention reply per user per *cooldown* seconds."""

    def __init__(self, cooldown: int) -> None:
        self._cooldown = cooldown
        self._last: dict[int, float] = {}

    def allow(self, user_id: int) -> bool:
        now = time.monotonic()
        if now - self._last.get(user_id, float("-inf")) < self._cooldown:
            return False
        self._last[user_id] = now
        if len(self._last) > 10_000:  # bound memory; stale entries are harmless to drop
            self._last = {u: t for u, t in self._last.items() if now - t < self._cooldown}
        return True


def _mentions(message: types.Message, username: str) -> bool:
    text = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []
    target = username.lower()
    for e in entities:
        if e.type == "mention" and e.extract_from(text).lstrip("@").lower() == target:
            return True
    return False


def is_addressed(message: types.Message, bot_id: int, bot_username: str) -> bool:
    if message.chat.type == ChatType.PRIVATE:
        return True
    reply = message.reply_to_message
    if reply and reply.from_user and reply.from_user.id == bot_id:
        return True
    return bool(bot_username) and _mentions(message, bot_username)


def _format_dialogue(lines) -> str:
    return "\n".join(f"{line.author or voice.BOT_NAME}: {line.content}" for line in lines)


async def _answer(
    message: types.Message, bot_id: int, text: str, repo: Repository, ai: Gemini, settings: Settings
) -> None:
    name = display_name(message.from_user)
    context = _format_dialogue(
        await repo.recent_dialogue(message.chat.id, settings.mention_context_messages)
    )
    reply = message.reply_to_message
    if reply and reply.from_user and reply.from_user.id == bot_id:
        prompt = prompts.MENTION_REPLY.format(
            bot_name=voice.BOT_NAME, context=context, name=name,
            quoted=(reply.text or reply.caption or "")[:_QUOTE_CHARS], text=text[:_TEXT_CHARS],
        )
    else:
        prompt = prompts.MENTION_DIRECT.format(
            bot_name=voice.BOT_NAME, context=context, name=name, text=text[:_TEXT_CHARS],
        )

    try:
        answer = await ai.text(prompt, system=voice.CONVERSATION_PERSONA)
    except AIError as exc:
        logger.warning("ИИ недоступен для ответа на упоминание (chat=%d): %s",
                       message.chat.id, exc.user_message)
        answer = random.choice(voice.FALLBACK_REPLIES)
    sent = await message.reply(escape(answer))
    await repo.save_bot_message(message.chat.id, bot_id, answer, sent.message_id)


@router.message(F.text | F.caption)
async def on_message(
    message: types.Message,
    bot: Bot,
    repo: Repository,
    ai: Gemini,
    major: Major,
    mentions: MentionLimiter,
    settings: Settings,
) -> None:
    text = message.text or message.caption or ""
    user = message.from_user
    # Commands for other bots, other bots' and channels' posts are not chat speech.
    if text.startswith("/") or not user or user.is_bot:
        return

    await repo.save_user_message(
        chat_id=message.chat.id,
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        display_name=display_name(user),
        content=text,
        tg_message_id=message.message_id,
    )

    me = await bot.me()
    if is_addressed(message, me.id, me.username or ""):
        if mentions.allow(user.id):
            await _answer(message, me.id, text, repo, ai, settings)
        return

    await major.maybe_react(message.chat.id)
