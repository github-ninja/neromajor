"""Shared handler plumbing: status placeholder, multi-part replies, target resolution."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiogram import types
from aiogram.filters import CommandObject

from bot.db.repo import Repository, UserRef

logger = logging.getLogger(__name__)


class Status:
    """A "working..." message that is later replaced by the result."""

    def __init__(self, placeholder: types.Message) -> None:
        self._placeholder = placeholder
        self.done = False

    async def finish(self, *messages: str) -> None:
        """Replace the placeholder with the first message, send the rest after it."""
        first, *rest = messages
        await self._placeholder.edit_text(first)
        self.done = True
        for text in rest:
            await self._placeholder.answer(text)


@asynccontextmanager
async def status_message(
    message: types.Message, initial: str, error_text: str
) -> AsyncIterator[Status]:
    """
    Send a placeholder, run the body, guarantee a final state: if the body
    raises, the error is logged and the placeholder shows *error_text*.
    """
    status = Status(await message.answer(initial))
    try:
        yield status
    except Exception:
        logger.exception("Ошибка обработки команды %r (chat=%d).",
                         (message.text or "")[:40], message.chat.id)
        if not status.done:
            try:
                await status.finish(error_text)
            except Exception:
                logger.exception("Не удалось показать сообщение об ошибке.")


async def resolve_target(
    message: types.Message, command: CommandObject, repo: Repository
) -> tuple[UserRef | None, str | None]:
    """
    Who is the command about: the author of the replied-to message, a text
    mention, or @username from the arguments. Returns (user, label) where
    label is what the caller asked for (for "not found" replies), or
    (None, None) if no target was given at all.
    """
    reply = message.reply_to_message
    if reply and reply.from_user and not reply.from_user.is_bot and not command.args:
        user = await repo.get_user(reply.from_user.id)
        return user, reply.from_user.full_name

    for entity in message.entities or []:
        if entity.type == "text_mention" and entity.user:
            return await repo.get_user(entity.user.id), entity.user.full_name

    if command.args:
        username = command.args.split()[0].lstrip("@")
        if username:
            return await repo.find_chat_user(message.chat.id, username), f"@{username}"
    return None, None
