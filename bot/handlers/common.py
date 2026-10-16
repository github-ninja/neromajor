"""Shared handler plumbing: status placeholder, typing animation, target resolution."""

import asyncio
import itertools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from aiogram import types
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.filters import CommandObject

from bot.db.repo import Repository, UserRef

logger = logging.getLogger(__name__)

TYPING_FRAMES = ("✍️ <i>Печатает.</i>", "✍️ <i>Печатает..</i>", "✍️ <i>Печатает...</i>")
TYPING = TYPING_FRAMES[0]
# Each frame is a message edit; Telegram rate-limits edits per chat.
FRAME_INTERVAL = 3.0

Edit = Callable[[str], Awaitable[object]]


@asynccontextmanager
async def typing(edit: Edit) -> AsyncIterator[None]:
    """
    Animate a placeholder already showing TYPING while the body runs. The
    current frame edit is never cancelled, only awaited: a cancelled request
    may still reach Telegram after the real answer and overwrite it.
    """
    stop = asyncio.Event()

    async def animate() -> None:
        for frame in itertools.cycle(TYPING_FRAMES[1:] + TYPING_FRAMES[:1]):
            try:
                await asyncio.wait_for(stop.wait(), FRAME_INTERVAL)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await edit(frame)
            except TelegramAPIError as exc:
                logger.debug("Анимация остановлена: %s", exc)
                return

    task = asyncio.create_task(animate())
    try:
        yield
    finally:
        stop.set()
        await task


async def replace_placeholder(edit: Edit, text: str) -> None:
    """Final edit: survives one flood-control pause, which animation may have caused."""
    try:
        await edit(text)
    except TelegramRetryAfter as exc:
        await asyncio.sleep(exc.retry_after)
        await edit(text)


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
