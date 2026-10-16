"""
Shared helpers for command handlers.

Provides a single context manager that encapsulates the pattern every
handler repeats:

    1. Send a "working..." status message to the chat.
    2. Run the command logic.
    3. Replace the status with the final result, OR with an error fallback
       if anything raises.

Handlers stay responsible for their own rendering and happy-path logic —
this module only takes the boilerplate.

Usage:

    async with status_message(message, "⚖️ Провожу аудит...",
                              error_text="❌ Ошибка при аудите.",
                              log_ctx="handle_stats chat=%d") as status:
        ...  # do work
        await status.finish("\n".join(lines), parse_mode="HTML")
"""

import logging
from contextlib import asynccontextmanager
from typing import Any

from aiogram import types
from aiogram.types import LinkPreviewOptions

logger = logging.getLogger(__name__)


class _StatusMessage:
    """Thin wrapper around aiogram's Message that tracks whether finish() was called."""

    __slots__ = ("_msg", "_finished")

    def __init__(self, msg: types.Message) -> None:
        self._msg = msg
        self._finished = False

    @property
    def finished(self) -> bool:
        return self._finished

    async def finish(self, text: str, **kwargs: Any) -> None:
        """Replace the status placeholder with the final response."""
        await self._msg.edit_text(text, **kwargs)
        self._finished = True

    async def replace(self, text: str, **kwargs: Any) -> None:
        """
        Replace the status text without marking the interaction as finished.
        Use for intermediate user-facing errors (validation, rate limits)
        that terminate the handler but shouldn't trigger the generic fallback.
        """
        await self._msg.edit_text(text, **kwargs)
        self._finished = True  # same effect — no fallback needed


@asynccontextmanager
async def status_message(
    message: types.Message,
    initial_text: str,
    *,
    error_text: str = "❌ Внутренняя ошибка.",
    log_ctx: str = "handler",
    parse_mode: str = "HTML",
):
    """
    Send a status placeholder, run the body, and guarantee a final edit.

    If the body raises, the exception is logged with log_ctx and the status
    message is replaced with error_text. The exception is not re-raised —
    Telegram handlers don't benefit from bubbling it up further.

    If the body exits normally but never called .finish()/.replace(),
    the status message is left as-is (the body may have chosen to delete
    or ignore it).
    """
    status_msg = await message.answer(
        initial_text,
        parse_mode=parse_mode,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    status = _StatusMessage(status_msg)
    try:
        yield status
    except Exception:
        logger.exception("Ошибка в %s (chat=%d).", log_ctx, message.chat.id)
        if not status.finished:
            try:
                await status_msg.edit_text(error_text, parse_mode=parse_mode)
            except Exception:
                # We really cannot do anything useful here — swallow so we
                # don't mask the original error in the log.
                logger.exception("Не удалось показать fallback-сообщение об ошибке.")
