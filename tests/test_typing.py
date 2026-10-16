import asyncio

import pytest
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import EditMessageText

from bot.ai import voice
from bot.ai.gemini import AIContentError, AIError, AIRateLimited, AITimeout
from bot.handlers import common
from bot.handlers.common import TYPING_FRAMES, replace_placeholder, typing


@pytest.mark.asyncio
async def test_typing_animates_until_body_finishes_and_answer_comes_last(monkeypatch):
    monkeypatch.setattr(common, "FRAME_INTERVAL", 0.01)
    edits = []

    async def edit(text):
        await asyncio.sleep(0.005)  # an edit in flight when the body ends must finish first
        edits.append(text)

    async with typing(edit):
        await asyncio.sleep(0.08)
    await replace_placeholder(edit, "ответ")
    assert edits[0] == TYPING_FRAMES[1] and len(edits) >= 3
    assert set(edits[:-1]) <= set(TYPING_FRAMES) and edits[-1] == "ответ"


@pytest.mark.asyncio
async def test_final_edit_waits_out_flood_control(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(common.asyncio, "sleep", fake_sleep)
    attempts = []

    async def edit(text):
        attempts.append(text)
        if len(attempts) == 1:
            raise TelegramRetryAfter(EditMessageText(text=text), "flood", retry_after=3)

    await replace_placeholder(edit, "ответ")
    assert attempts == ["ответ", "ответ"] and sleeps == [3]


@pytest.mark.parametrize("exc, kind", [
    (AIRateLimited("x"), "rate_limited"),
    (AITimeout("x"), "timeout"),
    (AIContentError("x"), "blocked"),
    (AIError("x"), "error"),
])
def test_fallback_reply_matches_failure(exc, kind):
    assert voice.fallback_reply(exc) in voice.FALLBACK_REPLIES[kind]
