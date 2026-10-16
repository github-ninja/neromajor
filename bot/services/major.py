"""
The Major's unprompted remarks.

1. REACTIVE — after an incoming message, when a live conversation is going
   on (SCHEDULER_ACTIVITY_MIN_MESSAGES+ messages from 2+ people within
   SCHEDULER_ACTIVITY_WINDOW minutes), with SCHEDULER_REACT_PROBABILITY and
   at most once per SCHEDULER_COOLDOWN minutes since the bot last spoke.

2. PROACTIVE — one remark per chat per day at a random time inside the
   active window. The time is derived from (chat, date), and the day is
   marked in the DB, so restarts neither skip nor duplicate it.
"""

import asyncio
import hashlib
import logging
import random
from datetime import date, datetime, time, timedelta

from aiogram import Bot

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.config import Settings
from bot.db.repo import Repository
from bot.tg import escape

logger = logging.getLogger(__name__)

_TICK = 300  # seconds between proactive checks
_RECENT_SELF = 8
_RECENT_HISTORY = 10


def planned_time(chat_id: int, day: date, settings: Settings) -> datetime:
    """Deterministic pseudo-random moment inside the day's active window."""
    seed = hashlib.sha256(f"{chat_id}:{day.isoformat()}".encode()).digest()
    window = (settings.active_hour_end - settings.active_hour_start) * 3600
    offset = int.from_bytes(seed[:8], "big") % window
    start = datetime.combine(day, time(settings.active_hour_start), tzinfo=settings.timezone)
    return start + timedelta(seconds=offset)


class Major:
    def __init__(self, bot: Bot, repo: Repository, ai: Gemini, settings: Settings) -> None:
        self._bot = bot
        self._repo = repo
        self._ai = ai
        self._settings = settings
        self._reacting: set[int] = set()

    def _now(self) -> datetime:
        return datetime.now(self._settings.timezone)

    def _in_active_hours(self) -> bool:
        return self._settings.active_hour_start <= self._now().hour < self._settings.active_hour_end

    # -- generation ---------------------------------------------------------

    async def _remark_task(self, chat_id: int) -> str:
        scenario = random.choices(["generic", "recent", "citizen"], weights=[3, 4, 3])[0]
        if scenario == "recent":
            msgs = await self._repo.recent_human_messages(chat_id, _RECENT_HISTORY)
            if msgs:
                history = "\n".join(f"{name}: {text}" for name, text in msgs)
                return prompts.REMARK_RECENT.format(history=history)
        elif scenario == "citizen":
            citizen = await self._repo.random_citizen(chat_id)
            if citizen:
                if citizen.total_fines > 0:
                    return prompts.REMARK_OFFENDER.format(
                        name=citizen.display_name, fines=citizen.total_fines
                    )
                if citizen.last_violation is None:
                    return prompts.REMARK_NEVER_CAUGHT.format(name=citizen.display_name)
                days = (datetime.now(citizen.last_violation.tzinfo) - citizen.last_violation).days
                return prompts.REMARK_CLEAN_CITIZEN.format(name=citizen.display_name, days=days)
        return random.choice(prompts.REMARK_GENERIC_ANGLES)

    async def _generate(self, chat_id: int) -> str:
        task = await self._remark_task(chat_id)
        recent = await self._repo.recent_bot_messages(chat_id, _RECENT_SELF)
        return await self._ai.text(voice.remark_prompt(task, recent), system=voice.PERSONA)

    async def say(self, chat_id: int, text: str) -> None:
        sent = await self._bot.send_message(chat_id, escape(text))
        await self._repo.save_bot_message(chat_id, self._bot.id, text, sent.message_id)

    async def _speak(self, chat_id: int) -> bool:
        try:
            text = await self._generate(chat_id)
        except AIError as exc:
            logger.warning("Майор промолчал в chat=%d: %s", chat_id, exc.user_message)
            return False
        await self.say(chat_id, text)
        logger.info("Майор написал в chat=%d.", chat_id)
        return True

    # -- reactive -----------------------------------------------------------

    async def maybe_react(self, chat_id: int) -> None:
        if chat_id in self._reacting or not self._in_active_hours():
            return
        if random.random() >= self._settings.react_probability:
            return
        self._reacting.add(chat_id)
        try:
            last = await self._repo.last_bot_message_at(chat_id)
            if last and datetime.now(last.tzinfo) - last < timedelta(
                minutes=self._settings.react_cooldown
            ):
                return
            count, users = await self._repo.activity(chat_id, self._settings.activity_window)
            if count < self._settings.activity_min_messages or users < 2:
                return
            await self._speak(chat_id)
        except Exception:
            logger.exception("Ошибка реактивного ответа в chat=%d.", chat_id)
        finally:
            self._reacting.discard(chat_id)

    # -- proactive ----------------------------------------------------------

    async def run_forever(self) -> None:
        s = self._settings
        logger.info("Планировщик запущен (окно %02d:00–%02d:00 %s, 1 раз в сутки).",
                    s.active_hour_start, s.active_hour_end, s.timezone.key)
        while True:
            try:
                await self._tick()
            except Exception:
                logger.exception("Планировщик: непредвиденная ошибка.")
            await asyncio.sleep(_TICK)

    async def _tick(self) -> None:
        if not self._in_active_hours():
            return
        now = self._now()
        today = now.date()
        for chat_id in await self._repo.proactive_candidates(self._settings.inactive_hours, today):
            if now < planned_time(chat_id, today, self._settings):
                continue
            # Mark first: a failure costs one remark, never a duplicate.
            await self._repo.mark_proactive(chat_id, today)
            try:
                await self._speak(chat_id)
            except Exception:
                logger.exception("Планировщик: ошибка в chat=%d.", chat_id)
