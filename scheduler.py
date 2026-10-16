"""
Scheduler: two modes of unprompted messages from the Major.

1. REACTIVE — triggered after each incoming message when a live conversation
   is detected (5+ messages from 2+ users in the last N minutes).
   Fires with a configurable probability, at most once per cooldown period.

2. PROACTIVE — background loop sending one message per day to each active
   chat at a random time within the allowed window (09:00–22:00 MSK).
   Skips chats silent for longer than SCHEDULER_INACTIVE_HOURS.
"""

import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

from aiogram import Bot
from aiogram.types import LinkPreviewOptions
from psycopg2.extras import DictCursor

import config
import db
import major_voice
from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

_TZ_OFFSET = timedelta(hours=3)
_HOUR_START = 9
_HOUR_END   = 22

# Per-chat cooldown tracker: chat_id → datetime of last bot message
_last_sent: dict[int, datetime] = {}

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------
#
# The voice randomisation (mood/device/shape) and the no-repeat history
# block live in major_voice — they're shared with /stats and /summary so the
# Major speaks with one voice across the whole bot.


def _build_generic_prompt(recent_self: list[str]) -> str:
    angles = [
        "Поделись мыслью вслух — о чате, о гражданах, о службе или о жизни вообще.",
        "Выскажи недовольство подозрительной тишиной в чате.",
        "Ненавязчиво напомни, что наблюдение ведётся.",
        "Оброни что-то вроде «оперативной мудрости» к случаю.",
        "Посетуй на современных граждан в сравнении с прежними временами.",
        "Намекни на недавно заведённое дело — без подробностей.",
    ]
    return (
        f"{major_voice.PERSONA}\n\n"
        f"{major_voice.mood_block()}\n\n"
        f"Задача: {random.choice(angles)}\n"
        f"{major_voice.no_repeat_block(recent_self)}"
        "Выдай ровно одну реплику, без пояснений и вариантов."
    )


def _build_recent_prompt(history: str, recent_self: list[str]) -> str:
    return (
        f"{major_voice.PERSONA}\n\n"
        f"{major_voice.mood_block()}\n\n"
        "Ты только что прочитал последние сообщения в чате:\n"
        f"{history}\n\n"
        "Прокомментируй происходящее. Не пересказывай сообщения и не цитируй их — "
        "дай свою оценку, зацепись за одну деталь или тон беседы. "
        "Не обращайся к гражданам по имени, если они сами не представились в этих сообщениях.\n"
        f"{major_voice.no_repeat_block(recent_self)}"
        "Выдай ровно одну реплику, без пояснений и вариантов."
    )


def _build_citizen_prompt(name: str, days: int, recent_self: list[str]) -> str:
    return (
        f"{major_voice.PERSONA}\n\n"
        f"{major_voice.mood_block()}\n\n"
        f"Гражданин {name} уже {days} дней числится «чистым» — ни одного нарушения. "
        "Это подозрительно: либо он что-то замышляет, либо перешёл на шифрованную связь. "
        "Озвучь свои сомнения. Имя гражданина упомяни ровно один раз.\n"
        f"{major_voice.no_repeat_block(recent_self)}"
        "Выдай ровно одну реплику, без пояснений и вариантов."
    )


def _build_offender_prompt(name: str, fines: int, recent_self: list[str]) -> str:
    return (
        f"{major_voice.PERSONA}\n\n"
        f"{major_voice.mood_block()}\n\n"
        f"Гражданин {name} числится в реестре нарушителей, "
        f"суммарный штраф — {fines} рублей. "
        f"Напомни ему об этом. Сумму {fines} рублей укажи ровно такой, не округляй и не меняй. "
        "Имя гражданина упомяни ровно один раз.\n"
        f"{major_voice.no_repeat_block(recent_self)}"
        "Выдай ровно одну реплику, без пояснений и вариантов."
    )

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_active_chats() -> list[int]:
    """Return chat_ids with at least one message in the last SCHEDULER_INACTIVE_HOURS hours."""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT chat_id FROM messages
                WHERE timestamp > NOW() - %s::interval
                """,
                (f"{config.SCHEDULER_INACTIVE_HOURS} hours",),
            )
            return [r[0] for r in cur.fetchall()]


def _fetch_recent_messages(chat_id: int, limit: int = 10) -> list[str]:
    """Return last *limit* human messages from the chat."""
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content
                FROM messages m
                JOIN users u ON u.user_id = m.user_id
                WHERE m.chat_id = %s AND m.is_bot = FALSE
                ORDER BY m.timestamp DESC
                LIMIT %s
                """,
                (chat_id, limit),
            )
            rows = cur.fetchall()
    return [f"{r['display_name']}: {r['content']}" for r in reversed(rows)]


def _fetch_random_citizen(chat_id: int) -> dict | None:
    """Return a random human user from the chat with their violation stats."""
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
         """
         SELECT
            u.display_name,
            COALESCE(SUM(v.fines), 0) AS total_fines,
            MAX(v.created_at)          AS last_violation
         FROM users u
         LEFT JOIN violation_logs v
            ON v.user_id = u.user_id AND v.chat_id = %s
         WHERE u.user_id IN (
            SELECT DISTINCT user_id FROM messages
            WHERE chat_id = %s AND is_bot = FALSE
        )
        GROUP BY u.user_id, u.display_name
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (chat_id, chat_id),
    )
            row = cur.fetchone()
    if not row:
        return None
    last = row["last_violation"]
    if last and last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    clean_days = (datetime.now(tz=timezone.utc) - last).days if last else 999
    return {
        "name": row["display_name"],
        "fines": int(row["total_fines"]),
        "clean_days": clean_days,
    }


def _check_conversation_activity(chat_id: int) -> bool:
    """Return True if there are enough recent human messages to be called a conversation."""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT user_id)
                FROM messages
                WHERE chat_id = %s
                  AND is_bot = FALSE
                  AND timestamp > NOW() - %s::interval
                """,
                (chat_id, f"{config.SCHEDULER_ACTIVITY_WINDOW} minutes"),
            )
            msg_count, user_count = cur.fetchone()
    return msg_count >= config.SCHEDULER_ACTIVITY_MIN_MESSAGES and user_count >= 2


def _fetch_recent_self(chat_id: int, limit: int = 8) -> list[str]:
    """
    Return the Major's last *limit* own messages in this chat.
    Used to tell the model what it has already said so it doesn't repeat itself.
    """
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT content FROM messages
                WHERE chat_id = %s AND is_bot = TRUE
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (chat_id, limit),
            )
            return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Message generation
# ---------------------------------------------------------------------------

async def _generate_message(chat_id: int) -> str | None:
    """Generate a Major remark: generic / comment on recent messages / citizen mention."""
    scenario = random.choices(
        ["generic", "recent", "citizen"],
        weights=[3, 4, 3],
    )[0]

    # Pull the Major's own recent remarks so the prompt can tell the model
    # not to recycle them. Done once up front for all scenarios.
    recent_self = await db.run_in_thread(_fetch_recent_self, chat_id)

    if scenario == "generic":
        prompt = _build_generic_prompt(recent_self)
    elif scenario == "recent":
        msgs = await db.run_in_thread(_fetch_recent_messages, chat_id)
        prompt = (
            _build_recent_prompt("\n".join(msgs), recent_self)
            if msgs else _build_generic_prompt(recent_self)
        )
    else:
        citizen = await db.run_in_thread(_fetch_random_citizen, chat_id)
        if not citizen:
            prompt = _build_generic_prompt(recent_self)
        elif citizen["fines"] > 0:
            prompt = _build_offender_prompt(citizen["name"], citizen["fines"], recent_self)
        else:
            prompt = _build_citizen_prompt(citizen["name"], citizen["clean_days"], recent_self)

    result = await safe_generate_content(prompt)
    return result["text"].strip() if result.get("status") == "ok" else None


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

async def _send(bot: Bot, chat_id: int, text: str) -> None:
    await bot.send_message(
        chat_id,
        escape_html(text),
        parse_mode="HTML",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    _last_sent[chat_id] = datetime.now(tz=timezone.utc)

    # Persist the Major's own remark so future prompts can include it in the
    # "do not repeat" list. Failure here must not break message delivery.
    try:
        from handlers.message import save_bot_message
        bot_user = await bot.me()
        await db.run_in_thread(save_bot_message, chat_id, bot_user.id, text)
    except Exception:
        logger.exception("Не удалось сохранить реплику майора в БД (chat=%d).", chat_id)

    logger.info("Майор написал в chat=%d.", chat_id)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _is_active_hour() -> bool:
    now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
    return _HOUR_START <= now_msk.hour < _HOUR_END


def _seconds_until_window_open() -> float:
    now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
    next_start = now_msk.replace(hour=_HOUR_START, minute=0, second=0, microsecond=0)
    if now_msk >= next_start:
        next_start += timedelta(days=1)
    return (next_start - now_msk).total_seconds()


def _cooldown_ok(chat_id: int) -> bool:
    last = _last_sent.get(chat_id)
    if last is None:
        return True
    return (datetime.now(tz=timezone.utc) - last).total_seconds() >= config.SCHEDULER_COOLDOWN * 60


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def maybe_respond(bot: Bot, chat_id: int) -> None:
    """
    Reactive response: called after every incoming human message.
    Fires only if the chat is currently active and the cooldown has passed.
    """
    if not _is_active_hour():
        return
    if not _cooldown_ok(chat_id):
        return
    if random.random() > config.SCHEDULER_REACT_PROBABILITY:
        return
    active = await db.run_in_thread(_check_conversation_activity, chat_id)
    if not active:
        return
    try:
        text = await _generate_message(chat_id)
        if text:
            await _send(bot, chat_id, text)
    except Exception:
        logger.exception("Ошибка реактивного ответа в chat=%d.", chat_id)


async def scheduler_loop(bot: Bot) -> None:
    """
    Proactive loop: once per day, sends one message to each active chat
    at a random time within the allowed window (09:00–22:00 MSK).
    """
    logger.info("Планировщик запущен (окно %02d:00–%02d:00 МСК, 1 раз в сутки).", _HOUR_START, _HOUR_END)

    while True:
        if not _is_active_hour():
            wait = _seconds_until_window_open()
            logger.info("Планировщик: вне окна, ждём %.0f мин.", wait / 60)
            await asyncio.sleep(wait)
            continue

        # Sleep a random offset within the window before firing,
        # so the message doesn't always arrive right at 09:00.
        window_seconds = (_HOUR_END - _HOUR_START) * 3600
        delay = random.randint(0, window_seconds)
        logger.info("Планировщик: сегодня напишу через %.0f мин.", delay / 60)
        await asyncio.sleep(delay)

        # After the delay we may have left the window — check again.
        if not _is_active_hour():
            continue

        chat_ids = await db.run_in_thread(_fetch_active_chats)
        if not chat_ids:
            logger.info("Планировщик: нет активных чатов, пропускаем.")
        for chat_id in chat_ids:
            try:
                text = await _generate_message(chat_id)
                if text:
                    await _send(bot, chat_id, text)
            except Exception:
                logger.exception("Планировщик: ошибка в chat=%d.", chat_id)

        # Sleep until tomorrow's window opens.
        wait = _seconds_until_window_open()
        logger.info("Планировщик: следующий раз через %.0f мин.", wait / 60)
        await asyncio.sleep(wait)
