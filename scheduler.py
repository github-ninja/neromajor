"""
Scheduler: two modes of unprompted messages from the Major.

1. REACTIVE — triggered after each incoming message when a conversation
   is detected (5+ messages from 2+ users in the last 10 minutes).
   Fires with 15% probability, but no more than once per 30 minutes per chat.

2. PROACTIVE — a background loop that sends 2–3 initiative messages per day
   to each active chat during the allowed time window (09:00–22:00 MSK),
   regardless of chat activity.
"""

import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

from aiogram import Bot
from psycopg2.extras import DictCursor

import config
import db
from utils import safe_generate_content, escape_html

logger = logging.getLogger(__name__)

# Moscow time = UTC+3
_TZ_OFFSET = timedelta(hours=3)
_HOUR_START = 9   # 09:00 MSK
_HOUR_END   = 22  # 22:00 MSK

# Per-chat cooldown tracker: chat_id → datetime of last bot message
_last_sent: dict[int, datetime] = {}

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_PROMPTS_GENERIC = [
    (
        "Ты — майор ФСБ, который только что вернулся с оперативного совещания "
        "и решил поделиться мыслями с подопечными гражданами. "
        "Напиши одно короткое сообщение — саркастичное, с налётом угрозы, "
        "в духе советского следака. Без приветствий, сразу по существу. "
        "Без звёздочек и markdown-разметки."
    ),
    (
        "Ты — майор ФСБ. Сейчас ты наблюдаешь за чатом и тебе скучно — "
        "нарушений давно не было, граждане подозрительно тихие. "
        "Выскажи своё недовольство этой тишиной одной хлёсткой фразой. "
        "Без звёздочек и markdown-разметки."
    ),
    (
        "Ты — майор ФСБ. Напомни гражданам чата, что за ними ведётся наблюдение. "
        "Одна фраза, коротко, с иронией и лёгкой угрозой. "
        "Без звёздочек и markdown-разметки."
    ),
    (
        "Ты — майор ФСБ. Поделись одной 'мудростью' из своего оперативного опыта — "
        "что-то вроде народной мудрости, но в духе спецслужб. "
        "Без звёздочек и markdown-разметки."
    ),
]

_PROMPT_RECENT = (
    "Ты — майор ФСБ, который только что прочитал последние сообщения в чате. "
    "Вот они:\n{history}\n\n"
    "Прокомментируй происходящее одной короткой фразой — саркастично, "
    "с налётом угрозы, в стиле советского следователя. "
    "Не пересказывай сообщения, просто дай оценку. "
    "Без звёздочек и markdown-разметки."
)

_PROMPT_CITIZEN = (
    "Ты — майор ФСБ. Один из подопечных граждан — {name} — "
    "давно не попадал в реестр нарушений ({days} дней чист). "
    "Это подозрительно. Выскажи своё мнение об этом одной короткой фразой — "
    "саркастично, в стиле советского следователя. "
    "Без звёздочек и markdown-разметки."
)

_PROMPT_OFFENDER = (
    "Ты — майор ФСБ. Гражданин {name} числится в реестре нарушителей "
    "с суммарным штрафом {fines} рублей. "
    "Напомни ему об этом одной короткой угрожающей фразой. "
    "Без звёздочек и markdown-разметки."
)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_active_chats() -> list[int]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT chat_id FROM messages")
            return [r[0] for r in cur.fetchall()]


def _fetch_recent_messages(chat_id: int, limit: int = 10) -> list[str]:
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content
                FROM messages m
                JOIN users u ON u.user_id = m.user_id
                WHERE m.chat_id = %s
                ORDER BY m.timestamp DESC
                LIMIT %s
                """,
                (chat_id, limit),
            )
            rows = cur.fetchall()
    return [f"{r['display_name']}: {r['content']}" for r in reversed(rows)]


def _fetch_random_citizen(chat_id: int) -> dict | None:
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    u.display_name,
                    COALESCE(SUM(v.fines), 0) AS total_fines,
                    MAX(v.created_at)          AS last_violation
                FROM messages m
                JOIN users u ON u.user_id = m.user_id
                LEFT JOIN violation_logs v
                    ON v.user_id = m.user_id AND v.chat_id = m.chat_id
                WHERE m.chat_id = %s
                GROUP BY u.user_id, u.display_name
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (chat_id,),
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
        "fines": row["total_fines"],
        "clean_days": clean_days,
    }


def _check_conversation_activity(chat_id: int) -> bool:
    """
    Return True if there are 5+ messages from 2+ distinct users
    in the last SCHEDULER_ACTIVITY_WINDOW minutes.
    """
    window = timedelta(minutes=config.SCHEDULER_ACTIVITY_WINDOW)
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS msg_count, COUNT(DISTINCT user_id) AS user_count
                FROM messages
                WHERE chat_id = %s AND timestamp > NOW() - %s::interval
                """,
                (chat_id, f"{config.SCHEDULER_ACTIVITY_WINDOW} minutes"),
            )
            row = cur.fetchone()
    msg_count, user_count = row[0], row[1]
    return msg_count >= config.SCHEDULER_ACTIVITY_MIN_MESSAGES and user_count >= 2


# ---------------------------------------------------------------------------
# Message generation
# ---------------------------------------------------------------------------

async def _generate_message(chat_id: int) -> str | None:
    scenario = random.choices(
        ["generic", "recent", "citizen"],
        weights=[3, 4, 3],
    )[0]

    if scenario == "generic":
        prompt = random.choice(_PROMPTS_GENERIC)
    elif scenario == "recent":
        msgs = await db.run_in_thread(_fetch_recent_messages, chat_id)
        prompt = _PROMPT_RECENT.format(history="\n".join(msgs)) if msgs else random.choice(_PROMPTS_GENERIC)
    else:
        citizen = await db.run_in_thread(_fetch_random_citizen, chat_id)
        if not citizen:
            prompt = random.choice(_PROMPTS_GENERIC)
        elif citizen["fines"] > 0:
            prompt = _PROMPT_OFFENDER.format(name=citizen["name"], fines=citizen["fines"])
        else:
            prompt = _PROMPT_CITIZEN.format(name=citizen["name"], days=citizen["clean_days"])

    result = await safe_generate_content(prompt)
    return result["text"].strip() if result.get("status") == "ok" else None


async def _send(bot: Bot, chat_id: int) -> None:
    text = await _generate_message(chat_id)
    if text:
        await bot.send_message(
            chat_id,
            f"🕵️ {escape_html(text)}",
            parse_mode="HTML",
        )
        _last_sent[chat_id] = datetime.now(tz=timezone.utc)
        logger.info("Майор написал в chat=%d.", chat_id)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_active_hour() -> bool:
    now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
    return _HOUR_START <= now_msk.hour < _HOUR_END


def _cooldown_ok(chat_id: int) -> bool:
    """Return True if enough time has passed since the last bot message."""
    last = _last_sent.get(chat_id)
    if last is None:
        return True
    return (datetime.now(tz=timezone.utc) - last).total_seconds() >= config.SCHEDULER_COOLDOWN * 60


async def maybe_respond(bot: Bot, chat_id: int) -> None:
    """
    Called after every incoming message. Fires a reactive reply if:
    - within the allowed time window
    - conversation is active (5+ messages, 2+ users in last N minutes)
    - cooldown since last bot message has passed
    - random chance check passes
    """
    if not _is_active_hour():
        return
    if not _cooldown_ok(chat_id):
        return
    if random.random() > config.SCHEDULER_REACT_PROBABILITY:
        return
    # Heavy check last — only query DB if everything else passes
    active = await db.run_in_thread(_check_conversation_activity, chat_id)
    if not active:
        return

    try:
        await _send(bot, chat_id)
    except Exception:
        logger.exception("Ошибка реактивного ответа в chat=%d.", chat_id)


async def scheduler_loop(bot: Bot) -> None:
    """
    Proactive loop: 2–3 initiative messages per day per chat,
    sent at random times within the allowed window.
    """
    logger.info("Планировщик запущен (окно %02d:00–%02d:00 МСК).", _HOUR_START, _HOUR_END)

    while True:
        if not _is_active_hour():
            # Sleep until 09:00 MSK
            now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
            next_start = now_msk.replace(hour=_HOUR_START, minute=0, second=0, microsecond=0)
            if now_msk >= next_start:
                next_start += timedelta(days=1)
            wait = (next_start - now_msk).total_seconds()
            logger.info("Планировщик: вне окна, ждём %.0f мин.", wait / 60)
            await asyncio.sleep(wait)
            continue

        chat_ids = await db.run_in_thread(_fetch_active_chats)
        for chat_id in chat_ids:
            if _cooldown_ok(chat_id):
                try:
                    await _send(bot, chat_id)
                except Exception:
                    logger.exception("Планировщик: ошибка отправки в chat=%d.", chat_id)

        # 2–3 proactive messages per 13-hour window → interval ~3–5 hours
        interval = random.randint(180, 300) * 60
        logger.info("Планировщик: следующая инициатива через %.0f мин.", interval / 60)
        await asyncio.sleep(interval)
