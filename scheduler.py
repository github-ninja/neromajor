"""Scheduler: periodic unprompted messages from the Major to all active chats."""

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

# ---------------------------------------------------------------------------
# Prompts — one picked at random each time
# ---------------------------------------------------------------------------

# Each prompt template may use {name} and/or {count} placeholders.
# Templates without placeholders are used as-is.

_PROMPTS_GENERIC = [
    (
        "Ты — майор ФСБ, который только что вернулся с оперативного совещания "
        "и решил поделиться мыслями с подопечными гражданами. "
        "Напиши одно короткое сообщение — саркастичное, с налётом угрозы, "
        "в духе советского следака. Без приветствий, сразу по существу."
    ),
    (
        "Ты — майор ФСБ. Сейчас ты наблюдаешь за чатом и тебе скучно — "
        "нарушений давно не было, граждане подозрительно тихие. "
        "Выскажи своё недовольство этой тишиной одной хлёсткой фразой."
    ),
    (
        "Ты — майор ФСБ. Напомни гражданам чата, что за ними ведётся наблюдение. "
        "Одна фраза, коротко, с иронией и лёгкой угрозой."
    ),
    (
        "Ты — майор ФСБ. Поделись одной 'мудростью' из своего оперативного опыта — "
        "что-то вроде народной мудрости, но в духе спецслужб."
    ),
]

_PROMPT_RECENT = (
    "Ты — майор ФСБ, который только что прочитал последние сообщения в чате. "
    "Вот они:\n{history}\n\n"
    "Прокомментируй происходящее одной короткой фразой — саркастично, "
    "с налётом угрозы, в стиле советского следователя. "
    "Не пересказывай сообщения, просто дай оценку."
)

_PROMPT_CITIZEN = (
    "Ты — майор ФСБ. Один из подопечных граждан — {name} — "
    "давно не попадал в реестр нарушений ({days} дней чист). "
    "Это подозрительно. Выскажи своё мнение об этом одной короткой фразой — "
    "саркастично, в стиле советского следователя."
)

_PROMPT_OFFENDER = (
    "Ты — майор ФСБ. Гражданин {name} числится в реестре нарушителей "
    "с суммарным штрафом {fines} рублей. "
    "Напомни ему об этом одной короткой угрожающей фразой."
)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_active_chats() -> list[int]:
    """Return all chat_ids that have at least one message in the DB."""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT chat_id FROM messages")
            return [r[0] for r in cur.fetchall()]


def _fetch_recent_messages(chat_id: int, limit: int = 10) -> list[str]:
    """Return the last *limit* messages from the chat."""
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
    """Return a random user who has messages in this chat."""
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


# ---------------------------------------------------------------------------
# Message generation
# ---------------------------------------------------------------------------

async def _generate_message(chat_id: int) -> str | None:
    """Pick a random scenario and generate a message for the chat."""
    scenario = random.choices(
        ["generic", "recent", "citizen"],
        weights=[3, 4, 3],
    )[0]

    if scenario == "generic":
        prompt = random.choice(_PROMPTS_GENERIC)

    elif scenario == "recent":
        msgs = await db.run_in_thread(_fetch_recent_messages, chat_id)
        if not msgs:
            prompt = random.choice(_PROMPTS_GENERIC)
        else:
            prompt = _PROMPT_RECENT.format(history="\n".join(msgs))

    else:  # citizen
        citizen = await db.run_in_thread(_fetch_random_citizen, chat_id)
        if not citizen:
            prompt = random.choice(_PROMPTS_GENERIC)
        elif citizen["fines"] > 0:
            prompt = _PROMPT_OFFENDER.format(
                name=citizen["name"],
                fines=citizen["fines"],
            )
        else:
            prompt = _PROMPT_CITIZEN.format(
                name=citizen["name"],
                days=citizen["clean_days"],
            )

    result = await safe_generate_content(prompt)
    if result.get("status") == "ok":
        return result["text"].strip()
    return None


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

def _is_active_hour() -> bool:
    """Return True if current Moscow time is within the allowed window."""
    now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
    return _HOUR_START <= now_msk.hour < _HOUR_END


def _seconds_until_start() -> float:
    """Return seconds until 09:00 MSK if we're currently outside the window."""
    now_msk = datetime.now(tz=timezone.utc) + _TZ_OFFSET
    next_start = now_msk.replace(hour=_HOUR_START, minute=0, second=0, microsecond=0)
    if now_msk >= next_start:
        next_start += timedelta(days=1)
    return (next_start - now_msk).total_seconds()


async def scheduler_loop(bot: Bot) -> None:
    """
    Main scheduler loop. Sends one message to every active chat,
    then sleeps for a random interval within the configured range.
    """
    logger.info("Планировщик запущен (окно %02d:00–%02d:00 МСК).", _HOUR_START, _HOUR_END)

    while True:
        if not _is_active_hour():
            wait = _seconds_until_start()
            logger.info(
                "Планировщик: за пределами активного окна, ждём %.0f мин.",
                wait / 60,
            )
            await asyncio.sleep(wait)
            continue

        # Send to all active chats
        chat_ids = await db.run_in_thread(_fetch_active_chats)
        for chat_id in chat_ids:
            try:
                text = await _generate_message(chat_id)
                if text:
                    await bot.send_message(
                        chat_id,
                        f"🕵️ {escape_html(text)}",
                        parse_mode="HTML",
                    )
                    logger.info("Планировщик: отправил сообщение в chat=%d.", chat_id)
            except Exception:
                logger.exception("Планировщик: ошибка отправки в chat=%d.", chat_id)

        # Sleep for a random interval within the configured range
        interval = random.randint(
            config.SCHEDULER_INTERVAL_MIN * 60,
            config.SCHEDULER_INTERVAL_MAX * 60,
        )
        logger.info("Планировщик: следующее сообщение через %.0f мин.", interval / 60)
        await asyncio.sleep(interval)
