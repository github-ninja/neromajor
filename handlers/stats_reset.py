"""Handler: /stats_reset — wipe violation history (admins only)."""

import logging

from aiogram import types
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest

import db

logger = logging.getLogger(__name__)

_ADMIN_STATUSES = {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}


async def _is_admin(message: types.Message) -> bool:
    if message.chat.type == "private":
        return True
    try:
        member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in _ADMIN_STATUSES
    except TelegramBadRequest:
        logger.warning(
            "Не удалось проверить права администратора (chat=%d, user=%d).",
            message.chat.id, message.from_user.id,
        )
        return False


def _reset(chat_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM violation_logs WHERE chat_id = %s", (chat_id,))
            # Advance checkpoint to NOW() instead of deleting it.
            # This way the next /stats only looks at messages written AFTER
            # the amnesty — previously seen messages won't be re-analysed.
            cur.execute(
                """
                INSERT INTO stats_checkpoint (chat_id, last_check) VALUES (%s, NOW())
                ON CONFLICT (chat_id) DO UPDATE SET last_check = NOW()
                """,
                (chat_id,),
            )
        conn.commit()


async def handle_stats_reset(message: types.Message) -> None:
    if not await _is_admin(message):
        await message.answer(
            "🚫 <b>Недостаточно полномочий.</b>\n"
            "Амнистию может объявить только администратор чата.",
            parse_mode="HTML",
        )
        return

    try:
        await db.run_in_thread(_reset, message.chat.id)
        await message.answer("⚖️ <b>Амнистия объявлена.</b> Журнал нарушений очищен.", parse_mode="HTML")
    except Exception:
        logger.exception("Ошибка в handle_stats_reset (chat=%d).", message.chat.id)
        await message.answer("❌ Ошибка при амнистии.")
