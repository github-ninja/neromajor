"""/stats_reset — amnesty: wipe the chat's violation history (admins only)."""

import logging

from aiogram import Router, types
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command

from bot.db.repo import Repository

logger = logging.getLogger(__name__)
router = Router(name="stats_reset")

_ADMIN_STATUSES = {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}


async def is_admin(message: types.Message) -> bool:
    if message.chat.type == ChatType.PRIVATE:
        return True
    # Anonymous admins post on behalf of the chat itself.
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return True
    if not message.from_user:
        return False
    try:
        member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
    except TelegramAPIError:
        logger.warning("Не удалось проверить права администратора (chat=%d, user=%d).",
                       message.chat.id, message.from_user.id)
        return False
    return member.status in _ADMIN_STATUSES


@router.message(Command("stats_reset"))
async def handle_stats_reset(message: types.Message, repo: Repository) -> None:
    if not await is_admin(message):
        await message.answer(
            "🚫 <b>Недостаточно полномочий.</b>\n"
            "Амнистию может объявить только администратор чата."
        )
        return
    try:
        await repo.reset_violations(message.chat.id)
    except Exception:
        logger.exception("Ошибка амнистии (chat=%d).", message.chat.id)
        await message.answer("❌ Ошибка при амнистии.")
        return
    await message.answer("⚖️ <b>Амнистия объявлена.</b> Журнал нарушений очищен.")
