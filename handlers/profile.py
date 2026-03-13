"""Handler: /profile @username — psychological dossier in the style of a KGB major."""

import logging

from aiogram import types
from aiogram.filters import CommandObject
from aiogram.types import LinkPreviewOptions
from psycopg2.extras import DictCursor

import config
import db
from utils import escape_html, safe_generate_content

logger = logging.getLogger(__name__)

_PROFILE_PROMPT = (
    "Отвечай plain text без какого-либо форматирования: без звёздочек, без решёток, "
    "без подчёркиваний, без markdown-разметки любого вида. "
    "Заголовки разделов пиши ЗАГЛАВНЫМИ БУКВАМИ.\n\n"
    "Ты — аналитик ФСБ, составляющий оперативно-психологическую характеристику на фигуранта дела. "
    "Стиль — с едва сдерживаемой иронией. Как в личном деле подрзеваемого, "
    "но с пониманием, что объект — обычный участник интернет-чата. "
    "Никаких вводных фраз вроде 'На основании анализа' — сразу по существу.\n\n"
    "Составь характеристику по следующим разделам:\n\n"
    "1. ОБЩИЙ ПСИХОЛОГИЧЕСКИЙ ТИП\n"
    "Кто он в группе: лидер, наблюдатель, шут, провокатор, миротворец или что-то иное. "
    "Как себя подаёт, насколько это соответствует реальному положению в чате.\n\n"
    "2. РЕАКЦИЯ НА НЕСОГЛАСИЕ И ДАВЛЕНИЕ\n"
    "Спорит или уступает. Проявляет агрессию или уходит в молчание. "
    "Меняет позицию под давлением группы или стоит на своём.\n\n"
    "3. ИНТЕРЕСЫ И МИРОВОЗЗРЕНИЕ\n"
    "О чём говорит чаще всего. Есть ли устойчивые убеждения. "
    "Отношение к технологиям, деньгам, людям — если прослеживается.\n\n"
    "4. СОЦИАЛЬНОЕ ПОВЕДЕНИЕ\n"
    "Инициирует темы или подхватывает чужие. "
    "Есть ли характерные союзники или антагонисты в этом же чате. Если есть, то назови имена. "
    "Подстраивается под собеседника или общается одинаково со всеми.\n\n"
    "5. РЕЧЕВОЙ ПОРТРЕТ\n"
    "Многословен или лаконичен. Характер юмора — если есть. "
    "Если есть характерные словечки, манеры, речевые паттерны, то назови их.\n\n"
    "6. ОПЕРАТИВНАЯ ОЦЕНКА\n"
    "Управляем ли. Предсказуем ли. Потенциальная угроза или балласт. "
    "Одна финальная фраза-вердикт — как резолюция на деле.\n\n"
    "Переписка фигуранта:\n"
)


def _fetch_messages(chat_id: int, username: str, limit: int) -> tuple[list[str], str | None]:
    """
    Return (messages, display_name) for the given username in the chat.
    Messages are ordered chronologically (oldest first).
    Returns ([], None) if user not found.
    """
    username = username.lstrip("@").strip().lower()
    if not username:
        return [], None

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content
                FROM messages m
                JOIN users u ON u.user_id = m.user_id
                WHERE m.chat_id = %s
                  AND u.username IS NOT NULL
                  AND LOWER(u.username) = %s
                ORDER BY m.timestamp DESC
                LIMIT %s
                """,
                (chat_id, username, limit),
            )
            rows = cur.fetchall()

    if not rows:
        return [], None

    display_name = rows[0]["display_name"]
    # Reverse to chronological order for better AI context
    messages = [r["content"] for r in reversed(rows)]
    return messages, display_name


async def handle_profile(message: types.Message, command: CommandObject) -> None:
    status_msg = await message.answer("🗂 <b>Поднимаю личное дело...</b>", parse_mode="HTML")
    chat_id = message.chat.id

    target = command.args.strip() if command.args else None
    if not target:
        await status_msg.edit_text(
            "Использование: /profile username\nПример: /profile ivanov  или  /profile @ivanov"
        )
        return

    try:
        messages, display_name = await db.run_in_thread(
            _fetch_messages, chat_id, target, config.PROFILE_MAX_MESSAGES
        )

        if not messages:
            clean_target = target.lstrip("@")
            await status_msg.edit_text(
                f"Сообщений от @{escape_html(clean_target)} не найдено. "
                "Убедитесь, что пользователь писал в этот чат и у него задан username."
            )
            return

        history = "\n".join(messages)
        ai_res = await safe_generate_content(_PROFILE_PROMPT + history)

        if ai_res.get("status") != "ok":
            await status_msg.edit_text(f"⚠️ {ai_res.get('message', 'Неизвестная ошибка ИИ')}")
            return

        profile_text = ai_res["text"].strip()

        header = (
            f"📁 <b>ЛИЧНОЕ ДЕЛО: {escape_html(display_name)}</b>\n"
            f"<i>Проанализировано сообщений: {len(messages)}</i>\n"
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n\n"
        )

        await status_msg.edit_text(
            header + escape_html(profile_text),
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    except Exception:
        logger.exception("Ошибка в handle_profile (chat=%d, target=%s).", chat_id, target)
        await status_msg.edit_text("❌ Ошибка при составлении досье.")
