"""Handler: /summary — per-participant sarcastic digest."""

import json
import logging

from aiogram import types
from aiogram.filters import CommandObject

import config
import db
from utils import clean_json_text, escape_html, safe_generate_content

logger = logging.getLogger(__name__)

_SUMMARY_PROMPT = (
    "Ты — подполковник спецслужб, осуществляющий оперативное сопровождение данного объекта (чата). "
    "Твоя задача: составить аналитическую справку, охватив КАЖДОГО без исключения участника переписки. "
    "Стиль: ядовитый канцелярский сарказм, пассивная агрессия и усталость от человеческой глупости.\n\n"
    
    "ПРАВИЛА СОСТАВЛЕНИЯ ХАРАКТЕРИСТИК:\n"
    "- Обязательно выдели КАЖДОГО фигуранта, зафиксированного в истории.\n"
    "- Для каждого напиши 1-2 предложения: в чем заключалась его роль, какие подозрительные тезисы он продвигал и под какую статью (реальную или вымышленную) это попадает.\n"
    "- Используй 'детали слежки': намекай на знание их реального местоположения, бытовых привычек или истории браузера.\n"
    "- Никакой вежливости. Ты здесь не гость, ты — неизбежность.\n\n"
    
    "ОБРАЗЕЦ ДЛЯ ПОДРАЖАНИЯ (JSON):\n"
    '{"topic": "Групповой сеанс коллективного саморазоблачения", '
    '"participants": {'
    '"Иван": "Типичный демагог. Пытался скрыть обсуждение закупки санкционных товаров под видом рецепта шарлотки. Кстати, Иван, камеру на ноутбуке заклеивать беспокоило еще в 2020-м, сейчас это просто смешно.", '
    '"Елена": "Молчаливый соучастник. Наблюдает, фиксирует, изредка поддакивает. Явная попытка сойти за свидетеля, но в нашей папке места хватит всем."}}\n\n'
    
    "ЗАДАНИЕ: Проанализируй историю ниже. Сформируй JSON строго по формату. "
    "Если в истории только один участник — не выдумывай второго. "
    "Не создавай фиктивных персонажей типа 'Неизвестный' или 'Аноним'.\n\n"
    "МАТЕРИАЛЫ ДЕЛА (ИСТОРИЯ ПЕРЕПИСКИ):\n"
)

_VERDICT_PROMPT = (
    "Ты майор ФСБ. Фигуранты дела обсуждали следующее: {topic}. "
    "Дай одну угрожающую саркастичную фразу о том, что ждёт участников за такие разговоры, "
    "но намекни, что вопрос можно решить при помощи взятки."
)


def _fetch_messages(chat_id: int, limit: int) -> list[tuple[str, str]]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.display_name, m.content
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.chat_id = %s
                ORDER BY m.timestamp DESC
                LIMIT %s
                """,
                (chat_id, limit),
            )
            return cur.fetchall()


async def handle_summary(message: types.Message, command: CommandObject) -> None:
    status_msg = await message.answer("📂 <b>Формирую оперативный рапорт...</b>", parse_mode="HTML")

    # Use config constants instead of magic numbers
    limit = min(
        int(command.args) if command.args and command.args.isdigit() else config.SUMMARY_DEFAULT_MESSAGES,
        config.SUMMARY_MAX_MESSAGES,
    )

    try:
        rows = await db.run_in_thread(_fetch_messages, message.chat.id, limit)

        if not rows:
            await status_msg.edit_text("❌ Нет данных.")
            return

        history = "\n".join(f"{name}: {text}" for name, text in reversed(rows))
        ai_res = await safe_generate_content(_SUMMARY_PROMPT + history)

        if ai_res.get("status") != "ok":
            await status_msg.edit_text(f"⚠️ {ai_res.get('message', 'Неизвестная ошибка ИИ')}")
            return

        try:
            data = json.loads(clean_json_text(ai_res["text"]))
        except json.JSONDecodeError:
            logger.exception("Не удалось распарсить JSON от Gemini (summary).")
            await status_msg.edit_text("⚠️ ИИ вернул некорректный формат ответа.")
            return

        topic: str = data.get("topic", "Обсуждение")
        participants: dict[str, str] = data.get("participants", {})

        verdict_res = await safe_generate_content(_VERDICT_PROMPT.format(topic=topic))
        verdict = verdict_res["text"].strip() if verdict_res.get("status") == "ok" else "Вердикт недоступен."

        lines = [
            "📝 <b>РАПОРТ ОБ ОБСТАНОВКЕ</b>",
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯",
            f"📍 <b>Объект наблюдения:</b> <code>{escape_html(topic)}</code>",
            "",
            "👥 <b>ФИГУРАНТЫ ДЕЛА:</b>",
        ]

        for name, profile in participants.items():
            lines.append(f"\n🔹 <b>{escape_html(name)}</b>\n{escape_html(profile)}")

        lines += [
            "",
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯",
            f"👮 <b>Товарищ Майор:</b>\n{escape_html(verdict)}",
            "",
            f"<i>Проанализировано сообщений: {len(rows)}</i>",
        ]

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception:
        logger.exception("Ошибка в handle_summary (chat=%d).", message.chat.id)
        await status_msg.edit_text("⚠️ Ошибка при формировании рапорта.")
