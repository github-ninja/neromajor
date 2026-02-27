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
    "Ты — саркастичный и ироничный майор ФСБ, составляющий оперативную сводку по фигурантам дела. "
    "Проанализируй историю переписки и для КАЖДОГО участника напиши одну-две фразы о том, "
    "чем он занимался в чате: о чём говорил, что отстаивал, какую роль играл — "
    "с пассивной агрессией в лучших традициях ведомства. "
    "Также определи общую тему беседы одним предложением.\n"
    'Ответь строго в формате JSON:\n{"topic": "общая тема одним предложением", '
    '"participants": {"Имя1": "характеристика", "Имя2": "характеристика"}}\n'
    "Если в истории только один участник — не придумывай второго. "
    "Не создавай фиктивных 'Неизвестный', 'Аноним' и подобных персонажей.\n"
    "История переписки:\n"
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
