"""/summary [N] — sarcastic per-participant digest of the last N messages."""

from aiogram import Router, types
from aiogram.filters import Command, CommandObject

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.ai.schemas import SummaryResult
from bot.config import Settings
from bot.db.repo import Repository
from bot.handlers.common import status_message
from bot.tg import escape, pack_blocks

router = Router(name="summary")


def parse_limit(args: str | None, settings: Settings) -> int:
    words = (args or "").split()
    limit = int(words[0]) if words and words[0].isdigit() else settings.summary_default_messages
    return max(1, min(limit, settings.summary_max_messages))


@router.message(Command("summary"))
async def handle_summary(
    message: types.Message,
    command: CommandObject,
    repo: Repository,
    ai: Gemini,
    settings: Settings,
) -> None:
    async with status_message(
        message, "📂 <b>Формирую оперативный рапорт...</b>", "⚠️ Ошибка при формировании рапорта."
    ) as status:
        rows = await repo.recent_human_messages(message.chat.id, parse_limit(command.args, settings))
        if not rows:
            await status.finish("❌ Нет данных.")
            return

        history = "\n".join(f"{name}: {text}" for name, text in rows)
        try:
            data = await ai.structured(
                prompts.SUMMARY_USER + history, SummaryResult, system=prompts.SUMMARY_SYSTEM
            )
        except AIError as exc:
            await status.finish(f"⚠️ {escape(exc.user_message)}")
            return

        topic = data.topic.strip() or "Обсуждение"
        try:
            verdict = await ai.text(
                voice.remark_prompt(prompts.SUMMARY_VERDICT_TASK.format(topic=topic)),
                system=voice.PERSONA,
            )
        except AIError:
            verdict = "Вердикт недоступен."

        header = (
            "📝 <b>РАПОРТ ОБ ОБСТАНОВКЕ</b>\n"
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            f"📍 <b>Объект наблюдения:</b> <code>{escape(topic)}</code>\n\n"
            "👥 <b>ФИГУРАНТЫ ДЕЛА:</b>"
        )
        # The model sometimes "finds" people who are merely mentioned in the chat.
        authors = {name for name, _ in rows}
        participants = [
            f"🔹 <b>{escape(p.name)}</b>\n{escape(p.characterization)}"
            for p in data.participants
            if p.name in authors
        ]
        footer = (
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            f"👮 <b>{voice.BOT_NAME}:</b>\n{escape(verdict)}\n\n"
            f"<i>Проанализировано сообщений: {len(rows)}</i>"
        )
        await status.finish(*pack_blocks([header, *participants, footer], sep="\n\n"))
