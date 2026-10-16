"""/profile — psychological dossier in the style of a KGB analyst."""

from aiogram import Router, types
from aiogram.filters import Command, CommandObject

from bot.ai import prompts
from bot.ai.gemini import AIError, Gemini
from bot.config import Settings
from bot.db.repo import Repository
from bot.handlers.common import resolve_target, status_message
from bot.tg import escape, plain_to_html_messages

router = Router(name="profile")

USAGE = (
    "Использование: /profile username, или ответьте командой /profile на сообщение гражданина.\n"
    "Пример: /profile ivanov  или  /profile @ivanov"
)


@router.message(Command("profile"))
async def handle_profile(
    message: types.Message,
    command: CommandObject,
    repo: Repository,
    ai: Gemini,
    settings: Settings,
) -> None:
    async with status_message(
        message, "🗂 <b>Поднимаю личное дело...</b>", "❌ Ошибка при составлении досье."
    ) as status:
        user, label = await resolve_target(message, command, repo)
        if label is None:
            await status.finish(USAGE)
            return
        messages = (
            await repo.user_messages(message.chat.id, user.user_id, settings.profile_max_messages)
            if user else []
        )
        if not messages:
            await status.finish(f"Сообщений от {escape(label)} в этом чате не найдено.")
            return

        try:
            profile = await ai.text(
                prompts.PROFILE_USER.format(name=user.display_name) + "\n".join(messages),
                system=prompts.PROFILE_SYSTEM,
            )
        except AIError as exc:
            await status.finish(f"⚠️ {escape(exc.user_message)}")
            return

        header = (
            f"📁 <b>ЛИЧНОЕ ДЕЛО: {escape(user.display_name)}</b>\n"
            f"<i>Проанализировано сообщений: {len(messages)}</i>\n"
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n\n"
        )
        await status.finish(*plain_to_html_messages(header, profile))
