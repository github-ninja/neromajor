"""/case — full violation dossier of one citizen."""

from aiogram import Router, types
from aiogram.filters import Command, CommandObject

from bot.config import Settings
from bot.db.repo import Repository, ViolationRow
from bot.handlers.common import resolve_target, status_message
from bot.tg import TG_LIMIT, escape, pack_blocks

router = Router(name="case")

_CONTENT_MAX = 120
_MAX_MESSAGES = 3  # cap on the number of Telegram messages for one dossier
_SEVERE_FINES = 50_000
_SEVERE_DAYS = 10

USAGE = (
    "Использование: /case username, или ответьте командой /case на сообщение гражданина.\n"
    "Пример: /case ivanov  или  /case @ivanov"
)


def _note(severe: int) -> str:
    if severe >= 3:
        return ("Гражданин демонстрирует устойчивую склонность к системным нарушениям. "
                "Рекомендуется усиленный контроль и профилактическая беседа.")
    if severe > 0:
        return "Отмечены отдельные опасные перегибы. Потенциал к исправлению присутствует."
    return "Нарушения носят мелкий и хаотичный характер. Находится под стандартным наблюдением."


def render_violation(v: ViolationRow, settings: Settings) -> str:
    sanctions = []
    if v.fines > 0:
        sanctions.append(f"Штраф {v.fines} ₽")
    if v.days > 0:
        sanctions.append(f"Срок {v.days} дн.")
    sanction = ", ".join(sanctions) or "Санкция не назначена"
    article = f" ({escape(v.article)})" if v.article else ""
    content = v.content if len(v.content) <= _CONTENT_MAX else v.content[:_CONTENT_MAX] + "…"
    when = v.committed_at.astimezone(settings.timezone)
    return (
        f"📅 <code>{when:%d.%m.%Y %H:%M}</code> | "
        f"<code>{escape(content)}</code> | "
        f"<code>{escape(sanction)}{article}</code>"
    )


@router.message(Command("case"))
async def handle_case(
    message: types.Message, command: CommandObject, repo: Repository, settings: Settings
) -> None:
    async with status_message(
        message, "📂 <b>Формирую досье гражданина...</b>", "❌ Ошибка при формировании досье."
    ) as status:
        user, label = await resolve_target(message, command, repo)
        if label is None:
            await status.finish(USAGE)
            return
        violations = await repo.user_violations(message.chat.id, user.user_id) if user else []
        if not violations:
            await status.finish(
                f"Досье на {escape(label)} отсутствует или у гражданина нет нарушений."
            )
            return

        severe = sum(1 for v in violations if v.days >= _SEVERE_DAYS or v.fines >= _SEVERE_FINES)
        header = (f"📂 <b>ДОСЬЕ: {escape(user.display_name)}</b>\n\n"
                  "🧾 <b>Зафиксированные нарушения:</b>")

        # Newest first; keep as many as fit into _MAX_MESSAGES messages.
        budget = TG_LIMIT * _MAX_MESSAGES - len(header) - 600
        rendered: list[str] = []
        for v in violations:
            block = render_violation(v, settings)
            budget -= len(block) + 2
            if budget < 0:
                break
            rendered.append(block)

        truncation = (f"<i>Показано последних {len(rendered)} из {len(violations)} нарушений.</i>\n"
                      if len(rendered) < len(violations) else "")
        footer = (f"⎯⎯⎯⎯⎯⎯⎯⎯\n{truncation}"
                  f"🕵️ <b>ЗАМЕТКИ ТОВАРИЩА МАЙОРА:</b>\n{escape(_note(severe))}")
        await status.finish(*pack_blocks([header, *rendered, footer], sep="\n\n"))
