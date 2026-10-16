"""/stats — consolidated violation registry with the loyalty index."""

import logging
import random
from datetime import datetime, timezone

from aiogram import Router, types
from aiogram.filters import Command

from bot.ai import prompts, voice
from bot.ai.gemini import AIError, Gemini
from bot.config import Settings
from bot.db.repo import OffenderStats, Repository
from bot.handlers.common import status_message
from bot.services.audit import AuditOutcome, Auditor
from bot.services.loyalty import Loyalty, icon, loyalty_for
from bot.tg import escape, pack_blocks

logger = logging.getLogger(__name__)
router = Router(name="stats")

METHODOLOGY_URL = "https://telegra.ph/Nejromajor--logika-podscheta-narushenij-02-27"


def render_offender(p: OffenderStats, loyalty: Loyalty) -> str:
    lines = [f"{icon(loyalty.index)} <b>{escape(p.display_name)}</b>"]
    if p.day_count:
        lines.append(
            "⚡️ За последние 24 ч:\n"
            f"   └ Нарушений: <code>{p.day_count}</code> | "
            f"Штраф: <code>{p.day_fines} ₽</code> | Срок: <code>{p.day_days} дн.</code>"
        )
    else:
        lines.append("⚡️ За последние 24 ч: Чисто ✅")
    lines.append(
        "🗂 Всего за время наблюдения:\n"
        f"   └ Нарушений: <code>{p.total_count}</code>\n"
        f"   └ Штраф: <code>{p.total_fines} ₽</code> | Срок: <code>{p.total_days} дн.</code>"
    )
    if loyalty.clean_days > 0 and loyalty.index < 100:
        lines.append(
            f"   └ 🧼 Без нарушений: <code>{loyalty.clean_days} дн.</code> "
            f"(+{loyalty.daily_recovery} пт/день, бонус: +{loyalty.bonus})"
        )
    lines.append(f"   ┗ <b>Индекс патриотичности:</b> <code>{loyalty.index}%</code>")
    return "\n".join(lines)


def _opinion_context(p: OffenderStats, loyalty: Loyalty) -> str:
    if p.day_count:
        return (f"Гражданин {p.display_name} за последние сутки нарушил порядок "
                f"{p.day_count} раз(а). Штрафы растут.")
    if loyalty.clean_days > 0:
        return (f"Гражданин {p.display_name} не нарушает уже {loyalty.clean_days} дн. "
                "и встал на путь исправления.")
    return f"Гражданин {p.display_name} под наблюдением, пока ведёт себя тихо."


def _audit_note(outcome: AuditOutcome) -> str | None:
    if not outcome.pending:
        return None
    reason = "аудит идёт прямо сейчас" if outcome.busy else (
        outcome.error or "будут проверены в фоновом режиме"
    )
    return f"<i>⏳ Ожидают проверки: {outcome.pending} сообщ. ({escape(reason)})</i>"


@router.message(Command("stats"))
async def handle_stats(
    message: types.Message, repo: Repository, ai: Gemini, auditor: Auditor, settings: Settings
) -> None:
    chat_id = message.chat.id
    async with status_message(
        message, "⚖️ <b>Провожу аудит и сверку...</b>", "❌ Ошибка при аудите."
    ) as status:
        # Catch up on whatever the background audit hasn't reached yet, within
        # a small request allowance so /stats stays responsive.
        outcome = await auditor.audit_chat(chat_id, settings.audit_stats_requests, wait=False)

        offenders = await repo.offender_stats(chat_id)
        note = _audit_note(outcome)
        if not offenders:
            await status.finish("\n".join(filter(None, ["✅ Нарушений не зафиксировано.", note])))
            return

        now = datetime.now(timezone.utc)
        scored = sorted(
            ((p, loyalty_for(p, now)) for p in offenders), key=lambda pair: pair[1].index
        )

        header = ["🚨 <b>СВОДНЫЙ РЕЕСТР</b> 🚨"]
        last_audit = await repo.last_audit_at(chat_id)
        if last_audit:
            local = last_audit.astimezone(settings.timezone)
            header.append(f"<i>Последний аудит: {local:%d.%m.%Y %H:%M} ({settings.timezone.key})</i>")
        if note:
            header.append(note)

        target, target_loyalty = random.choice(scored)
        recent = await repo.recent_bot_messages(chat_id, 5)
        try:
            opinion = await ai.text(
                voice.remark_prompt(
                    prompts.STATS_OPINION_TASK.format(context=_opinion_context(target, target_loyalty)),
                    recent,
                ),
                system=voice.PERSONA,
                deadline=settings.gemini_reply_deadline,
            )
        except AIError:
            opinion = "Майор временно недоступен. Подождите минуту."

        footer = (
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            f"<b>ОСОБОЕ МНЕНИЕ:</b>\n{escape(opinion)}\n\n"
            f'<a href="{METHODOLOGY_URL}">📋 Методика расчёта санкций</a>'
        )
        blocks = ["\n".join(header)] + [render_offender(p, l) for p, l in scored] + [footer]
        await status.finish(*pack_blocks(blocks, sep="\n\n"))
