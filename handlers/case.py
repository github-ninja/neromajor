"""Handler: /case @username — display full violation dossier for one user."""

import logging

from aiogram import types
from aiogram.filters import CommandObject
from psycopg2.extras import DictCursor

import db
from utils import escape_html

logger = logging.getLogger(__name__)

_TG_LIMIT = 4096
_FOOTER_RESERVE = 300
_CONTENT_MAX = 120


def _fetch_case(chat_id: int, username: str) -> tuple[dict[int, list], str] | None:
    """
    Find violations by username (case-insensitive, leading @ stripped).
    Returns (grouped_by_user_id, display_name) or None if not found.
    """
    username = username.lstrip("@").strip().lower()
    if not username:
        return None

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    v.user_id,
                    u.display_name,
                    v.created_at, v.content, v.article, v.fines, v.days
                FROM violation_logs v
                JOIN users u ON u.user_id = v.user_id
                WHERE u.username IS NOT NULL
                  AND LOWER(u.username) = %s
                  AND v.chat_id = %s
                ORDER BY v.user_id, v.created_at DESC
                """,
                (username, chat_id),
            )
            rows = cur.fetchall()

    if not rows:
        return None

    display_name = rows[0]["display_name"]
    grouped: dict[int, list] = {}
    for r in rows:
        grouped.setdefault(r["user_id"], []).append(r)

    return grouped, display_name


def _build_footer(severe_count: int, total: int, shown: int) -> str:
    if severe_count >= 3:
        note = (
            "Гражданин демонстрирует устойчивую склонность к системным нарушениям. "
            "Рекомендуется усиленный контроль и профилактическая беседа."
        )
    elif severe_count > 0:
        note = (
            "Отмечены отдельные опасные перегибы. "
            "Потенциал к исправлению присутствует."
        )
    else:
        note = (
            "Нарушения носят мелкий и хаотичный характер. "
            "Находится под стандартным наблюдением."
        )

    truncation_notice = (
        f"\n<i>Показано последних {shown} из {total} нарушений.</i>"
        if shown < total else ""
    )

    return (
        f"⎯⎯⎯⎯⎯⎯⎯⎯{truncation_notice}\n"
        f"🕵️ <b>ЗАМЕТКИ ТОВАРИЩА МАЙОРА:</b>\n{escape_html(note)}"
    )


def _render_violations(rows: list, budget: int) -> tuple[str, int, int]:
    """
    Render violation blocks fitting within *budget* characters.
    Returns (rendered_text, shown_count, total_count).
    """
    blocks: list[str] = []
    used = 0
    shown = 0

    for r in rows:
        sanctions: list[str] = []
        if r["fines"] > 0:
            sanctions.append(f"Штраф {r['fines']} ₽")
        if r["days"] > 0:
            sanctions.append(f"Срок {r['days']} дн.")
        sanction_str = ", ".join(sanctions) or "Санкция не назначена"

        date_str = r["created_at"].strftime("%d.%m.%Y %H:%M")
        article_part = f" ({escape_html(r['article'])})" if r["article"] else ""

        content = r["content"] or ""
        if len(content) > _CONTENT_MAX:
            content = content[:_CONTENT_MAX] + "…"

        block = (
            f"📅 <code>{escape_html(date_str)}</code> | "
            f"<code>{escape_html(content)}</code> | "
            f"<code>{escape_html(sanction_str)}{article_part}</code>\n\n"
        )

        if used + len(block) > budget:
            break

        blocks.append(block)
        used += len(block)
        shown += 1

    return "".join(blocks), shown, len(rows)


async def handle_case(message: types.Message, command: CommandObject) -> None:
    status_msg = await message.answer("📂 <b>Формирую досье гражданина...</b>", parse_mode="HTML")
    chat_id = message.chat.id

    target = command.args.strip() if command.args else None
    if not target:
        await status_msg.edit_text(
            "Использование: /case username\nПример: /case ivanov  или  /case @ivanov"
        )
        return

    try:
        result = await db.run_in_thread(_fetch_case, chat_id, target)

        if result is None:
            clean_target = target.lstrip("@")
            await status_msg.edit_text(
                f"Досье пользователя @{escape_html(clean_target)} отсутствует или у него нет нарушений.\n"
                "Убедитесь, что пользователь имеет username в Telegram и был проверен через /stats."
            )
            return

        grouped, display_name = result
        multi = len(grouped) > 1

        header = f"📂 <b>ДОСЬЕ: {escape_html(display_name)}</b>"
        if multi:
            header += f" <i>({len(grouped)} фигуранта с одинаковым username)</i>"

        all_sections: list[str] = [header, ""]

        for idx, (uid, rows) in enumerate(grouped.items(), start=1):
            severe_count = sum(1 for r in rows if r["days"] >= 10 or r["fines"] >= 50_000)

            if multi:
                all_sections.append(f"<b>— Фигурант #{idx} (ID: <code>{uid}</code>) —</b>")

            all_sections.append("🧾 <b>Зафиксированные нарушения:</b>\n")

            sub_budget = (_TG_LIMIT - _FOOTER_RESERVE - len(header)) // len(grouped)
            body, shown, total = _render_violations(rows, sub_budget)
            all_sections.append(body)
            all_sections.append(_build_footer(severe_count, total, shown))
            all_sections.append("")

        await status_msg.edit_text("\n".join(all_sections), parse_mode="HTML")

    except Exception:
        logger.exception("Ошибка в handle_case (chat=%d, target=%s).", chat_id, target)
        await status_msg.edit_text("❌ Ошибка при формировании досье.")
