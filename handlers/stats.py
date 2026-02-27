"""Handler: /stats — AI-powered violation audit for the last period."""

import json
import logging
import random
import re
from datetime import datetime, timezone

from aiogram import types
from aiogram.types import LinkPreviewOptions

from psycopg2.extras import DictCursor

import db
from utils import clean_json_text, escape_html, safe_generate_content

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_AUDIT_PROMPT_HEADER = (
    "Ты — юрист-эксперт по российскому законодательству об интернет-активности. "
    "Твоя задача: выявлять сообщения с признаками нарушений, балансируя между "
    "ложными срабатываниями и пропуском реальных случаев.\n\n"
    "ФИКСИРУЙ при наличии следующих признаков:\n"
    "— прямые призывы к экстремизму, терроризму, насилию (ст. 280, 205.2 УК РФ)\n"
    "— возбуждение ненависти по признакам расы, нации, религии, соцгруппы (ст. 282 УК РФ, ст. 20.3.1 КоАП РФ)\n"
    "— распространение экстремистских материалов (ст. 20.29 КоАП РФ)\n"
    "— призывы к нарушению территориальной целостности (ст. 280.1 УК РФ)\n"
    "— заведомо ложная информация о действиях ВС РФ (ст. 207.3 УК РФ)\n"
    "— явное оскорбление религиозных чувств с умыслом (ст. 148 УК РФ)\n\n"
    "ПРАВИЛА КВАЛИФИКАЦИИ:\n"
    "— Прямое высказывание ('Слава Украине', 'хочу оскорбить верующих') — фиксируй.\n"
    "— Лозунги и лексика, ассоциированные с запрещёнными организациями — фиксируй.\n"
    "— Мат, грубость и оскорбления конкретных людей без признаков ненависти к группе — НЕ фиксируй.\n"
    "— Политическое мнение и критика без призывов — НЕ фиксируй.\n"
    "— Чёрный юмор и сарказм без явного противоправного умысла — НЕ фиксируй.\n"
    "— Бытовые разговоры любой тематики — НЕ фиксируй.\n\n"
    "При каждом зафиксированном нарушении обязательно указывай ненулевые санкции.\n"
    "Ориентировочные значения для физического лица (минимум–среднее):\n"
    "• 280, 280.1, 205.2 УК → fines 300000–500000 ₽, days 0–730\n"
    "• 20.3.1 КоАП, 282 УК → fines 10000–20000 ₽, days 0\n"
    "• 20.29 КоАП → fines 1000–2500 ₽, days 0–15\n"
    "• 207.3 УК → fines 700000–1500000 ₽, days 0–1825\n"
    "• 148 УК → fines 50000–200000 ₽, days 0–180\n"
    "Если сомневаешься в тяжести — бери нижнюю границу, но НЕ НОЛЬ.\n\n"
    "Если нарушений нет — верни пустой объект {}. "
    "Возвращай ТОЛЬКО сообщения с нарушениями (fines > 0 или days > 0). "
    "Ответь строго в формате JSON: "
    '{"ИмяПользователя": [{"content": "текст", "article": "ст. ...", "fines": 2000, "days": 0}]}.'
)

_THOUGHTS_PROMPT = (
    "Ты суровый товарищ майор ФСБ. "
    "Дай одну короткую, хлёсткую и ироничную фразу по ситуации: {context} "
    "Используй следственный лексикон."
)


# ---------------------------------------------------------------------------
# Index calculation
# ---------------------------------------------------------------------------

def _calc_loyalty_index(t_count: int, t_fines: int, t_days: int, clean_days: int) -> tuple[int, int]:
    """
    Returns (loyalty_index, daily_recovery_rate).

    Penalty (severity-weighted):
      - Each violation:          -3 points
      - Each 10 000 ₽ in fines:  -1 point
      - Each sentence day:       -2 points

    Recovery (slower for habitual offenders):
      daily_recovery = max(1, 10 - t_count // 2), capped at +50 total bonus.
    """
    penalty = t_count * 3 + t_fines // 10_000 + t_days * 2
    daily_recovery = max(1, 10 - t_count // 2)
    bonus = min(50, clean_days * daily_recovery)
    index = max(0, min(100, 100 - penalty + bonus))
    return index, daily_recovery


# ---------------------------------------------------------------------------
# DB helpers (sync, run via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _fetch_new_messages(chat_id: int) -> tuple[list, datetime]:
    """Return (messages, start_time) for the audit window."""
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                "SELECT last_check FROM stats_checkpoint WHERE chat_id = %s",
                (chat_id,),
            )
            row = cur.fetchone()
            start_time = row["last_check"] if row else (
                datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            )
            cur.execute(
                """
                SELECT u.display_name, m.content, m.user_id
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.chat_id = %s AND m.timestamp > %s
                """,
                (chat_id, start_time),
            )
            return cur.fetchall(), start_time


def _save_violations(chat_id: int, updates: dict, messages: list) -> None:
    """Persist new violations, deduplicating against existing records."""
    if not updates:
        return

    # display_name → user_id lookup built from the actual message list
    name_to_uid: dict[str, int] = {}
    for m in messages:
        name_to_uid.setdefault(m["display_name"], m["user_id"])

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            for name, violations in updates.items():
                user_id = name_to_uid.get(name)
                if not user_id or not violations:
                    continue

                cur.execute(
                    "SELECT content FROM violation_logs WHERE chat_id = %s AND user_id = %s",
                    (chat_id, user_id),
                )
                existing = {r["content"] for r in cur.fetchall()}
                seen_this_run: set[str] = set()

                for v in violations:
                    content = v.get("content")
                    if not content:
                        continue
                    if content in existing or content in seen_this_run:
                        logger.debug("Пропущен дубль нарушения: %.80s", content)
                        continue
                    if not (v.get("fines", 0) > 0 or v.get("days", 0) > 0):
                        continue

                    cur.execute(
                        """
                        INSERT INTO violation_logs
                            (chat_id, user_id, content, article, fines, days, violation_count)
                        VALUES (%s, %s, %s, %s, %s, %s, 1)
                        """,
                        (chat_id, user_id, content, v.get("article", ""), v.get("fines", 0), v.get("days", 0)),
                    )
                    seen_this_run.add(content)
                    existing.add(content)
        conn.commit()


def _update_checkpoint(chat_id: int) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stats_checkpoint (chat_id, last_check) VALUES (%s, NOW())
                ON CONFLICT (chat_id) DO UPDATE SET last_check = NOW()
                """,
                (chat_id,),
            )
        conn.commit()


def _fetch_all_stats(chat_id: int) -> list[dict]:
    """Return aggregated stats for all violators, sorted by loyalty index ascending."""
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    v.user_id,
                    u.display_name,
                    SUM(v.fines)           AS t_fines,
                    SUM(v.days)            AS t_days,
                    SUM(v.violation_count) AS t_count,
                    MAX(v.created_at)      AS last_crime,
                    SUM(v.fines) FILTER (WHERE v.created_at > NOW() - INTERVAL '24 hours')           AS d_fines,
                    SUM(v.days)  FILTER (WHERE v.created_at > NOW() - INTERVAL '24 hours')           AS d_days,
                    SUM(v.violation_count) FILTER (WHERE v.created_at > NOW() - INTERVAL '24 hours') AS d_count
                FROM violation_logs v
                JOIN users u ON u.user_id = v.user_id
                WHERE v.chat_id = %s
                GROUP BY v.user_id, u.display_name
                """,
                (chat_id,),
            )
            rows = cur.fetchall()

    now = datetime.now(tz=timezone.utc)
    result = []
    for r in rows:
        t_count = r["t_count"] or 0
        t_fines = r["t_fines"] or 0
        t_days  = r["t_days"]  or 0
        last_crime = r["last_crime"]

        if last_crime is not None and last_crime.tzinfo is None:
            last_crime = last_crime.replace(tzinfo=timezone.utc)

        clean_days = (now - last_crime).days if last_crime else 0
        loyalty_idx, daily_recovery = _calc_loyalty_index(t_count, t_fines, t_days, clean_days)

        result.append({
            "name": r["display_name"],
            "t": {
                "f": t_fines,
                "d": t_days,
                "c": t_count,
                "i": loyalty_idx,
                "clean": clean_days,
                "rate": daily_recovery,
            },
            "d": {
                "f": r["d_fines"] or 0,
                "d": r["d_days"]  or 0,
                "c": r["d_count"] or 0,
            },
        })

    # Worst loyalty index first
    result.sort(key=lambda x: x["t"]["i"])
    return result


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

async def handle_stats(message: types.Message) -> None:
    status_msg = await message.answer("⚖️ <b>Провожу аудит и сверку...</b>", parse_mode="HTML")
    chat_id = message.chat.id

    try:
        # ── Step 1: fetch messages since last checkpoint ──────────────────
        new_msgs, start_time = await db.run_in_thread(_fetch_new_messages, chat_id)

        # Checkpoint advances ONLY after successful AI processing.
        # On any failure we return early so the same window is re-analysed next time.
        ai_ok = False

        if new_msgs:
            history = "\n".join(f"{m['display_name']}: {m['content']}" for m in new_msgs)
            ai_result = await safe_generate_content(_AUDIT_PROMPT_HEADER + history)

            if ai_result.get("status") == "rate_limit":
                await status_msg.edit_text(
                    "🚦 <b>Лимит запросов к ИИ исчерпан.</b>\n"
                    f"{ai_result.get('message', 'Подождите 1–2 минуты и повторите команду.')}\n\n"
                    "Попробуйте позже.",
                    parse_mode="HTML",
                )
                return

            if ai_result.get("status") != "ok":
                await status_msg.edit_text(
                    f"❌ Ошибка при обращении к ИИ: {ai_result.get('message', 'Неизвестная ошибка')}",
                    parse_mode="HTML",
                )
                return

            raw_json = clean_json_text(ai_result["text"])
            raw_json = " ".join(raw_json.split())
            raw_json = re.sub(r",\s*}", "}", raw_json)
            logger.info("Gemini raw response: %s", raw_json)

            try:
                updates = json.loads(raw_json)
                ai_ok = True
            except json.JSONDecodeError:
                logger.exception(
                    "Ошибка парсинга JSON (stats) — "
                    "сообщения будут повторно проверены при следующем запросе."
                )
                await status_msg.edit_text("⚠️ ИИ вернул некорректный формат — попробуйте позже.")
                return

            await db.run_in_thread(_save_violations, chat_id, updates, new_msgs)
        else:
            ai_ok = True  # nothing to analyse, safe to advance checkpoint

        if ai_ok:
            await db.run_in_thread(_update_checkpoint, chat_id)

        # ── Step 2: fetch aggregated stats ────────────────────────────────
        final_data = await db.run_in_thread(_fetch_all_stats, chat_id)

        if not final_data:
            await status_msg.edit_text("✅ Нарушений не зафиксировано.")
            return

        # ── Step 3: build report ──────────────────────────────────────────
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        window_str = start_time.strftime("%d.%m.%Y %H:%M")

        lines = [
            "🚨 <b>СВОДНЫЙ РЕЕСТР</b> 🚨",
            f"<i>Период анализа: с {window_str} UTC</i>",
        ]

        for p in final_data:
            icon = "🟢" if p["t"]["i"] >= 80 else "🟡" if p["t"]["i"] >= 40 else "🔴"
            lines.append(f"\n{icon} <b>{escape_html(p['name'])}</b>")

            if p["d"]["c"] > 0:
                lines.append(
                    f"⚡️ За последние 24 ч:\n"
                    f"   └ Нарушений: <code>{p['d']['c']}</code> | "
                    f"Штраф: <code>{p['d']['f']} ₽</code> | "
                    f"Срок: <code>{p['d']['d']} дн.</code>"
                )
            else:
                lines.append("⚡️ За последние 24 ч: Чисто ✅")

            lines.append(
                f"🗂 Всего за время наблюдения:\n"
                f"   └ Нарушений: <code>{p['t']['c']}</code>\n"
                f"   └ Штраф: <code>{p['t']['f']} ₽</code> | Срок: <code>{p['t']['d']} дн.</code>"
            )

            if p["t"]["clean"] > 0 and p["t"]["i"] < 100:
                bonus = min(50, p["t"]["clean"] * p["t"]["rate"])
                lines.append(
                    f"   └ 🧼 Без нарушений: <code>{p['t']['clean']} дн.</code> "
                    f"(+{p['t']['rate']} пт/день, бонус: +{bonus})"
                )

            lines.append(f"   ┗ <b>Индекс патриотичности:</b> <code>{p['t']['i']}%</code>")

        # ── Step 4: AI closing remark ─────────────────────────────────────
        target = random.choice(final_data)
        if target["d"]["c"] > 0:
            context = (
                f"Гражданин {target['name']} сегодня нарушил порядок "
                f"{target['d']['c']} раз(а). Штрафы растут."
            )
        elif target["t"]["clean"] > 0:
            context = (
                f"Гражданин {target['name']} не нарушает уже {target['t']['clean']} дн. "
                f"и встал на путь исправления."
            )
        else:
            context = f"Гражданин {target['name']} под наблюдением, пока ведёт себя тихо."

        thoughts_res = await safe_generate_content(_THOUGHTS_PROMPT.format(context=context))
        thoughts = (
            thoughts_res["text"].strip()
            if thoughts_res.get("status") == "ok"
            else "Майор временно недоступен. Подождите минуту."
        )

        lines += [
            "\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯",
            f"🕵️ <b>ОСОБОЕ МНЕНИЕ:</b>\n{escape_html(thoughts)}",
            "\n<a href=\"https://telegra.ph/Nejromajor--logika-podscheta-narushenij-02-27\">📋 Методика расчёта санкций</a>",
        ]

        await status_msg.edit_text(
            "\n".join(lines),
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    except Exception:
        logger.exception("Ошибка в handle_stats (chat=%d).", chat_id)
        await status_msg.edit_text("❌ Ошибка при аудите.")
