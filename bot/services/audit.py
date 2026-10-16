"""
Background violation audit.

Every human message is audited exactly once: each chat keeps an audit cursor
(last audited messages.id), batches are taken strictly after it, and the
cursor only moves in the same transaction that stores the batch's findings.
A failed request leaves the cursor in place, so nothing is skipped.
"""

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime

from bot.ai import prompts
from bot.ai.gemini import AIContentError, AIError, Gemini
from bot.ai.schemas import AuditFinding, AuditResult
from bot.config import Settings
from bot.db.repo import NewViolation, PendingMessage, Repository

logger = logging.getLogger(__name__)

_MESSAGE_CHARS = 1500  # longer messages are cut before auditing
_MAX_ARTICLE_CHARS = 200


@dataclass(slots=True)
class AuditOutcome:
    requests: int = 0
    audited: int = 0
    found: int = 0
    pending: int = 0
    error: str | None = None
    busy: bool = False  # another audit of this chat was already running


def build_batch(
    messages: list[PendingMessage], max_chars: int
) -> tuple[list[PendingMessage], str]:
    """Take messages in order while the rendered text fits *max_chars* (at least one)."""
    batch: list[PendingMessage] = []
    lines: list[str] = []
    used = 0
    for m in messages:
        text = " ".join(m.content.split())[:_MESSAGE_CHARS]
        line = f"[{m.id}] {m.display_name}: {text}"
        if batch and used + len(line) + 1 > max_chars:
            break
        batch.append(m)
        lines.append(line)
        used += len(line) + 1
    return batch, "\n".join(lines)


def to_violations(
    findings: list[AuditFinding], batch: list[PendingMessage]
) -> list[NewViolation]:
    """
    Validate model findings against the batch: only messages actually in it
    count (author, text and date then come from the message row, never from
    the model). Several findings for one message merge into one violation.
    """
    by_id = {m.id: m for m in batch}
    merged: dict[int, dict] = {}
    for f in findings:
        message = by_id.get(f.message_id)
        fines, days = max(0, f.fines), max(0, f.days)
        if message is None:
            logger.warning("Аудит: ИИ сослался на сообщение вне пачки (%d).", f.message_id)
            continue
        if fines == 0 and days == 0:
            continue
        article = " ".join(f.article.split())[:_MAX_ARTICLE_CHARS]
        slot = merged.setdefault(message.id, {"articles": [], "fines": 0, "days": 0})
        if article and article not in slot["articles"]:
            slot["articles"].append(article)
        slot["fines"] += fines
        slot["days"] += days

    return [
        NewViolation(
            message_id=mid,
            article="; ".join(slot["articles"]),
            fines=slot["fines"],
            days=slot["days"],
        )
        for mid, slot in merged.items()
    ]


class Auditor:
    def __init__(self, repo: Repository, ai: Gemini, settings: Settings) -> None:
        self._repo = repo
        self._ai = ai
        self._settings = settings
        self._locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        # Per-chat batch size; halves after a failed batch so one message the
        # model refuses to process can't block the chat forever.
        self._batch_chars: dict[int, int] = {}
        self._budget_day: date | None = None
        self._budget_used = 0

    def _today(self) -> date:
        return datetime.now(self._settings.timezone).date()

    def _budget_left(self) -> int:
        if self._budget_day != self._today():
            self._budget_day, self._budget_used = self._today(), 0
        return max(0, self._settings.audit_daily_requests - self._budget_used)

    async def audit_chat(
        self, chat_id: int, max_requests: int, *, wait: bool = True
    ) -> AuditOutcome:
        """
        Audit pending messages, spending at most *max_requests* Gemini calls.
        With wait=False returns immediately (busy=True) if the chat is being
        audited already, instead of queueing behind the background pass.
        """
        outcome = AuditOutcome()
        lock = self._locks[chat_id]
        if not wait and lock.locked():
            outcome.busy = True
            outcome.pending = await self._repo.count_pending(chat_id)
            return outcome
        async with lock:
            while outcome.requests < max_requests:
                pending = await self._repo.pending_messages(
                    chat_id, self._settings.audit_batch_messages
                )
                if not pending:
                    break
                max_chars = self._batch_chars.get(chat_id, self._settings.audit_batch_chars)
                batch, text = build_batch(pending, max_chars)

                outcome.requests += 1
                self._budget_used += 1
                try:
                    result = await self._ai.structured(
                        prompts.AUDIT_USER + text, AuditResult, system=prompts.AUDIT_SYSTEM
                    )
                except AIContentError as exc:
                    if len(batch) == 1:
                        # Nothing left to split: skip the message rather than stall.
                        logger.error("Аудит: сообщение %d пропущено (chat=%d): %s",
                                     batch[0].id, chat_id, exc.user_message)
                        await self._repo.commit_audit(chat_id, batch[0].id, [])
                        outcome.audited += 1
                        continue
                    self._batch_chars[chat_id] = max(200, len(text) // 2)
                    outcome.error = exc.user_message
                    logger.warning("Аудит: ИИ не принял пачку (chat=%d, %d сообщ.), уменьшаю.",
                                   chat_id, len(batch))
                    continue
                except AIError as exc:
                    # Rate limit, outage, bad key...: nothing to do with the
                    # messages themselves. Stop and retry them later as they are.
                    outcome.error = exc.user_message
                    break

                self._batch_chars.pop(chat_id, None)
                outcome.error = None
                violations = to_violations(result.violations, batch)
                inserted = await self._repo.commit_audit(chat_id, batch[-1].id, violations)
                outcome.audited += len(batch)
                outcome.found += inserted
                logger.info("Аудит chat=%d: проверено %d, нарушений %d.",
                            chat_id, len(batch), inserted)

            outcome.pending = await self._repo.count_pending(chat_id)
        return outcome

    async def run_forever(self) -> None:
        interval = self._settings.audit_interval * 60
        logger.info("Фоновый аудит запущен (каждые %d мин, до %d запросов в сутки).",
                    self._settings.audit_interval, self._settings.audit_daily_requests)
        while True:
            try:
                await self._pass()
            except Exception:
                logger.exception("Фоновый аудит: непредвиденная ошибка.")
            await asyncio.sleep(interval)

    async def _pass(self) -> None:
        chats = await self._repo.chats_with_pending()
        for n, chat_id in enumerate(chats):
            budget = self._budget_left()
            if budget == 0:
                logger.info("Фоновый аудит: дневной лимит запросов исчерпан.")
                return
            # Share the budget so one chat's backlog can't starve the others.
            share = max(1, budget // (len(chats) - n))
            outcome = await self.audit_chat(chat_id, share)
            if outcome.error and outcome.pending:
                logger.warning("Фоновый аудит chat=%d прерван: %s (осталось %d).",
                               chat_id, outcome.error, outcome.pending)
