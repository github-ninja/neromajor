import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from bot.ai.gemini import AIContentError, AIError, AIRateLimited
from bot.ai.schemas import AuditFinding, AuditResult
from bot.config import Settings
from bot.db.repo import PendingMessage
from bot.services.audit import Auditor, build_batch, to_violations

T0 = datetime(2026, 5, 1, tzinfo=timezone.utc)


def msg(i: int, text: str = "привет", user: int = 1) -> PendingMessage:
    return PendingMessage(i, user, f"User{user}", text, T0 + timedelta(minutes=i))


def test_build_batch_respects_chars_but_takes_at_least_one():
    msgs = [msg(i, "x" * 100) for i in range(1, 11)]
    batch, text = build_batch(msgs, 450)
    assert [m.id for m in batch] == [1, 2, 3, 4]
    assert text.splitlines()[0].startswith("[1] User1: ")
    huge, _ = build_batch([msg(1, "y" * 10_000)], 450)
    assert len(huge) == 1


def test_build_batch_flattens_newlines():
    _, text = build_batch([msg(1, "a\nb\n\nc")], 1000)
    assert text == "[1] User1: a b c"


def test_to_violations_takes_facts_from_message_not_model():
    batch = [msg(1, "крамола", user=7), msg(2)]
    findings = [
        AuditFinding(message_id=1, article="ст. 280 УК", fines=300_000, days=0),
        AuditFinding(message_id=1, article="ст. 282 УК", fines=10_000, days=0),
        AuditFinding(message_id=99, article="ст. 1", fines=1, days=0),  # not in batch
        AuditFinding(message_id=2, article="ст. 2", fines=0, days=0),  # no sanction
        AuditFinding(message_id=2, article="ст. 3", fines=-5, days=0),  # negative
    ]
    [v] = to_violations(findings, batch)
    assert (v.message_id, v.user_id, v.content) == (1, 7, "крамола")
    assert v.fines == 310_000 and v.article == "ст. 280 УК; ст. 282 УК"
    assert v.committed_at == batch[0].created_at


class FakeRepo:
    def __init__(self, messages):
        self.messages = messages
        self.cursor = 0
        self.stored = []

    async def pending_messages(self, chat_id, limit):
        return [m for m in self.messages if m.id > self.cursor][:limit]

    async def count_pending(self, chat_id):
        return sum(1 for m in self.messages if m.id > self.cursor)

    async def commit_audit(self, chat_id, cursor, violations):
        self.cursor = max(self.cursor, cursor)
        self.stored += violations
        return len(violations)


class FakeAI:
    def __init__(self, responses):
        self.responses = list(responses)
        self.prompts = []

    async def structured(self, prompt, schema, *, system=None):
        self.prompts.append(prompt)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def settings(**overrides) -> Settings:
    return dataclasses.replace(Settings.from_env(), **{"audit_batch_chars": 1000, **overrides})


@pytest.mark.asyncio
async def test_auditor_processes_backlog_in_batches_without_gaps():
    repo = FakeRepo([msg(i, "z" * 90) for i in range(1, 26)])  # ~25 lines of ~100 chars
    ai = FakeAI([
        AuditResult(violations=[AuditFinding(message_id=3, article="ст. 280", fines=300_000, days=0)]),
        AuditResult(),
        AuditResult(),
    ])
    outcome = await Auditor(repo, ai, settings()).audit_chat(1, max_requests=10)
    assert outcome.pending == 0 and outcome.audited == 25 and outcome.found == 1
    assert outcome.requests == 3
    audited_ids = [int(line[1:line.index("]")]) for p in ai.prompts for line in p.splitlines()[1:]]
    assert audited_ids == list(range(1, 26))


@pytest.mark.asyncio
async def test_auditor_rate_limit_keeps_cursor():
    repo = FakeRepo([msg(i) for i in range(1, 6)])
    ai = FakeAI([AIRateLimited("лимит")])
    outcome = await Auditor(repo, ai, settings()).audit_chat(1, max_requests=5)
    assert repo.cursor == 0 and outcome.pending == 5 and outcome.error == "лимит"


@pytest.mark.asyncio
async def test_auditor_api_error_never_skips_messages():
    repo = FakeRepo([msg(1), msg(2)])
    ai = FakeAI([AIError("location not supported")] * 5)
    outcome = await Auditor(repo, ai, settings()).audit_chat(1, max_requests=5)
    assert repo.cursor == 0 and outcome.requests == 1 and outcome.pending == 2


@pytest.mark.asyncio
async def test_auditor_respects_request_cap():
    repo = FakeRepo([msg(i, "z" * 90) for i in range(1, 26)])
    ai = FakeAI([AuditResult(), AuditResult(), AuditResult()])
    outcome = await Auditor(repo, ai, settings()).audit_chat(1, max_requests=1)
    assert outcome.requests == 1 and 0 < outcome.pending < 25


@pytest.mark.asyncio
async def test_auditor_shrinks_failing_batch_then_skips_poison_message():
    repo = FakeRepo([msg(1, "яд" * 200), msg(2, "ok")])
    ai = FakeAI([AIContentError("blocked"), AIContentError("blocked"), AuditResult()])
    outcome = await Auditor(repo, ai, settings(audit_batch_chars=2000)).audit_chat(1, max_requests=5)
    # batch [1,2] fails -> shrunk to [1] fails -> 1 skipped -> [2] succeeds
    assert repo.cursor == 2 and outcome.pending == 0 and outcome.requests == 3
