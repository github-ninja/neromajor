"""Dispatch real Telegram updates through the routers with a fake Bot API session."""

from datetime import datetime, timedelta, timezone

import pytest
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.base import BaseSession
from aiogram.enums import ParseMode
from aiogram.methods import EditMessageText, GetMe, SendMessage
from aiogram.types import Chat, Message, Update, User

from bot.config import Settings
from bot.db.repo import OffenderStats, UserRef
from bot.handlers import build_router
from bot.handlers.messages import MentionLimiter
from bot.services.audit import AuditOutcome

BOT = User(id=999, is_bot=True, first_name="Майор", username="neuromajor_bot")
CHAT = Chat(id=-100, type="supergroup", title="Чат")
USER = User(id=1, is_bot=False, first_name="Иван", username="ivan")
NOW = datetime.now(timezone.utc)


class FakeSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.sent = []

    async def make_request(self, bot, method, timeout=None):
        self.sent.append(method)
        if isinstance(method, GetMe):
            return BOT
        if isinstance(method, (SendMessage, EditMessageText)):
            return Message(message_id=len(self.sent) + 100, date=NOW, chat=CHAT,
                           from_user=BOT, text=method.text).as_(bot)
        return True

    async def stream_content(self, *a, **kw):  # pragma: no cover
        yield b""

    async def close(self):
        pass


class FakeRepo:
    def __init__(self):
        self.saved, self.bot_saved = [], []

    async def save_user_message(self, **kw):
        self.saved.append(kw)
        return 1

    async def save_bot_message(self, *args):
        self.bot_saved.append(args)

    async def recent_dialogue(self, chat_id, limit):
        return []

    async def recent_bot_messages(self, chat_id, limit):
        return []

    async def offender_stats(self, chat_id):
        return [OffenderStats(1, "Иван <script>", 2, 310_000, 0, NOW - timedelta(days=3), 0, 0, 0)]

    async def last_audit_at(self, chat_id):
        return NOW

    async def find_chat_user(self, chat_id, username):
        return UserRef(1, "Иван", "ivan") if username == "ivan" else None

    async def user_violations(self, chat_id, user_id):
        return []


class FakeAI:
    async def text(self, prompt, *, system=None):
        return "Всё записано <в дело>."


class FakeAuditor:
    async def audit_chat(self, chat_id, max_requests, *, wait=True):
        return AuditOutcome(requests=1, audited=5, pending=12, error="Достигнут лимит")


class FakeMajor:
    def __init__(self):
        self.reacted = []

    async def maybe_react(self, chat_id):
        self.reacted.append(chat_id)


@pytest.fixture(scope="module")
def dispatcher():
    # Routers are module-level singletons and can be attached only once.
    dp = Dispatcher()
    dp.include_router(build_router())
    return dp


@pytest.fixture
def env(dispatcher):
    session = FakeSession()
    bot = Bot("42:TEST", session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    repo, major = FakeRepo(), FakeMajor()
    dispatcher.workflow_data.update(
        settings=Settings.from_env(), repo=repo, ai=FakeAI(),
        auditor=FakeAuditor(), major=major, mentions=MentionLimiter(10),
    )
    return dispatcher, bot, session, repo, major


def update(text: str, entities=None, reply_to=None, uid=1) -> Update:
    return Update(update_id=uid, message=Message(
        message_id=uid, date=NOW, chat=CHAT, from_user=USER, text=text,
        entities=entities, reply_to_message=reply_to,
    ))


def texts(session):
    return [m.text for m in session.sent if isinstance(m, (SendMessage, EditMessageText))]


@pytest.mark.asyncio
async def test_stats_renders_escaped_registry_with_pending_note(env):
    dp, bot, session, *_ = env
    await dp.feed_update(bot, update("/stats"))
    final = texts(session)[-1]
    assert "СВОДНЫЙ РЕЕСТР" in final and "Иван &lt;script&gt;" in final
    assert "Ожидают проверки: 12" in final and "Всё записано &lt;в дело&gt;." in final
    assert "Индекс патриотичности:</b> <code>" in final


@pytest.mark.asyncio
async def test_plain_message_is_stored_and_offered_to_major(env):
    dp, bot, session, repo, major = env
    await dp.feed_update(bot, update("привет всем"))
    assert repo.saved[0]["content"] == "привет всем" and major.reacted == [-100]
    assert texts(session) == []


@pytest.mark.asyncio
async def test_mention_with_emoji_prefix_gets_reply_once_per_cooldown(env):
    dp, bot, session, repo, major = env
    text = "😀 @neuromajor_bot ты тут?"  # emoji = 2 UTF-16 units: offsets must be UTF-16
    entity = {"type": "mention", "offset": 3, "length": 15}
    await dp.feed_update(bot, update(text, entities=[entity], uid=1))
    await dp.feed_update(bot, update(text, entities=[entity], uid=2))
    assert texts(session) == ["Всё записано &lt;в дело&gt;."]
    assert len(repo.bot_saved) == 1 and major.reacted == []


@pytest.mark.asyncio
async def test_commands_are_not_stored(env):
    dp, bot, session, repo, _ = env
    await dp.feed_update(bot, update("/case @nobody"))
    assert repo.saved == []
    assert "отсутствует" in texts(session)[-1]
