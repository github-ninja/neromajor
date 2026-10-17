import asyncio
import dataclasses
from types import SimpleNamespace

import pytest
from google.genai import errors

from bot.ai import gemini as gemini_mod
from bot.ai.gemini import AIContentError, AIError, AIRateLimited, AITimeout, Gemini
from bot.config import Settings


def api_error(code: int, message: str, details: list | None = None) -> errors.APIError:
    payload = {"error": {"code": code, "message": message, "status": "X", "details": details or []}}
    cls = errors.ClientError if code < 500 else errors.ServerError
    return cls(code, payload)


def ok(text: str = "ответ"):
    return SimpleNamespace(text=text, parsed=None, candidates=[])


@pytest.fixture
def make(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(gemini_mod.asyncio, "sleep", no_sleep)

    def factory(responses, fallback=False):
        settings = dataclasses.replace(Settings.from_env(), gemini_rpm=10_000, gemini_retries=3)
        if not fallback:  # single model unless the test is about routing
            settings = dataclasses.replace(settings, gemini_model=settings.gemini_lite_model)
        g = Gemini(settings)
        calls = []

        async def generate_content(**kwargs):
            calls.append(kwargs)
            r = responses.pop(0)
            if isinstance(r, Exception):
                raise r
            return r

        g._client = SimpleNamespace(
            aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate_content))
        )
        return g, calls
    return factory


@pytest.mark.asyncio
async def test_transient_location_error_is_retried(make):
    g, calls = make([api_error(400, "User location is not supported for the API use."), ok()])
    assert await g.text("x", system="persona") == "ответ"
    assert len(calls) == 2
    config = calls[0]["config"]
    assert config.system_instruction == "persona" and config.temperature is None


@pytest.mark.asyncio
async def test_other_client_errors_fail_fast(make):
    g, calls = make([api_error(400, "API key not valid")])
    with pytest.raises(AIError) as exc:
        await g.text("x")
    assert not isinstance(exc.value, AIRateLimited) and len(calls) == 1


@pytest.mark.asyncio
async def test_long_retry_delay_means_quota_exhausted(make):
    retry = [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "3600s"}]
    g, calls = make([api_error(429, "quota", retry)])
    with pytest.raises(AIRateLimited):
        await g.text("x")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_short_429_is_retried(make):
    retry = [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "5s"}]
    g, calls = make([api_error(429, "slow down", retry), ok()])
    assert await g.text("x") == "ответ" and len(calls) == 2


@pytest.mark.asyncio
async def test_empty_answer_is_content_error(make):
    g, _ = make([ok("")])
    with pytest.raises(AIContentError):
        await g.text("x")


@pytest.mark.asyncio
async def test_spending_cap_fails_fast_and_pauses_further_calls(make):
    cap = api_error(429, "Your project has exceeded its monthly spending cap. Please go to AI Studio")
    g, calls = make([cap, ok()])
    with pytest.raises(AIRateLimited):
        await g.text("x")
    with pytest.raises(AIRateLimited):
        await g.text("y")  # paused: no request is sent at all
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_main_model_falls_back_to_lite_when_overloaded(make):
    busy = api_error(503, "This model is currently experiencing high demand.")
    g, calls = make([busy, ok("попроще")], fallback=True)
    assert await g.text("x") == "попроще"
    models = [c["model"] for c in calls]
    assert models == [g._settings.gemini_model, g._settings.gemini_lite_model]


@pytest.mark.asyncio
async def test_lite_requests_never_touch_main_model(make):
    g, calls = make([ok()], fallback=True)
    await g.text("x", lite=True)
    assert [c["model"] for c in calls] == [g._settings.gemini_lite_model]


@pytest.mark.asyncio
async def test_quota_pause_is_per_model(make):
    cap = api_error(429, "Your project has exceeded its monthly spending cap.")
    g, calls = make([cap, ok("lite"), ok("lite again")], fallback=True)
    assert await g.text("x") == "lite"  # main capped -> fallback
    assert await g.text("y") == "lite again"  # main paused: straight to lite
    assert [c["model"] for c in calls] == [g._settings.gemini_model] + [g._settings.gemini_lite_model] * 2


@pytest.mark.asyncio
async def test_deadline_bounds_the_whole_call(make):
    g, calls = make([])

    async def hang(**kwargs):
        calls.append(kwargs)
        await asyncio.Event().wait()

    g._client.aio.models.generate_content = hang
    with pytest.raises(AITimeout):
        await g.text("x", deadline=0.05)
    assert len(calls) == 1


BILLING_429 = "You exceeded your current quota, please check your plan and billing details."






def test_retry_delay_from_message_hint():
    exc = api_error(429, "Quota exceeded for metric: free_tier_requests, limit: 20\nPlease retry in 14h53m22.85s.")
    assert gemini_mod._retry_delay(exc) == pytest.approx(14 * 3600 + 53 * 60 + 22.85)


@pytest.mark.asyncio
async def test_daily_quota_pauses_until_reset(make):
    daily = api_error(429, "Quota exceeded, limit: 20\nPlease retry in 2h0m0s.")
    g, _ = make([daily])
    with pytest.raises(AIRateLimited):
        await g.text("x")
    left = g._paused_until[g._settings.gemini_lite_model] - gemini_mod.time.monotonic()
    assert 7000 < left <= 7200


@pytest.mark.asyncio
async def test_per_minute_429_mentioning_billing_is_still_retried(make):
    retry = [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "20s"}]
    g, calls = make([api_error(429, BILLING_429, retry), ok()])
    assert await g.text("x") == "ответ" and len(calls) == 2
    assert not g._paused_until


@pytest.mark.asyncio
async def test_rate_limit_is_per_model(make, monkeypatch):
    waits = []

    async def record_sleep(delay):
        waits.append(delay)

    monkeypatch.setattr(gemini_mod.asyncio, "sleep", record_sleep)
    busy = api_error(503, "This model is currently experiencing high demand.")
    g, calls = make([busy, ok()], fallback=True)
    g._settings = dataclasses.replace(g._settings, gemini_rpm=10)  # 6 s between starts
    assert await g.text("x") == "ответ"
    assert [c["model"] for c in calls] == [g._settings.gemini_model, g._settings.gemini_lite_model]
    assert waits == []  # the fallback model has its own, still empty, schedule


def slow_main(g, calls, main_answer, lite_answer):
    """Main model answers only after the lite one has been asked and has replied."""
    lite_replied = asyncio.Event()

    async def generate_content(**kwargs):
        calls.append(kwargs["model"])
        if kwargs["model"] == g._settings.gemini_model:
            await lite_replied.wait()
            return main_answer
        lite_replied.set()
        if isinstance(lite_answer, Exception):
            raise lite_answer
        return lite_answer

    g._client.aio.models.generate_content = generate_content
    g._settings = dataclasses.replace(g._settings, gemini_reply_hedge_after=0.02, gemini_retries=1)


@pytest.mark.asyncio
async def test_slow_main_model_is_raced_against_lite(make):
    g, calls = make([], fallback=True)
    slow_main(g, calls, ok("основная"), ok("лайт"))
    assert await g.text("x", deadline=5) == "лайт"
    assert calls == [g._settings.gemini_model, g._settings.gemini_lite_model]


@pytest.mark.asyncio
async def test_slow_main_model_still_answers_if_lite_fails(make):
    g, calls = make([], fallback=True)
    slow_main(g, calls, ok("основная"), api_error(503, "high demand"))
    assert await g.text("x", deadline=5) == "основная"


@pytest.mark.asyncio
async def test_calls_without_deadline_keep_the_long_timeout(make, monkeypatch):
    seen = []
    real_wait_for = gemini_mod.asyncio.wait_for

    async def spy(aw, timeout):
        seen.append(timeout)
        return await real_wait_for(aw, timeout)

    monkeypatch.setattr(gemini_mod.asyncio, "wait_for", spy)
    g, _ = make([ok()])
    await g.text("x")
    assert seen == [g._settings.gemini_timeout]
