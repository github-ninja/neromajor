import dataclasses
from types import SimpleNamespace

import pytest
from google.genai import errors

from bot.ai import gemini as gemini_mod
from bot.ai.gemini import AIContentError, AIError, AIRateLimited, Gemini
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
    g, calls = make([busy, busy, ok("попроще")], fallback=True)
    assert await g.text("x") == "попроще"
    models = [c["model"] for c in calls]
    assert models == [g._settings.gemini_model] * 2 + [g._settings.gemini_lite_model]


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
