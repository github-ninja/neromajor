"""Async Gemini client: rate limiting, retries, structured output."""

import asyncio
import logging
import re
import time
from collections.abc import Awaitable
from typing import Any, TypeVar

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
from pydantic import BaseModel, ValidationError

from bot.ai import routing
from bot.config import Settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)
R = TypeVar("R")

# A 429 asking us to wait longer than this is a daily/hard quota, not a burst
# limit: retrying would only burn the remaining attempts.
_MAX_RETRY_DELAY = 65.0
# After a hard quota error (daily quota, project spending cap) every call
# fails fast for this long instead of hitting the API again.
_HARD_QUOTA_PAUSE = 15 * 60


class AIError(Exception):
    """Gemini failed; ``user_message`` is safe to show in the chat."""

    def __init__(self, user_message: str) -> None:
        super().__init__(user_message)
        self.user_message = user_message


class AIRateLimited(AIError):
    pass


class AITimeout(AIError):
    pass


class AIContentError(AIError):
    """The request went through but this particular input produced no usable
    answer (safety block, empty or off-schema response). Retrying the same
    input won't help; a smaller or different input might."""


class _RateLimiter:
    """Spaces request starts evenly so we never exceed *rpm* requests per minute."""

    def __init__(self, rpm: int) -> None:
        self._interval = 60.0 / rpm
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._interval
        if wait > 0:
            await asyncio.sleep(wait)


def _retry_delay(exc: genai_errors.APIError) -> float | None:
    """
    google.rpc.RetryInfo.retryDelay (e.g. "37s") from a 429 payload, else the
    "Please retry in 14h53m22.8s" hint in the message.
    """
    details = exc.details
    if isinstance(details, dict):
        details = details.get("error", details).get("details", [])
    for item in details if isinstance(details, list) else []:
        if isinstance(item, dict) and str(item.get("@type", "")).endswith("RetryInfo"):
            match = re.match(r"([\d.]+)s", str(item.get("retryDelay", "")))
            if match:
                return float(match.group(1))
    match = re.search(r"retry in (?:(\d+)h)?(?:(\d+)m(?!s))?(?:([\d.]+)s)?", exc.message or "")
    if match and any(match.groups()):
        h, m, s = match.groups()
        return int(h or 0) * 3600 + int(m or 0) * 60 + float(s or 0)
    return None


def _is_hard_quota(exc: genai_errors.APIError, delay: float | None) -> bool:
    """
    Quota that won't recover within a retry: per-day limits, spending cap.
    Every quota 429 says "check your plan and billing details", so that text
    only counts when the API gives no retry delay to go by.
    """
    text = f"{exc.message} {exc.details}".lower()
    if "perday" in text.replace("_", "") or "spending cap" in text:
        return True
    if delay is not None:
        return delay > _MAX_RETRY_DELAY
    return "billing" in text


def _refused_location(exc: genai_errors.APIError) -> bool:
    """"User location is not supported": the exit node we went through, see routing."""
    return exc.code == 400 and "location" in (exc.message or "").lower()


def _is_transient(exc: genai_errors.APIError) -> bool:
    return exc.code >= 500 or _refused_location(exc)


async def _first_success(*tasks: asyncio.Future[R]) -> R:
    """Result of whichever task succeeds first; the last error if all fail."""
    pending = set(tasks)
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.exception() is None:
                    return task.result()
                error = task.exception()
        raise error
    finally:
        for task in pending:
            task.cancel()


class Gemini:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._nodes = routing.NodeResolver()
        self._client: Any = None  # created on first use: aiohttp needs a running loop
        self._session: Any = None
        # Per model, like Google's quotas: falling back to the lite model must
        # not queue behind the main model's request that just failed.
        self._limiters: dict[str, _RateLimiter] = {}
        # Quotas are per model, so is the pause after exhausting one.
        self._paused_until: dict[str, float] = {}

    def _api(self) -> Any:
        if self._client is None:
            self._session = routing.session(self._nodes)
            self._client = genai.Client(
                api_key=self._settings.gemini_api_key,
                http_options=genai_types.HttpOptions(aiohttp_client=self._session),
            )
        return self._client

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    def _config(self, system: str | None, **extra: Any) -> genai_types.GenerateContentConfig:
        kwargs: dict[str, Any] = {"system_instruction": system, **extra}
        if self._settings.gemini_thinking_level:
            kwargs["thinking_config"] = genai_types.ThinkingConfig(
                thinking_level=self._settings.gemini_thinking_level.upper()
            )
        return genai_types.GenerateContentConfig(**kwargs)

    async def text(
        self,
        prompt: str,
        *,
        system: str | None = None,
        lite: bool = False,
        deadline: float | None = None,
    ) -> str:
        """
        *deadline*: give up after this many seconds in total, retries included.
        Such a call is someone waiting for an answer, so when the main model is
        silent for GEMINI_REPLY_HEDGE_AFTER the lite one is asked in parallel and
        the first answer wins. A slow request is never abandoned: under load
        Google often takes a minute, but does answer.
        """
        hedge_after = self._settings.gemini_reply_hedge_after if deadline else None
        call = self._generate_routed(prompt, self._config(system), lite, hedge_after)
        response = await self._within(deadline, call)
        return response.text.strip()

    async def structured(
        self, prompt: str, schema: type[T], *, system: str | None = None, lite: bool = False
    ) -> T:
        config = self._config(
            system, response_mime_type="application/json", response_schema=schema
        )
        response = await self._generate_routed(prompt, config, lite)
        if isinstance(response.parsed, schema):
            return response.parsed
        try:
            return schema.model_validate_json(response.text)
        except ValidationError as exc:
            logger.warning("Gemini вернул ответ не по схеме %s: %s", schema.__name__, exc)
            raise AIContentError("ИИ вернул некорректный формат ответа.") from exc

    @staticmethod
    async def _within(deadline: float | None, call: Awaitable[R]) -> R:
        if deadline is None:
            return await call
        try:
            return await asyncio.wait_for(call, timeout=deadline)
        except asyncio.TimeoutError:
            logger.warning("Gemini: ответ не уложился в %.0f с.", deadline)
            raise AITimeout("ИИ не успел ответить.") from None

    async def _generate_routed(
        self,
        prompt: str,
        config: genai_types.GenerateContentConfig,
        lite: bool,
        hedge_after: float | None = None,
    ) -> genai_types.GenerateContentResponse:
        """
        lite=True: bulk work (audit) on the lite model only. Otherwise the main
        model, falling back to the lite one when the main model is overloaded
        or out of quota, so the Major answers more plainly instead of not at all.
        *hedge_after*: also ask the lite model if the main one is still silent.
        """
        main, fallback = self._settings.gemini_model, self._settings.gemini_lite_model
        retries = self._settings.gemini_retries
        if lite or main == fallback:
            return await self._generate(prompt, config, fallback, retries)
        # One attempt: overload ("high demand" 503) is model-wide and
        # usually outlasts a retry, so the fallback is the retry.
        first = asyncio.ensure_future(self._generate(prompt, config, main, 1))
        try:
            await asyncio.wait({first}, timeout=hedge_after)
            if not first.done():
                logger.warning("Gemini [%s] молчит %.0f с, параллельно спрашиваю %s.",
                               main, hedge_after, fallback)
                second = asyncio.ensure_future(self._generate(prompt, config, fallback, retries))
                return await _first_success(first, second)
            try:
                return first.result()
            except AIContentError:
                raise
            except AIError as exc:
                logger.warning("Gemini [%s] недоступна (%s), переключаюсь на %s.",
                               main, exc.user_message, fallback)
                return await self._generate(prompt, config, fallback, retries)
        finally:
            first.cancel()

    async def _generate(
        self,
        prompt: str,
        config: genai_types.GenerateContentConfig,
        model: str,
        retries: int,
    ) -> genai_types.GenerateContentResponse:
        if time.monotonic() < self._paused_until.get(model, 0.0):
            raise AIRateLimited("Лимит запросов к ИИ исчерпан. Попробуйте позже.")
        for attempt in range(1, retries + 1):
            last = attempt == retries
            limiter = self._limiters.setdefault(model, _RateLimiter(self._settings.gemini_rpm))
            await limiter.acquire()
            logger.info(
                "Gemini [%s] попытка %d/%d, промпт %d симв.", model, attempt, retries, len(prompt)
            )
            try:
                with routing.track() as nodes:
                    response = await asyncio.wait_for(
                        self._api().aio.models.generate_content(
                            model=model, contents=prompt, config=config
                        ),
                        timeout=self._settings.gemini_timeout,
                    )
            except asyncio.TimeoutError:
                logger.warning("Gemini таймаут (попытка %d/%d).", attempt, retries)
                if last:
                    raise AITimeout("Превышено время ожидания ответа от ИИ.") from None
                continue  # the wait itself was the backoff
            except genai_errors.APIError as exc:
                if exc.code == 429:
                    hinted = _retry_delay(exc)
                    delay = hinted or 10.0 * attempt
                    logger.warning("Gemini 429 (попытка %d/%d, ждать %.0f с): %s",
                                   attempt, retries, delay, exc.message)
                    hard = _is_hard_quota(exc, hinted)
                    if hard:
                        # Wait as long as the API says, e.g. until the daily reset.
                        pause = max(_HARD_QUOTA_PAUSE, int(delay) if delay > _MAX_RETRY_DELAY else 0)
                        self._paused_until[model] = time.monotonic() + pause
                        logger.error("Gemini [%s]: квота исчерпана, пауза %d мин: %s",
                                     model, pause // 60, exc.message)
                    if hard or last:
                        raise AIRateLimited(
                            "Достигнут лимит запросов к ИИ. Попробуйте позже."
                        ) from exc
                    await asyncio.sleep(delay)
                    continue
                if _refused_location(exc):
                    self._nodes.refused(nodes)
                if _is_transient(exc) and not last:
                    logger.warning("Gemini %d (попытка %d/%d): %s",
                                   exc.code, attempt, retries, exc.message)
                    # Another node will serve the retry: no need to wait.
                    await asyncio.sleep(0 if _refused_location(exc) else 2 ** attempt)
                    continue
                logger.error("Gemini ошибка %s: %s", exc.code, exc.message)
                raise AIError("Ошибка связи с ИИ.") from exc
            except Exception as exc:
                logger.exception("Gemini: сетевая или непредвиденная ошибка.")
                if last:
                    raise AIError("Ошибка связи с ИИ.") from exc
                await asyncio.sleep(2 ** attempt)
                continue

            if not response.text:
                # Safety block or empty candidate: deterministic, don't retry.
                reason = response.candidates[0].finish_reason if response.candidates else None
                logger.warning("Gemini вернул пустой ответ (finish_reason=%s).", reason)
                raise AIContentError("ИИ не дал ответа.")
            return response

        raise AIError("Не удалось получить ответ от ИИ.")  # pragma: no cover
