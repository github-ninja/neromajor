"""Async Gemini client: rate limiting, retries, structured output."""

import asyncio
import logging
import re
import time
from typing import Any, TypeVar

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
from pydantic import BaseModel, ValidationError

from bot.config import Settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

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
    """Extract google.rpc.RetryInfo.retryDelay (e.g. "37s") from a 429 payload."""
    details = exc.details
    if isinstance(details, dict):
        details = details.get("error", details).get("details", [])
    if not isinstance(details, list):
        return None
    for item in details:
        if isinstance(item, dict) and str(item.get("@type", "")).endswith("RetryInfo"):
            match = re.match(r"([\d.]+)s", str(item.get("retryDelay", "")))
            if match:
                return float(match.group(1))
    return None


def _is_hard_quota(exc: genai_errors.APIError) -> bool:
    """Quota that won't recover within a retry: per-day limits, spending cap, billing."""
    text = f"{exc.message} {exc.details}".lower()
    return (
        "perday" in text.replace("_", "")
        or "spending cap" in text
        or "billing" in text
    )


def _is_transient(exc: genai_errors.APIError) -> bool:
    # "User location is not supported" comes and goes here: the API is reached
    # through DNS-based routing whose exit point varies between requests.
    return exc.code >= 500 or (exc.code == 400 and "location" in (exc.message or "").lower())


class Gemini:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = genai.Client(api_key=settings.gemini_api_key)
        self._limiter = _RateLimiter(settings.gemini_rpm)
        # Quotas are per model, so is the pause after exhausting one.
        self._paused_until: dict[str, float] = {}

    def _config(self, system: str | None, **extra: Any) -> genai_types.GenerateContentConfig:
        kwargs: dict[str, Any] = {"system_instruction": system, **extra}
        if self._settings.gemini_thinking_level:
            kwargs["thinking_config"] = genai_types.ThinkingConfig(
                thinking_level=self._settings.gemini_thinking_level.upper()
            )
        return genai_types.GenerateContentConfig(**kwargs)

    async def text(self, prompt: str, *, system: str | None = None, lite: bool = False) -> str:
        response = await self._generate_routed(prompt, self._config(system), lite)
        return response.text.strip()

    async def text_with_search(self, prompt: str, *, system: str | None = None) -> str:
        config = self._config(
            system, tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())]
        )
        response = await self._generate(prompt, config, self._settings.gemini_search_model)
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

    async def _generate_routed(
        self, prompt: str, config: genai_types.GenerateContentConfig, lite: bool
    ) -> genai_types.GenerateContentResponse:
        """
        lite=True: bulk work (audit) on the lite model only. Otherwise the main
        model, falling back to the lite one when the main model is overloaded
        or out of quota, so the Major answers more plainly instead of not at all.
        """
        main, fallback = self._settings.gemini_model, self._settings.gemini_lite_model
        if lite or main == fallback:
            return await self._generate(prompt, config, fallback, self._settings.gemini_retries)
        try:
            # Few attempts on the main model: the fallback is better than a long wait.
            return await self._generate(prompt, config, main, min(2, self._settings.gemini_retries))
        except AIContentError:
            raise
        except AIError as exc:
            logger.warning("Gemini [%s] недоступна (%s), переключаюсь на %s.",
                           main, exc.user_message, fallback)
            return await self._generate(prompt, config, fallback, self._settings.gemini_retries)

    async def _generate(
        self, prompt: str, config: genai_types.GenerateContentConfig, model: str, retries: int
    ) -> genai_types.GenerateContentResponse:
        if time.monotonic() < self._paused_until.get(model, 0.0):
            raise AIRateLimited("Лимит запросов к ИИ исчерпан. Попробуйте позже.")
        for attempt in range(1, retries + 1):
            last = attempt == retries
            await self._limiter.acquire()
            logger.info(
                "Gemini [%s] попытка %d/%d, промпт %d симв.", model, attempt, retries, len(prompt)
            )
            try:
                response = await asyncio.wait_for(
                    self._client.aio.models.generate_content(
                        model=model, contents=prompt, config=config
                    ),
                    timeout=self._settings.gemini_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("Gemini таймаут (попытка %d/%d).", attempt, retries)
                if last:
                    raise AIError("Превышено время ожидания ответа от ИИ.") from None
                await asyncio.sleep(2 ** attempt)
                continue
            except genai_errors.APIError as exc:
                if exc.code == 429:
                    delay = _retry_delay(exc) or 10.0 * attempt
                    logger.warning("Gemini 429 (попытка %d/%d, ждать %.0f с): %s",
                                   attempt, retries, delay, exc.message)
                    hard = _is_hard_quota(exc)
                    if hard:
                        self._paused_until[model] = time.monotonic() + _HARD_QUOTA_PAUSE
                        logger.error("Gemini [%s]: квота исчерпана, пауза %d мин: %s",
                                     model, _HARD_QUOTA_PAUSE // 60, exc.message)
                    if hard or last or delay > _MAX_RETRY_DELAY:
                        raise AIRateLimited(
                            "Достигнут лимит запросов к ИИ. Попробуйте позже."
                        ) from exc
                    await asyncio.sleep(delay)
                    continue
                if _is_transient(exc) and not last:
                    logger.warning("Gemini %d (попытка %d/%d): %s",
                                   exc.code, attempt, retries, exc.message)
                    await asyncio.sleep(2 ** attempt)
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
