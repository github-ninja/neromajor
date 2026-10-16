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


def _is_daily_quota(exc: genai_errors.APIError) -> bool:
    return "perday" in str(exc.details).lower().replace("_", "")


def _is_transient(exc: genai_errors.APIError) -> bool:
    # "User location is not supported" comes and goes here: the API is reached
    # through DNS-based routing whose exit point varies between requests.
    return exc.code >= 500 or (exc.code == 400 and "location" in (exc.message or "").lower())


class Gemini:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = genai.Client(api_key=settings.gemini_api_key)
        self._limiter = _RateLimiter(settings.gemini_rpm)

    def _config(self, system: str | None, **extra: Any) -> genai_types.GenerateContentConfig:
        kwargs: dict[str, Any] = {"system_instruction": system, **extra}
        if self._settings.gemini_thinking_level:
            kwargs["thinking_config"] = genai_types.ThinkingConfig(
                thinking_level=self._settings.gemini_thinking_level.upper()
            )
        return genai_types.GenerateContentConfig(**kwargs)

    async def text(self, prompt: str, *, system: str | None = None) -> str:
        response = await self._generate(prompt, self._config(system), self._settings.gemini_model)
        return response.text.strip()

    async def text_with_search(self, prompt: str, *, system: str | None = None) -> str:
        config = self._config(
            system, tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())]
        )
        response = await self._generate(prompt, config, self._settings.gemini_search_model)
        return response.text.strip()

    async def structured(self, prompt: str, schema: type[T], *, system: str | None = None) -> T:
        config = self._config(
            system, response_mime_type="application/json", response_schema=schema
        )
        response = await self._generate(prompt, config, self._settings.gemini_model)
        if isinstance(response.parsed, schema):
            return response.parsed
        try:
            return schema.model_validate_json(response.text)
        except ValidationError as exc:
            logger.warning("Gemini вернул ответ не по схеме %s: %s", schema.__name__, exc)
            raise AIContentError("ИИ вернул некорректный формат ответа.") from exc

    async def _generate(
        self, prompt: str, config: genai_types.GenerateContentConfig, model: str
    ) -> genai_types.GenerateContentResponse:
        retries = self._settings.gemini_retries
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
                    if last or delay > _MAX_RETRY_DELAY or _is_daily_quota(exc):
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
