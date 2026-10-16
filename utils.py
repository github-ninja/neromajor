"""Shared utilities: Gemini wrapper, text helpers."""

import asyncio
import html
import logging
import re

from google import genai
from google.genai import types as genai_types

import config

logger = logging.getLogger(__name__)

_gemini_client = genai.Client(api_key=config.GEMINI_API_KEY)


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

async def _gemini_request(
    prompt: str,
    model: str,
    retries: int,
    tools: list | None = None,
    json_mode: bool = False,
) -> dict:
    """
    Core Gemini request with exponential back-off retry.

    Sampling parameters (temperature, top_p, top_k) are intentionally not
    sent: Gemini ignores them since 3.6 and upcoming models reject them.

    Args:
      json_mode: if True, instructs Gemini to return valid JSON via
        response_mime_type. Removes the need for manual JSON cleanup.

    Returns:
      {"status": "ok",         "text": "..."}
      {"status": "rate_limit", "message": "..."}
      {"status": "error",      "message": "..."}
    """
    gen_config_kwargs = {"tools": tools or []}
    if json_mode:
        gen_config_kwargs["response_mime_type"] = "application/json"
    gen_config = genai_types.GenerateContentConfig(**gen_config_kwargs)

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "Gemini запрос [%s], попытка %d/%d (промпт: %d симв.)",
                model, attempt, retries, len(prompt),
            )
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    _gemini_client.models.generate_content,
                    model=model,
                    contents=prompt,
                    config=gen_config,
                ),
                timeout=config.GEMINI_TIMEOUT,
            )
            logger.info("Gemini ответил успешно (попытка %d).", attempt)
            return {"status": "ok", "text": response.text}

        except asyncio.TimeoutError:
            logger.warning("Gemini таймаут на попытке %d/%d.", attempt, retries)
            if attempt == retries:
                return {"status": "error", "message": "Превышено время ожидания ответа от ИИ."}
            await asyncio.sleep(2 ** attempt)

        except Exception as exc:
            error_str = str(exc).lower()
            is_rate_limit = any(kw in error_str for kw in ("429", "resourceexhausted", "quota"))
            if is_rate_limit:
                logger.warning("Gemini rate limit на попытке %d/%d: %s", attempt, retries, exc)
                if attempt == retries:
                    return {
                        "status": "rate_limit",
                        "message": "Достигнут лимит запросов к ИИ. Подождите 1–2 минуты и повторите команду.",
                    }
                await asyncio.sleep(10 * attempt)
            else:
                logger.error("Ошибка Gemini на попытке %d/%d: %s", attempt, retries, exc)
                if attempt == retries:
                    return {"status": "error", "message": f"Ошибка связи с ИИ: {exc}"}
                await asyncio.sleep(2 ** attempt)

    return {"status": "error", "message": "Не удалось получить ответ от ИИ после всех попыток."}


async def safe_generate_content(
    prompt: str,
    retries: int = config.GEMINI_RETRIES,
    json_mode: bool = False,
) -> dict:
    """Standard Gemini request without web search."""
    return await _gemini_request(
        prompt, config.GEMINI_MODEL, retries, json_mode=json_mode,
    )


async def safe_generate_with_search(
    prompt: str,
    retries: int = config.GEMINI_RETRIES,
) -> dict:
    """
    Gemini request with Google Search grounding enabled.
    Requires a model that supports grounding (see config.GEMINI_SEARCH_MODEL).
    """
    search_tool = genai_types.Tool(google_search=genai_types.GoogleSearch())
    return await _gemini_request(prompt, config.GEMINI_SEARCH_MODEL, retries, tools=[search_tool])


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def escape_html(text: str) -> str:
    """Escape characters that have special meaning in Telegram HTML mode."""
    return html.escape(str(text), quote=False)


def clean_json_text(text: str) -> str:
    """
    Strip markdown code fences and extract the outermost JSON object.
    Returns "{}" if nothing useful is found.
    """
    if not text:
        return "{}"
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    text = text.strip()
    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    return match.group(0) if match else "{}"
