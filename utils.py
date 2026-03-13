"""Shared utilities: Gemini wrapper, text helpers."""

import asyncio
import html
import logging
import re

from google import genai

import config

logger = logging.getLogger(__name__)

_gemini_client = genai.Client(api_key=config.GEMINI_API_KEY)


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

async def safe_generate_content(prompt: str, retries: int = config.GEMINI_RETRIES) -> dict:
    """
    Generate content via Gemini with exponential back-off retry logic.

    Returns a dict with:
      {"status": "ok",         "text": "..."}
      {"status": "rate_limit", "message": "..."}
      {"status": "error",      "message": "..."}
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "Gemini запрос, попытка %d/%d (длина промпта: %d симв.)",
                attempt, retries, len(prompt),
            )
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    _gemini_client.models.generate_content,
                    model=config.GEMINI_MODEL,
                    contents=prompt,
                    config={"temperature": config.GEMINI_TEMPERATURE},
                ),
                timeout=config.GEMINI_TIMEOUT,
            )
            logger.info("Gemini ответил успешно (попытка %d).", attempt)
            return {"status": "ok", "text": response.text}

        except asyncio.TimeoutError:
            logger.warning("Gemini таймаут на попытке %d/%d.", attempt, retries)
            if attempt == retries:
                return {"status": "error", "message": "Превышено время ожидания ответа от ИИ."}
            await asyncio.sleep(2 ** attempt)  # 2, 4, 8, 16 с

        except Exception as exc:
            error_str = str(exc).lower()
            is_rate_limit = any(
                kw in error_str for kw in ("429", "resourceexhausted", "quota")
            )

            if is_rate_limit:
                logger.warning("Gemini rate limit (429) на попытке %d/%d: %s", attempt, retries, exc)
                if attempt == retries:
                    return {
                        "status": "rate_limit",
                        "message": "Достигнут лимит запросов к ИИ. Подождите 1–2 минуты и повторите команду.",
                    }
                await asyncio.sleep(10 * attempt)  # 10, 20, 30, 40 с
            else:
                logger.error("Ошибка Gemini на попытке %d/%d: %s", attempt, retries, exc)
                if attempt == retries:
                    return {"status": "error", "message": f"Ошибка связи с ИИ: {exc}"}
                # Небольшая пауза перед повтором на нелимитные ошибки
                await asyncio.sleep(2 ** attempt)

    # Страховочный возврат (не должен достигаться)
    return {"status": "error", "message": "Не удалось получить ответ от ИИ после всех попыток."}


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def escape_html(text: str) -> str:
    """Escape characters that have special meaning in Telegram HTML mode."""
    return html.escape(str(text), quote=False)


def clean_json_text(text: str) -> str:
    """
    Strip code fences and extract the outermost JSON object from a raw
    Gemini response. Returns "{}" if nothing useful is found.
    """
    if not text:
        return "{}"

    # Remove ```json ... ``` wrappers
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    text = text.strip()

    match = re.search(r"\{[\s\S]*\}", text, re.DOTALL)
    return match.group(0) if match else "{}"