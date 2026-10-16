"""Centralized configuration and environment validation."""

import os
import sys
import logging

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logger.critical("Переменная окружения '%s' не задана. Запуск невозможен.", name)
        sys.exit(1)
    return value


def validate() -> None:
    """
    Force-evaluate required environment variables. Called explicitly from
    main() so linters don't strip the config import as unused and so the
    contract is visible at the call site.
    """
    _ = (TELEGRAM_TOKEN, DB_URL, GEMINI_API_KEY)


TELEGRAM_TOKEN: str = _require_env("TELEGRAM_TOKEN")
DB_URL: str = _require_env("DB_URL")
GEMINI_API_KEY: str = _require_env("GEMINI_API_KEY")

# Gemini — standard model for all regular requests
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
# Gemini — model used for search-grounded requests; must support grounding
GEMINI_SEARCH_MODEL: str = os.getenv("GEMINI_SEARCH_MODEL", "gemini-3.6-flash")

GEMINI_TIMEOUT: float = float(os.getenv("GEMINI_TIMEOUT", "90.0"))
GEMINI_RETRIES: int = int(os.getenv("GEMINI_RETRIES", "5"))

# DB connection pool
DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

# /summary
SUMMARY_DEFAULT_MESSAGES: int = int(os.getenv("SUMMARY_DEFAULT_MESSAGES", "100"))
SUMMARY_MAX_MESSAGES: int = int(os.getenv("SUMMARY_MAX_MESSAGES", "200"))

# /profile
PROFILE_MAX_MESSAGES: int = int(os.getenv("PROFILE_MAX_MESSAGES", "500"))

# Scheduler — reactive mode (triggered by incoming messages)
SCHEDULER_REACT_PROBABILITY: float = float(os.getenv("SCHEDULER_REACT_PROBABILITY", "0.15"))
SCHEDULER_COOLDOWN: int = int(os.getenv("SCHEDULER_COOLDOWN", "30"))               # minutes between bot messages per chat
SCHEDULER_ACTIVITY_WINDOW: int = int(os.getenv("SCHEDULER_ACTIVITY_WINDOW", "10")) # minutes to look back for activity
SCHEDULER_ACTIVITY_MIN_MESSAGES: int = int(os.getenv("SCHEDULER_ACTIVITY_MIN_MESSAGES", "5"))

# Scheduler — proactive mode (once per day at a random time within the active window)
SCHEDULER_INACTIVE_HOURS: int = int(os.getenv("SCHEDULER_INACTIVE_HOURS", "24"))   # skip chat if silent for this long

# Mention/reply handler
MENTION_CONTEXT_MESSAGES: int = int(os.getenv("MENTION_CONTEXT_MESSAGES", "20"))
