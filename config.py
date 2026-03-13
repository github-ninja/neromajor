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


TELEGRAM_TOKEN: str = _require_env("TELEGRAM_TOKEN")
DB_URL: str = _require_env("DB_URL")
GEMINI_API_KEY: str = _require_env("GEMINI_API_KEY")

# Gemini
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite")
GEMINI_TEMPERATURE: float = float(os.getenv("GEMINI_TEMPERATURE", "0.3"))
GEMINI_TIMEOUT: float = float(os.getenv("GEMINI_TIMEOUT", "90.0"))
GEMINI_RETRIES: int = int(os.getenv("GEMINI_RETRIES", "5"))

# DB connection pool
DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

# Summary
SUMMARY_DEFAULT_MESSAGES: int = int(os.getenv("SUMMARY_DEFAULT_MESSAGES", "100"))
SUMMARY_MAX_MESSAGES: int = int(os.getenv("SUMMARY_MAX_MESSAGES", "200"))

# Profile
PROFILE_MAX_MESSAGES: int = int(os.getenv("PROFILE_MAX_MESSAGES", "500"))

# Scheduler: interval between messages in minutes (random within range)
# Default: every 90–150 min → ~10 messages per 09:00–22:00 window
SCHEDULER_INTERVAL_MIN: int = int(os.getenv("SCHEDULER_INTERVAL_MIN", "90"))
SCHEDULER_INTERVAL_MAX: int = int(os.getenv("SCHEDULER_INTERVAL_MAX", "150"))
