"""Settings loaded from environment variables, validated once at startup."""

import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo

_THINKING_LEVELS = {"minimal", "low", "medium", "high"}


class ConfigError(RuntimeError):
    pass


def _str(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise ConfigError(f"Переменная окружения '{name}' не задана.")
    return value


def _int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw else default
    except ValueError:
        raise ConfigError(f"'{name}' должна быть целым числом, получено {raw!r}.") from None
    if value < minimum:
        raise ConfigError(f"'{name}' должна быть >= {minimum}, получено {value}.")
    return value


def _float(name: str, default: float, minimum: float = 0.0, maximum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw else default
    except ValueError:
        raise ConfigError(f"'{name}' должна быть числом, получено {raw!r}.") from None
    if value < minimum or (maximum is not None and value > maximum):
        raise ConfigError(f"'{name}' вне допустимого диапазона: {value}.")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    telegram_token: str
    db_url: str
    db_pool_min: int
    db_pool_max: int

    gemini_api_key: str
    gemini_model: str  # everything the Major says
    gemini_lite_model: str  # bulk audit, and fallback when the main model is unavailable
    # None = model default. Sampling parameters are deliberately absent:
    # Gemini ignores them since 3.6 and upcoming models reject them.
    gemini_thinking_level: str | None
    gemini_timeout: float
    gemini_retries: int
    gemini_rpm: int  # client-side cap on requests per minute per model (free tier friendly)
    # Total time a conversational reply may take before a canned one is sent.
    gemini_reply_deadline: float
    gemini_reply_attempt_timeout: float  # per attempt within that deadline

    timezone: ZoneInfo
    active_hour_start: int
    active_hour_end: int

    # Background audit
    audit_interval: int  # minutes between background passes
    audit_batch_messages: int
    audit_batch_chars: int
    audit_daily_requests: int  # Gemini requests the auditor may spend per day
    audit_stats_requests: int  # extra requests /stats may spend to catch up

    summary_default_messages: int
    summary_max_messages: int
    profile_max_messages: int

    # Reactive remarks during live conversations
    react_probability: float
    react_cooldown: int  # minutes since the bot last spoke in the chat
    activity_window: int  # minutes
    activity_min_messages: int
    # Proactive daily remark: skip chats silent for longer than this
    inactive_hours: int

    mention_context_messages: int
    mention_cooldown: int  # seconds between replies to the same user

    @classmethod
    def from_env(cls) -> "Settings":
        thinking = os.getenv("GEMINI_THINKING_LEVEL", "").strip().lower() or None
        if thinking is not None and thinking not in _THINKING_LEVELS:
            raise ConfigError(f"GEMINI_THINKING_LEVEL: ожидается одно из {sorted(_THINKING_LEVELS)}.")

        start = _int("ACTIVE_HOUR_START", 9)
        end = _int("ACTIVE_HOUR_END", 22)
        if not 0 <= start < end <= 24:
            raise ConfigError("Ожидается 0 <= ACTIVE_HOUR_START < ACTIVE_HOUR_END <= 24.")

        pool_min = _int("DB_POOL_MIN", 1, minimum=1)
        pool_max = _int("DB_POOL_MAX", 10, minimum=1)
        if pool_min > pool_max:
            raise ConfigError("DB_POOL_MIN не может быть больше DB_POOL_MAX.")

        return cls(
            telegram_token=_str("TELEGRAM_TOKEN"),
            db_url=_str("DB_URL"),
            db_pool_min=pool_min,
            db_pool_max=pool_max,
            gemini_api_key=_str("GEMINI_API_KEY"),
            gemini_model=_str("GEMINI_MODEL", "gemini-3.8-flash"),
            gemini_lite_model=_str("GEMINI_LITE_MODEL", "gemini-3.1-flash-lite"),
            gemini_thinking_level=thinking,
            gemini_timeout=_float("GEMINI_TIMEOUT", 90.0, minimum=1.0),
            gemini_retries=_int("GEMINI_RETRIES", 4, minimum=1),
            gemini_rpm=_int("GEMINI_RPM", 10, minimum=1),
            gemini_reply_deadline=_float("GEMINI_REPLY_DEADLINE", 25.0, minimum=1.0),
            gemini_reply_attempt_timeout=_float("GEMINI_REPLY_ATTEMPT_TIMEOUT", 10.0, minimum=1.0),
            timezone=ZoneInfo(_str("BOT_TIMEZONE", "Europe/Moscow")),
            active_hour_start=start,
            active_hour_end=end,
            audit_interval=_int("AUDIT_INTERVAL", 360, minimum=1),
            audit_batch_messages=_int("AUDIT_BATCH_MESSAGES", 400, minimum=1),
            audit_batch_chars=_int("AUDIT_BATCH_CHARS", 30_000, minimum=1000),
            audit_daily_requests=_int("AUDIT_DAILY_REQUESTS", 40),
            audit_stats_requests=_int("AUDIT_STATS_REQUESTS", 3),
            summary_default_messages=_int("SUMMARY_DEFAULT_MESSAGES", 100, minimum=1),
            summary_max_messages=_int("SUMMARY_MAX_MESSAGES", 200, minimum=1),
            profile_max_messages=_int("PROFILE_MAX_MESSAGES", 500, minimum=1),
            react_probability=_float("SCHEDULER_REACT_PROBABILITY", 0.15, maximum=1.0),
            react_cooldown=_int("SCHEDULER_COOLDOWN", 30),
            activity_window=_int("SCHEDULER_ACTIVITY_WINDOW", 10, minimum=1),
            activity_min_messages=_int("SCHEDULER_ACTIVITY_MIN_MESSAGES", 5, minimum=1),
            inactive_hours=_int("SCHEDULER_INACTIVE_HOURS", 24, minimum=1),
            mention_context_messages=_int("MENTION_CONTEXT_MESSAGES", 20, minimum=1),
            mention_cooldown=_int("MENTION_COOLDOWN", 10),
        )
