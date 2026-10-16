import pytest

from bot.config import ConfigError, Settings


def test_defaults_load():
    s = Settings.from_env()
    assert s.gemini_model and s.timezone.key == "Europe/Moscow"
    assert s.gemini_thinking_level is None


def test_invalid_values_rejected(monkeypatch):
    monkeypatch.setenv("SCHEDULER_REACT_PROBABILITY", "1.5")
    with pytest.raises(ConfigError):
        Settings.from_env()


def test_missing_token(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TOKEN")
    with pytest.raises(ConfigError):
        Settings.from_env()
