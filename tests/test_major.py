from datetime import date

from bot.config import Settings
from bot.services.major import planned_time


def test_planned_time_is_inside_window_and_stable():
    settings = Settings.from_env()
    day = date(2026, 10, 16)
    times = [planned_time(chat_id, day, settings) for chat_id in range(-500, 500)]
    assert all(settings.active_hour_start <= t.hour < settings.active_hour_end for t in times)
    assert all(t.date() == day for t in times)
    assert planned_time(42, day, settings) == planned_time(42, day, settings)
    assert len({t.hour for t in times}) > 5  # actually spread out
