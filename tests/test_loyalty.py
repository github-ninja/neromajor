from datetime import datetime, timedelta, timezone

from bot.db.repo import OffenderStats
from bot.services.loyalty import calc_loyalty, icon, loyalty_for


def test_penalty_components():
    # 2 violations, 25 000 ₽, 3 days: 6 + 2 + 6 = 14 penalty, no bonus
    assert calc_loyalty(2, 25_000, 3, 0).index == 86


def test_index_is_not_clamped():
    assert calc_loyalty(10, 1_500_000, 365, 0).index < 0


def test_bonus_capped_and_slower_for_repeat_offenders():
    first = calc_loyalty(1, 0, 0, 100)
    assert first.daily_recovery == 10 and first.bonus == 50
    habitual = calc_loyalty(30, 0, 0, 3)
    assert habitual.daily_recovery == 1 and habitual.bonus == 3


def test_loyalty_for_counts_days_since_last_violation():
    now = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)
    stats = OffenderStats(1, "Иван", 1, 10_000, 0, now - timedelta(days=4, hours=1), 0, 0, 0)
    assert loyalty_for(stats, now).clean_days == 4


def test_icons():
    assert [icon(i) for i in (100, 80, 40, 0, -1)] == ["🟢", "🟢", "🟡", "🔴", "💀"]
