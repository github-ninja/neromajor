"""Loyalty ("patriotism") index — the scoring shown in /stats."""

from dataclasses import dataclass
from datetime import datetime

from bot.db.repo import OffenderStats

BONUS_CAP = 50


@dataclass(frozen=True, slots=True)
class Loyalty:
    index: int
    clean_days: int
    daily_recovery: int
    bonus: int


def calc_loyalty(count: int, fines: int, days: int, clean_days: int) -> Loyalty:
    """
    Index is NOT clamped — it can go negative so the full severity is visible.

    Penalty: 3 points per violation, 1 per 10 000 ₽ of fines, 2 per sentence day.
    Recovery: max(1, 10 - count // 2) points per clean day (slower for habitual
    offenders), at most +50 in total.
    """
    penalty = count * 3 + fines // 10_000 + days * 2
    daily_recovery = max(1, 10 - count // 2)
    bonus = min(BONUS_CAP, max(0, clean_days) * daily_recovery)
    return Loyalty(100 - penalty + bonus, clean_days, daily_recovery, bonus)


def loyalty_for(stats: OffenderStats, now: datetime) -> Loyalty:
    clean_days = max(0, (now - stats.last_violation).days)
    return calc_loyalty(stats.total_count, stats.total_fines, stats.total_days, clean_days)


def icon(index: int) -> str:
    if index >= 80:
        return "🟢"
    if index >= 40:
        return "🟡"
    if index >= 0:
        return "🔴"
    return "💀"
