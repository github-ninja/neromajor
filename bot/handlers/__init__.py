"""Router assembly. Order matters: commands first, the catch-all message router last."""

from aiogram import Router

from bot.handlers import case, guest, messages, profile, stats, stats_reset, summary


def build_router() -> Router:
    root = Router(name="root")
    root.include_routers(
        stats.router,
        stats_reset.router,
        case.router,
        profile.router,
        summary.router,
        guest.router,
        messages.router,
    )
    return root
