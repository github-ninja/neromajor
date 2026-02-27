"""Entry point: bot setup, handler registration, startup/shutdown lifecycle."""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.filters import Command

import config  # noqa: F401 (side-effect: validates env vars)

import db
from handlers.case import handle_case
from handlers.message import store_message
from handlers.profile import handle_profile
from handlers.stats import handle_stats
from handlers.stats_reset import handle_stats_reset
from handlers.summary import handle_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=config.TELEGRAM_TOKEN)
dp = Dispatcher()

# Command handlers (specific routes before the catch-all)
dp.message(Command("summary"))(handle_summary)
dp.message(Command("stats"))(handle_stats)
dp.message(Command("case"))(handle_case)
dp.message(Command("profile"))(handle_profile)
dp.message(Command("stats_reset"))(handle_stats_reset)
dp.message()(store_message)


async def on_startup() -> None:
    await db.init_db()
    logger.info("Бот запущен.")


async def on_shutdown() -> None:
    db.close_pool()
    await bot.session.close()
    logger.info("Бот остановлен.")


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
