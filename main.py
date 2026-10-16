"""Entry point: bot setup, handler registration, startup/shutdown lifecycle."""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.filters import Command

import config

import db
from handlers.case import handle_case
from handlers.guest import handle_guest_message
from handlers.message import store_message, set_bot
from handlers.profile import handle_profile
from handlers.stats import handle_stats
from handlers.stats_reset import handle_stats_reset
from handlers.summary import handle_summary
from scheduler import scheduler_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

config.validate()

bot = Bot(token=config.TELEGRAM_TOKEN)
dp = Dispatcher()

# Specific command routes must come before the catch-all store_message handler
dp.message(Command("summary"))(handle_summary)
dp.message(Command("stats"))(handle_stats)
dp.message(Command("case"))(handle_case)
dp.message(Command("profile"))(handle_profile)
dp.message(Command("stats_reset"))(handle_stats_reset)
dp.message()(store_message)
dp.guest_message()(handle_guest_message)


async def on_startup() -> None:
    await db.init_db()
    me = await bot.get_me()
    set_bot(bot, me.id, me.username or "")
    asyncio.create_task(scheduler_loop(bot))
    logger.info("Бот запущен. ID=%d, username=@%s.", me.id, me.username)


async def on_shutdown() -> None:
    db.close_pool()
    await bot.session.close()
    logger.info("Бот остановлен.")


async def main() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    allowed = dp.resolve_used_update_types()
    logger.info("Подписка на апдейты: %s", allowed)
    await dp.start_polling(bot, allowed_updates=allowed)


if __name__ == "__main__":
    asyncio.run(main())
