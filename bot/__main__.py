"""Entry point: python -m bot"""

import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot import db
from bot.ai.gemini import Gemini
from bot.config import ConfigError, Settings
from bot.db.repo import Repository
from bot.handlers import build_router
from bot.handlers.messages import MentionLimiter
from bot.services.audit import Auditor
from bot.services.major import Major

logger = logging.getLogger("bot")


async def run(settings: Settings) -> None:
    pool = await db.create_pool(settings.db_url, settings.db_pool_min, settings.db_pool_max)
    try:
        await db.migrate(pool)
        repo = Repository(pool)
        ai = Gemini(settings)
        bot = Bot(
            settings.telegram_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
        )
        auditor = Auditor(repo, ai, settings)
        major = Major(bot, repo, ai, settings)

        # Everything below is injected into handlers by parameter name.
        dp = Dispatcher(
            settings=settings,
            repo=repo,
            ai=ai,
            auditor=auditor,
            major=major,
            mentions=MentionLimiter(settings.mention_cooldown),
        )
        dp.include_router(build_router())

        background: list[asyncio.Task] = []

        async def on_startup() -> None:
            me = await bot.me()
            background.append(asyncio.create_task(auditor.run_forever(), name="auditor"))
            background.append(asyncio.create_task(major.run_forever(), name="major"))
            logger.info("Бот запущен. ID=%d, username=@%s.", me.id, me.username)

        async def on_shutdown() -> None:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            logger.info("Бот остановлен.")

        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # google_genai logs "AFC is enabled" on every request.
    logging.getLogger("google_genai.models").setLevel(logging.WARNING)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        logger.critical("Ошибка конфигурации: %s", exc)
        sys.exit(1)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
