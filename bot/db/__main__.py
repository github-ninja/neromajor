"""python -m bot.db — apply pending migrations and exit."""

import asyncio
import logging
import os

from bot import db


async def main() -> None:
    pool = await db.create_pool(os.environ["DB_URL"], 1, 1)
    try:
        await db.migrate(pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(main())
