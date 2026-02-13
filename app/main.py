from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import Config, load_config
from app.db import Database
from app.handlers import build_admin_router, build_business_router


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def create_bot_and_dispatcher(config: Config, db: Database) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(build_business_router(db, config))
    dp.include_router(build_admin_router(db))
    return bot, dp


async def _run_polling(config: Config) -> None:
    db = Database(config.database_url)
    await db.create_pool()

    bot, dp = create_bot_and_dispatcher(config, db)
    await bot.delete_webhook(drop_pending_updates=False)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        await db.close()


async def _run(config: Config) -> None:
    if config.mode == "webhook":
        import uvicorn

        uvicorn.run(
            "app.webapp:app",
            host="0.0.0.0",
            port=int(os.getenv("PORT", "8080")),
            log_level="info",
        )
        return

    await _run_polling(config)


def run() -> None:
    setup_logging()
    config = load_config()
    asyncio.run(_run(config))
