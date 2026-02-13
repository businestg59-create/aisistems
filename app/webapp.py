from __future__ import annotations

import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Update
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import load_config
from app.db import Database
from app.handlers import build_admin_router, build_business_router

logger = logging.getLogger(__name__)

config = load_config()
db = Database(config.database_url)
bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
dp.include_router(build_business_router(db, config))
dp.include_router(build_admin_router(db))

app = FastAPI(title="Telegram Business Bot")


@app.on_event("startup")
async def on_startup() -> None:
    await db.create_pool()

    if config.mode == "webhook":
        if not config.webhook_base_url:
            raise ValueError("WEBHOOK_BASE_URL is required when MODE=webhook")

        full_webhook_url = f"{config.webhook_base_url.rstrip('/')}{config.webhook_path}"
        await bot.set_webhook(
            url=full_webhook_url,
            drop_pending_updates=False,
            allowed_updates=dp.resolve_used_update_types(),
        )
        logger.info("Webhook set to %s", full_webhook_url)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if config.mode == "webhook":
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            logger.exception("Failed to delete webhook on shutdown")

    await bot.session.close()
    await db.close()


@app.get("/")
async def root() -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/health")
async def health() -> JSONResponse:
    db_ok = await db.ping(timeout_seconds=1.0)
    return JSONResponse({"ok": db_ok})


@app.get("/ready")
async def ready() -> JSONResponse:
    db_ok = await db.ping(timeout_seconds=1.0)
    status = 200 if db_ok else 503
    return JSONResponse({"ok": db_ok}, status_code=status)


@app.post(config.webhook_path)
async def telegram_webhook(request: Request) -> dict[str, bool]:
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot=bot, update=update)
    return {"ok": True}
