from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.types import Message

from app.db import Database

logger = logging.getLogger(__name__)


def build_admin_router(db: Database) -> Router:
    router = Router(name="admin")

    @router.message(F.chat.type == "private")
    async def on_private_message(message: Message) -> None:
        try:
            await db.set_admin_chat_id(message.chat.id)
            await message.answer(
                "✅ Админ-чат подключен. Теперь все уведомления о клиентах, негативе и запросах на человека будут приходить сюда."
            )
        except Exception:
            logger.exception("Failed to register admin chat")

    return router
