from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(slots=True)
class Config:
    bot_token: str
    admin_chat_id: int | None
    database_url: str
    mode: str
    webhook_base_url: str | None
    webhook_path: str
    openai_api_key: str | None
    openai_model: str
    openai_embedding_model: str
    kb_sites: tuple[str, ...]


def load_config() -> Config:
    load_dotenv()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("BOT_TOKEN is required")

    admin_chat_id_raw = os.getenv("ADMIN_CHAT_ID", "").strip()
    admin_chat_id = int(admin_chat_id_raw) if admin_chat_id_raw else None

    mode = os.getenv("MODE", "webhook").strip().lower()
    if mode not in {"polling", "webhook"}:
        raise ValueError("MODE must be either 'polling' or 'webhook'")

    webhook_base_url = (
        os.getenv("WEBHOOK_BASE_URL", "").strip()
        or os.getenv("WEBHOOK_URL", "").strip()
        or None
    )
    webhook_path = os.getenv("WEBHOOK_PATH", "/tg/webhook").strip() or "/tg/webhook"
    if not webhook_path.startswith("/"):
        webhook_path = f"/{webhook_path}"

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise ValueError("DATABASE_URL is required")

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip() or None
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    openai_embedding_model = (
        os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small").strip()
        or "text-embedding-3-small"
    )
    kb_sites_raw = os.getenv(
        "KB_SITES",
        "https://ai-sistemy.ru/,https://aisistems-tg.ru/",
    )
    kb_sites = tuple(site.strip() for site in kb_sites_raw.split(",") if site.strip())

    return Config(
        bot_token=token,
        admin_chat_id=admin_chat_id,
        database_url=database_url,
        mode=mode,
        webhook_base_url=webhook_base_url,
        webhook_path=webhook_path,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_embedding_model=openai_embedding_model,
        kb_sites=kb_sites,
    )
