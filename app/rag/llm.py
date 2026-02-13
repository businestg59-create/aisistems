from __future__ import annotations

import json
import logging
import re
from typing import Any

from openai import AsyncOpenAI

from app.config import Config
from app.rag.prompts import SYSTEM_PROMPT, build_user_prompt
from app.rag.store import RetrievedChunk

logger = logging.getLogger(__name__)

MAX_CHUNK_TEXT = 1400


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        return {}
    return {}


async def _call_json(
    config: Config,
    *,
    instructions: str,
    user_input: str,
    default: dict[str, Any],
    max_output_tokens: int = 220,
) -> dict[str, Any]:
    if not config.openai_api_key:
        return default

    client = AsyncOpenAI(api_key=config.openai_api_key)
    try:
        response = await client.responses.create(
            model=config.openai_model,
            instructions=instructions,
            input=user_input,
            temperature=0.0,
            max_output_tokens=max_output_tokens,
        )
    except Exception:
        logger.exception("LLM JSON call failed")
        return default

    raw = (response.output_text or "").strip()
    parsed = _extract_json_object(raw)
    if not parsed:
        return default
    return parsed


async def classify_intent(config: Config, message: str) -> dict[str, Any]:
    default = {"intent": "unknown", "confidence": 0.0, "reason": "fallback"}
    result = await _call_json(
        config,
        instructions=(
            "Классифицируй намерение клиента в JSON. "
            "Допустимые intent: bot, site, automation, other, unknown. "
            "Верни строго JSON: {\"intent\":..., \"confidence\":0..1, \"reason\":\"...\"}"
        ),
        user_input=message,
        default=default,
    )
    intent = str(result.get("intent", "unknown")).strip().lower()
    if intent not in {"bot", "site", "automation", "other", "unknown"}:
        intent = "unknown"
    confidence = float(result.get("confidence", 0.0) or 0.0)
    confidence = min(1.0, max(0.0, confidence))
    reason = str(result.get("reason", ""))
    return {"intent": intent, "confidence": confidence, "reason": reason}


async def classify_risk(config: Config, message: str) -> dict[str, Any]:
    default = {
        "need_human": False,
        "negative": False,
        "urgency": "low",
        "reason": "fallback",
        "confidence": 0.0,
    }
    result = await _call_json(
        config,
        instructions=(
            "Определи риск эскалации клиента в JSON. "
            "Верни строго: "
            "{\"need_human\":true|false,"
            "\"negative\":true|false,"
            "\"urgency\":\"low|medium|high\","
            "\"reason\":\"...\","
            "\"confidence\":0..1}"
        ),
        user_input=message,
        default=default,
    )
    urgency = str(result.get("urgency", "low")).lower()
    if urgency not in {"low", "medium", "high"}:
        urgency = "low"
    return {
        "need_human": bool(result.get("need_human", False)),
        "negative": bool(result.get("negative", False)),
        "urgency": urgency,
        "reason": str(result.get("reason", "")),
        "confidence": float(result.get("confidence", 0.0) or 0.0),
    }


async def extract_lead_fields(config: Config, message: str) -> dict[str, str | None]:
    default = {
        "need": None,
        "budget": None,
        "timeline": None,
        "contact_method": None,
        "phone": None,
    }
    result = await _call_json(
        config,
        instructions=(
            "Извлеки поля лида в JSON. "
            "Верни строго: "
            "{\"need\":string|null,"
            "\"budget\":string|null,"
            "\"timeline\":string|null,"
            "\"contact_method\":string|null,"
            "\"phone\":string|null}"
        ),
        user_input=message,
        default=default,
    )
    return {
        "need": _none_or_str(result.get("need")),
        "budget": _none_or_str(result.get("budget")),
        "timeline": _none_or_str(result.get("timeline")),
        "contact_method": _none_or_str(result.get("contact_method")),
        "phone": _none_or_str(result.get("phone")),
    }


def _none_or_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


async def generate_answer(
    config: Config,
    question: str,
    retrieved_chunks: list[RetrievedChunk],
    lead_context: dict | None = None,
) -> str:
    if not config.openai_api_key:
        return (
            "Сейчас ИИ-режим недоступен. "
            "Извините, не совсем понимаю, о чем речь. Уточните, пожалуйста, что именно хотите сделать?"
        )

    context_parts: list[str] = []
    for i, chunk in enumerate(retrieved_chunks, start=1):
        text = chunk.text[:MAX_CHUNK_TEXT]
        context_parts.append(f"[{i}] title={chunk.title or '-'}; source={chunk.source_url}\n{text}")

    if not context_parts:
        return (
            "Извините, не совсем понимаю, о чем речь. "
            "Уточните, пожалуйста, что именно вы хотите: бот / сайт / автоматизация / другое?"
        )

    lead_ctx = str(lead_context) if lead_context else None
    user_prompt = build_user_prompt(
        question=question,
        context_text="\n\n".join(context_parts),
        lead_context=lead_ctx,
    )

    client = AsyncOpenAI(api_key=config.openai_api_key)
    try:
        response = await client.responses.create(
            model=config.openai_model,
            instructions=SYSTEM_PROMPT,
            input=user_prompt,
            temperature=0.2,
            max_output_tokens=550,
        )
    except Exception:
        logger.exception("LLM generation failed")
        return (
            "Извините, не совсем понимаю, о чем речь. "
            "Уточните, пожалуйста, что именно хотите сделать: бот / сайт / автоматизация / другое?"
        )

    answer_text = (response.output_text or "").strip()
    if not answer_text:
        answer_text = (
            "Извините, не совсем понимаю, о чем речь. "
            "Уточните, пожалуйста, что именно хотите сделать: бот / сайт / автоматизация / другое?"
        )

    source_lines: list[str] = []
    seen: set[str] = set()
    for chunk in retrieved_chunks:
        url = chunk.source_url.strip()
        if url and url not in seen:
            seen.add(url)
            source_lines.append(f"Источник: {url}")

    if source_lines:
        answer_text = f"{answer_text}\n\n" + "\n".join(source_lines)

    return answer_text
