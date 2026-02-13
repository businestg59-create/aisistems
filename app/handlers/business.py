from __future__ import annotations

import json
import logging
import re

from aiogram import Bot, Router
from aiogram.types import BusinessConnection, Message

from app.config import Config
from app.db import Database, LeadInfo
from app.keyboards import (
    MANAGER_BUTTON,
    budget_keyboard,
    contact_keyboard,
    deadline_keyboard,
    need_keyboard,
    remove_keyboard,
)
from app.rag.llm import classify_intent, classify_risk, extract_lead_fields, generate_answer
from app.rag.store import RAGStore

logger = logging.getLogger(__name__)

STEP_WELCOME = 0
STEP_NEED = 1
STEP_BUDGET = 2
STEP_DEADLINE = 3
STEP_CONTACT_METHOD = 4
STEP_PHONE = 5
STEP_CALL_TIME = 6
STEP_DONE = 7

MAX_USER_QUESTION_LEN = 2000
TEXT_NON_MESSAGE = "<не текстовое сообщение>"

ALLOWED_NEED = {"бот", "сайт", "автоматизация", "другое"}
ALLOWED_BUDGET = {"до 30k", "30–80k", "80–150k", "150k+"}
ALLOWED_DEADLINE = {"срочно 1–3 дня", "1–2 недели", "в течение месяца", "не горит"}
ALLOWED_CONTACT = {"в Telegram", "по телефону", "созвон"}

HUMAN_REQUEST_PATTERNS = (
    "оператор",
    "менеджер",
    "человек",
    "живой",
    "свяжите",
    "позовите",
    "переключите",
    "не бот",
    "хочу поговорить",
    "передай руководителю",
)
NEGATIVE_HARD_PATTERNS = (
    "мошенники",
    "обман",
    "развод",
    "верните деньги",
    "обманули",
    "суд",
    "прокуратур",
    "роспотребнадзор",
    "заявление",
    "жалоба",
    "претензия",
)
NEGATIVE_SOFT_PATTERNS = (
    "плохой сервис",
    "вы достали",
    "ужас",
    "ненавижу",
    "не нравится",
    "разочарован",
)
PROFANITY_PATTERNS = ("идиот", "тупые", "сука", "блять", "хер", "долбо", "уроды")


def build_business_router(db: Database, config: Config) -> Router:
    router = Router(name="business")
    rag_store = RAGStore(config)

    @router.business_connection()
    async def on_business_connection(event: BusinessConnection, bot: Bot) -> None:
        try:
            await db.upsert_connection(
                business_connection_id=event.id,
                owner_user_id=event.user.id,
                owner_user_chat_id=event.user_chat_id,
                can_reply=event.can_reply,
            )
            logger.info(
                "Business connection updated: id=%s can_reply=%s user_chat_id=%s",
                event.id,
                event.can_reply,
                event.user_chat_id,
            )
            if event.user_chat_id:
                await db.set_admin_chat_id(event.user_chat_id)
        except Exception:
            logger.exception("Failed to process business_connection update")

    @router.business_message()
    async def on_business_message(message: Message, bot: Bot) -> None:
        if not message.business_connection_id:
            logger.warning("business_message without business_connection_id")
            return

        bcid = message.business_connection_id
        client_chat_id = message.chat.id
        text = (message.text or message.caption or "").strip() or TEXT_NON_MESSAGE
        question = text[:MAX_USER_QUESTION_LEN]
        username = message.from_user.username if message.from_user else None
        full_name = (
            message.from_user.full_name
            if message.from_user
            else (message.chat.full_name if message.chat else None)
        )

        try:
            await _ensure_connection_info(bot=bot, db=db, business_connection_id=bcid)

            is_new_client = await db.touch_client(
                business_connection_id=bcid,
                client_chat_id=client_chat_id,
                username=username,
                full_name=full_name,
            )
            if is_new_client:
                await _notify_new_client(
                    bot=bot,
                    db=db,
                    config=config,
                    business_connection_id=bcid,
                    client_chat_id=client_chat_id,
                    username=username,
                    full_name=full_name,
                    text=question,
                )

            connection = await db.get_connection(bcid)
            if not connection or not connection.can_reply:
                await _notify_cannot_reply(
                    bot=bot,
                    db=db,
                    config=config,
                    business_connection_id=bcid,
                    client_chat_id=client_chat_id,
                )
                return

            lead = await db.get_lead(bcid, client_chat_id)
            if lead is None:
                lead = await db.create_or_reset_lead(bcid, client_chat_id)

            await db.update_lead_fields(
                bcid,
                client_chat_id,
                last_client_message=question,
            )

            if question == MANAGER_BUTTON:
                await _escalate_to_human(
                    bot=bot,
                    db=db,
                    config=config,
                    business_connection_id=bcid,
                    client_chat_id=client_chat_id,
                    full_name=full_name,
                    username=username,
                    text=question,
                    lead=lead,
                    reason="Запрос кнопкой 'Позвать менеджера'",
                    urgency="high",
                    need_human=True,
                    negative=False,
                )
                return

            risk = _rule_based_risk(question)
            if risk is None:
                risk = await classify_risk(config, question)

            if _should_critical_escalate(risk):
                await _escalate_to_human(
                    bot=bot,
                    db=db,
                    config=config,
                    business_connection_id=bcid,
                    client_chat_id=client_chat_id,
                    full_name=full_name,
                    username=username,
                    text=question,
                    lead=lead,
                    reason=str(risk.get("reason", "")),
                    urgency=str(risk.get("urgency", "high")),
                    need_human=bool(risk.get("need_human", False)),
                    negative=bool(risk.get("negative", False)),
                )
                return

            if bool(risk.get("negative")) and str(risk.get("urgency")) == "medium":
                await _send_business_message(
                    bot,
                    bcid,
                    client_chat_id,
                    (
                        "Понимаю ваше недовольство и извиняюсь за неудобства. "
                        "Постараюсь решить вопрос максимально быстро. "
                        "Если хотите, могу сразу подключить менеджера."
                    ),
                    reply_markup=need_keyboard(),
                )

            if lead.step == STEP_DONE:
                await db.create_or_reset_lead(bcid, client_chat_id)
                lead = await db.get_lead(bcid, client_chat_id)
                assert lead is not None

            if 0 < lead.step < STEP_DONE:
                await _handle_lead_dialog(
                    bot=bot,
                    db=db,
                    config=config,
                    business_connection_id=bcid,
                    client_chat_id=client_chat_id,
                    client_text=question,
                    lead=lead,
                )
                return

            await _handle_rag_entry(
                bot=bot,
                db=db,
                config=config,
                rag_store=rag_store,
                business_connection_id=bcid,
                client_chat_id=client_chat_id,
                client_text=question,
                lead=lead,
            )

        except Exception:
            logger.exception("Failed to process business_message bcid=%s chat_id=%s", bcid, client_chat_id)

    @router.edited_business_message()
    async def on_edited_business_message(message: Message) -> None:
        logger.info(
            "edited_business_message received: bcid=%s chat_id=%s msg_id=%s",
            message.business_connection_id,
            message.chat.id,
            message.message_id,
        )

    return router


def _should_critical_escalate(risk: dict) -> bool:
    confidence = float(risk.get("confidence", 0.0) or 0.0)
    return (
        bool(risk.get("need_human"))
        or str(risk.get("urgency", "")).lower() == "high"
        or (bool(risk.get("negative")) and confidence >= 0.6)
    )


def _rule_based_risk(text: str) -> dict | None:
    low = text.lower()
    if any(p in low for p in HUMAN_REQUEST_PATTERNS):
        return {
            "need_human": True,
            "negative": False,
            "urgency": "high",
            "reason": "Прямой запрос на оператора",
            "confidence": 0.95,
        }
    if any(p in low for p in NEGATIVE_HARD_PATTERNS) or any(p in low for p in PROFANITY_PATTERNS):
        return {
            "need_human": True,
            "negative": True,
            "urgency": "high",
            "reason": "Сильный негатив/конфликт",
            "confidence": 0.9,
        }
    if any(p in low for p in NEGATIVE_SOFT_PATTERNS):
        return {
            "need_human": False,
            "negative": True,
            "urgency": "medium",
            "reason": "Негатив средней силы",
            "confidence": 0.55,
        }
    return None


async def _escalate_to_human(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
    full_name: str | None,
    username: str | None,
    text: str,
    lead: LeadInfo | None,
    reason: str,
    urgency: str,
    need_human: bool,
    negative: bool,
) -> None:
    await _send_business_message(
        bot,
        business_connection_id,
        client_chat_id,
        (
            "Понимаю вас. Извините за неудобства 🙏\n"
            "Сейчас передам вопрос руководителю/менеджеру. "
            "Пожалуйста, уточните коротко: что случилось и какой результат нужен?"
        ),
        reply_markup=remove_keyboard(),
    )

    should_alert = await db.mark_escalation(
        business_connection_id,
        client_chat_id,
        reason=reason or "Эскалация по сообщению клиента",
        urgency=urgency or "high",
        last_message=text,
        cooldown_minutes=10,
    )
    if not should_alert:
        return

    admin_chat_id = await db.resolve_admin_chat_id(business_connection_id, config.admin_chat_id)
    if not admin_chat_id:
        logger.warning("Cannot send escalation alert: admin chat id is unknown")
        return

    username_text = f"@{username}" if username else "нет username"
    lead_state = _lead_state_text(lead)
    alert_text = (
        "🚨 КРИТИЧНО: клиент просит человека/негатив\n"
        f"Клиент: {full_name or 'без имени'} ({username_text})\n"
        f"chat_id: {client_chat_id}\n"
        f"business_connection_id: {business_connection_id}\n"
        f"need_human={need_human}, negative={negative}, urgency={urgency}\n"
        f"Причина: {reason or '-'}\n"
        f"Текст: {text[:1200]}\n"
        f"Статус лида: {lead_state}"
    )
    await bot.send_message(chat_id=admin_chat_id, text=alert_text)


def _lead_state_text(lead: LeadInfo | None) -> str:
    if lead is None:
        return "step=0, need=-, budget=-, timeline=-, contact=-"
    return (
        f"step={lead.step}, "
        f"need={lead.need or '-'}, "
        f"budget={lead.budget or '-'}, "
        f"timeline={lead.deadline or '-'}, "
        f"contact={lead.contact_method or '-'}"
    )


async def _ensure_connection_info(bot: Bot, db: Database, business_connection_id: str) -> None:
    existing = await db.get_connection(business_connection_id)
    if existing:
        return

    try:
        fetched = await bot.get_business_connection(business_connection_id=business_connection_id)
    except Exception:
        logger.warning("Could not fetch business connection via API: id=%s", business_connection_id)
        return

    await db.upsert_connection(
        business_connection_id=fetched.id,
        owner_user_id=fetched.user.id,
        owner_user_chat_id=fetched.user_chat_id,
        can_reply=fetched.can_reply,
    )


async def _notify_new_client(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
    username: str | None,
    full_name: str | None,
    text: str,
) -> None:
    admin_chat_id = await db.resolve_admin_chat_id(business_connection_id, config.admin_chat_id)
    if not admin_chat_id:
        logger.warning("Cannot notify new client: admin chat id is unknown")
        return

    display_name = full_name or "без имени"
    if username:
        display_name = f"{display_name} (@{username})"

    notify_text = f"🆕 Новый клиент: {display_name} | chat_id={client_chat_id} | текст={text}"
    await bot.send_message(chat_id=admin_chat_id, text=notify_text)


async def _notify_cannot_reply(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
) -> None:
    admin_chat_id = await db.resolve_admin_chat_id(business_connection_id, config.admin_chat_id)
    if not admin_chat_id:
        logger.warning("Cannot send can_reply warning: admin chat id is unknown")
        return

    text = (
        "⚠️ Нет права отвечать клиенту через Business API. "
        f"business_connection_id={business_connection_id}, chat_id={client_chat_id}"
    )
    await bot.send_message(chat_id=admin_chat_id, text=text)


async def _handle_rag_entry(
    bot: Bot,
    db: Database,
    config: Config,
    rag_store: RAGStore,
    business_connection_id: str,
    client_chat_id: int,
    client_text: str,
    lead: LeadInfo,
) -> None:
    question = client_text[:MAX_USER_QUESTION_LEN]
    retrieved = await rag_store.search(question, 6)
    if not retrieved:
        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            (
                "Извините, не совсем понимаю, о чем речь. "
                "Уточните, пожалуйста, что именно вы хотите: бот / сайт / автоматизация / другое?"
            ),
            reply_markup=need_keyboard(),
        )
        await db.update_lead_fields(
            business_connection_id,
            client_chat_id,
            step=STEP_NEED,
            last_client_message=question,
            rag_sources=[],
        )
        return

    answer = await generate_answer(
        config=config,
        question=question,
        retrieved_chunks=retrieved,
        lead_context={"step": lead.step},
    )
    source_urls = _unique_urls([item.source_url for item in retrieved])
    await _send_business_message(
        bot=bot,
        business_connection_id=business_connection_id,
        chat_id=client_chat_id,
        text=answer,
        reply_markup=need_keyboard(),
    )
    await db.update_lead_fields(
        business_connection_id,
        client_chat_id,
        step=STEP_NEED,
        last_client_message=question,
        rag_sources=source_urls,
    )

    extracted = await extract_lead_fields(config, question)
    await _apply_extracted_fields(db, business_connection_id, client_chat_id, extracted)
    updated = await db.get_lead(business_connection_id, client_chat_id)
    if updated:
        await _advance_or_ask_next(bot, db, config, updated)


async def _handle_lead_dialog(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
    client_text: str,
    lead: LeadInfo,
) -> None:
    await db.update_lead_fields(
        business_connection_id,
        client_chat_id,
        last_client_message=client_text,
    )

    if lead.step == STEP_NEED:
        need = _normalize_need(client_text)
        if need:
            await db.update_lead_fields(business_connection_id, client_chat_id, need=need)
    elif lead.step == STEP_BUDGET:
        budget = _normalize_budget(client_text)
        if budget:
            await db.update_lead_fields(business_connection_id, client_chat_id, budget=budget)
    elif lead.step == STEP_DEADLINE:
        deadline = _normalize_deadline(client_text)
        if deadline:
            await db.update_lead_fields(business_connection_id, client_chat_id, deadline=deadline)
    elif lead.step == STEP_CONTACT_METHOD:
        contact = _normalize_contact(client_text)
        if contact:
            await db.update_lead_fields(business_connection_id, client_chat_id, contact_method=contact)
    elif lead.step == STEP_PHONE:
        phone = _extract_phone(client_text)
        if phone:
            await db.update_lead_fields(business_connection_id, client_chat_id, phone=phone)
    elif lead.step == STEP_CALL_TIME:
        await db.update_lead_fields(business_connection_id, client_chat_id, call_time=client_text)

    extracted = await extract_lead_fields(config, client_text)
    await _apply_extracted_fields(db, business_connection_id, client_chat_id, extracted)
    updated = await db.get_lead(business_connection_id, client_chat_id)
    if not updated:
        return

    await _advance_or_ask_next(bot, db, config, updated)


async def _apply_extracted_fields(
    db: Database,
    business_connection_id: str,
    client_chat_id: int,
    extracted: dict[str, str | None],
) -> None:
    lead = await db.get_lead(business_connection_id, client_chat_id)
    if not lead:
        return

    updates: dict[str, str] = {}
    if not lead.need and extracted.get("need"):
        need = _normalize_need(extracted["need"] or "")
        if need:
            updates["need"] = need
    if not lead.budget and extracted.get("budget"):
        budget = _normalize_budget(extracted["budget"] or "")
        if budget:
            updates["budget"] = budget
    if not lead.deadline and extracted.get("timeline"):
        deadline = _normalize_deadline(extracted["timeline"] or "")
        if deadline:
            updates["deadline"] = deadline
    if not lead.contact_method and extracted.get("contact_method"):
        contact = _normalize_contact(extracted["contact_method"] or "")
        if contact:
            updates["contact_method"] = contact
    if not lead.phone and extracted.get("phone"):
        phone = _extract_phone(extracted["phone"] or "")
        if phone:
            updates["phone"] = phone

    if updates:
        await db.update_lead_fields(
            business_connection_id,
            client_chat_id,
            need=updates.get("need"),
            budget=updates.get("budget"),
            deadline=updates.get("deadline"),
            contact_method=updates.get("contact_method"),
            phone=updates.get("phone"),
        )


def _next_step(lead: LeadInfo) -> int:
    if not lead.need:
        return STEP_NEED
    if not lead.budget:
        return STEP_BUDGET
    if not lead.deadline:
        return STEP_DEADLINE
    if not lead.contact_method:
        return STEP_CONTACT_METHOD
    if lead.contact_method == "по телефону" and not lead.phone:
        return STEP_PHONE
    if lead.contact_method == "созвон" and not lead.call_time:
        return STEP_CALL_TIME
    return STEP_DONE


async def _advance_or_ask_next(
    bot: Bot,
    db: Database,
    config: Config,
    lead: LeadInfo,
) -> None:
    next_step = _next_step(lead)
    if next_step == STEP_DONE:
        await db.update_lead_fields(lead.business_connection_id, lead.client_chat_id, step=STEP_DONE)
        await _finalize_lead(bot, db, config, lead.business_connection_id, lead.client_chat_id)
        return

    await db.update_lead_fields(lead.business_connection_id, lead.client_chat_id, step=next_step)
    if next_step == STEP_NEED:
        intent = await classify_intent(config, lead.last_client_message or "")
        if intent.get("intent") in {"bot", "site", "automation", "other"}:
            await db.update_lead_fields(
                lead.business_connection_id,
                lead.client_chat_id,
                need=_intent_to_need(str(intent["intent"])),
                step=STEP_BUDGET,
            )
            await _send_business_message(
                bot,
                lead.business_connection_id,
                lead.client_chat_id,
                "Какой у вас ориентировочный бюджет?",
                reply_markup=budget_keyboard(),
            )
            return
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Что хотите сделать? (бот / сайт / автоматизация / другое)",
            reply_markup=need_keyboard(),
        )
        return
    if next_step == STEP_BUDGET:
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Какой у вас ориентировочный бюджет?",
            reply_markup=budget_keyboard(),
        )
        return
    if next_step == STEP_DEADLINE:
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Какие сроки реализации?",
            reply_markup=deadline_keyboard(),
        )
        return
    if next_step == STEP_CONTACT_METHOD:
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Как вам удобно связаться?",
            reply_markup=contact_keyboard(),
        )
        return
    if next_step == STEP_PHONE:
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Напишите, пожалуйста, номер телефона текстом.",
            reply_markup=remove_keyboard(),
        )
        return
    if next_step == STEP_CALL_TIME:
        await _send_business_message(
            bot,
            lead.business_connection_id,
            lead.client_chat_id,
            "Напишите удобное время для созвона.",
            reply_markup=remove_keyboard(),
        )


async def _finalize_lead(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
) -> None:
    lead = await db.get_lead(business_connection_id, client_chat_id)
    if lead is None:
        logger.warning("Cannot finalize lead: lead not found")
        return

    sources: list[str] = []
    if lead.rag_sources_json:
        try:
            loaded = json.loads(lead.rag_sources_json)
            if isinstance(loaded, list):
                sources = [str(x) for x in loaded if str(x).strip()]
        except json.JSONDecodeError:
            sources = []

    summary_data = {
        "business_connection_id": business_connection_id,
        "client_chat_id": client_chat_id,
        "need": lead.need,
        "budget": lead.budget,
        "timeline": lead.deadline,
        "contact_method": lead.contact_method,
        "phone": lead.phone,
        "call_time": lead.call_time,
        "last_client_message": lead.last_client_message,
        "sources": sources,
    }
    await db.update_lead_fields(
        business_connection_id,
        client_chat_id,
        summary=summary_data,
    )

    admin_chat_id = await db.resolve_admin_chat_id(business_connection_id, config.admin_chat_id)
    if admin_chat_id:
        hot = _is_hot_lead(lead)
        lines = [
            "📌 Новый лид",
            f"Клиент chat_id: {client_chat_id}",
            f"Потребность: {lead.need or '-'}",
            f"Бюджет: {lead.budget or '-'}",
            f"Срок: {lead.deadline or '-'}",
            f"Контакт: {lead.contact_method or '-'}",
            f"Телефон: {lead.phone or '-'}",
            f"Последний вопрос: {lead.last_client_message or '-'}",
        ]
        for url in sources[:5]:
            lines.append(f"Источник: {url}")
        if hot:
            lines.append("🔥 горячий лид")
        await bot.send_message(chat_id=admin_chat_id, text="\n".join(lines))
    else:
        logger.warning("Cannot send lead summary: admin chat id is unknown")

    await db.close_escalation(business_connection_id, client_chat_id)
    await _send_business_message(
        bot,
        business_connection_id,
        client_chat_id,
        "Спасибо! Я передал менеджеру, скоро свяжемся.",
        reply_markup=remove_keyboard(),
    )


def _is_hot_lead(lead: LeadInfo) -> bool:
    if lead.urgency == "high":
        return True
    budget = (lead.budget or "").lower()
    return "80" in budget or "150" in budget


def _normalize_need(value: str) -> str | None:
    low = value.lower().strip()
    if "бот" in low:
        return "бот"
    if "сайт" in low:
        return "сайт"
    if "автомат" in low:
        return "автоматизация"
    if "друго" in low:
        return "другое"
    return low if low in ALLOWED_NEED else None


def _normalize_budget(value: str) -> str | None:
    low = value.lower().replace(" ", "")
    if "30" in low and ("до" in low or "<" in low):
        return "до 30k"
    if "30" in low and "80" in low:
        return "30–80k"
    if "80" in low and "150" in low:
        return "80–150k"
    if "150" in low or "+" in low:
        return "150k+"
    return value if value in ALLOWED_BUDGET else None


def _normalize_deadline(value: str) -> str | None:
    low = value.lower()
    if "сроч" in low or "1-3" in low or "1–3" in low:
        return "срочно 1–3 дня"
    if "1-2" in low or "1–2" in low or "недел" in low:
        return "1–2 недели"
    if "месяц" in low:
        return "в течение месяца"
    if "не гор" in low:
        return "не горит"
    return value if value in ALLOWED_DEADLINE else None


def _normalize_contact(value: str) -> str | None:
    low = value.lower()
    if "telegram" in low:
        return "в Telegram"
    if "телефон" in low or "звон" in low:
        return "по телефону"
    if "созвон" in low:
        return "созвон"
    return value if value in ALLOWED_CONTACT else None


def _extract_phone(value: str) -> str | None:
    match = re.search(r"(\+?\d[\d\-\s\(\)]{8,}\d)", value)
    return match.group(1).strip() if match else None


def _intent_to_need(intent: str) -> str:
    return {
        "bot": "бот",
        "site": "сайт",
        "automation": "автоматизация",
        "other": "другое",
    }.get(intent, "другое")


def _unique_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        clean = url.strip()
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


async def _send_business_message(
    bot: Bot,
    business_connection_id: str,
    chat_id: int,
    text: str,
    reply_markup=None,
) -> None:
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        business_connection_id=business_connection_id,
        reply_markup=reply_markup,
    )
