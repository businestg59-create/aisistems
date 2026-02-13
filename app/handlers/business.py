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


def _is_greeting_only(text: str) -> bool:
    low = (text or "").strip().lower()
    if not low or low == TEXT_NON_MESSAGE:
        return True
    greetings = ("привет", "здравствуйте", "добрый", "hi", "hello", "hey")
    # если просто приветствие без сути
    return any(low == g or low.startswith(g + " ") for g in greetings) and len(low) <= 20


def _admin_contact_link(username: str | None, chat_id: int) -> str:
    # если есть username — лучший вариант
    if username:
        return f"https://t.me/{username}"
    # иначе пробуем deep link (часто кликается в Telegram Desktop/Mobile)
    return f"tg://user?id={chat_id}"


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
                owner_user_id=event.user.id if event.user else None,
                owner_user_chat_id=event.user_chat_id,
                can_reply=bool(event.can_reply),
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
                    username=username,
                    full_name=full_name,
                    text=question,
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
                    reason="Запрос на человека (кнопка)",
                    urgency="high",
                    need_human=True,
                    negative=False,
                )
                return

            rule_risk = _rule_based_risk(question)
            if rule_risk is None and config.openai_api_key:
                try:
                    rule_risk = await classify_risk(config=config, user_text=question)
                except Exception:
                    logger.exception("Risk classification failed, fallback to rule-based only")

            if rule_risk:
                if _should_critical_escalate(rule_risk):
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
                        reason=str(rule_risk.get("reason") or "Эскалация по сообщению клиента"),
                        urgency=str(rule_risk.get("urgency") or "high"),
                        need_human=bool(rule_risk.get("need_human")),
                        negative=bool(rule_risk.get("negative")),
                    )
                    return

            if lead.step > STEP_WELCOME:
                await _handle_lead_flow(
                    bot=bot,
                    db=db,
                    config=config,
                    rag_store=rag_store,
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
    link = _admin_contact_link(username, client_chat_id)
    alert_text = (
        "🚨 КРИТИЧНО: клиент просит человека/негатив\n"
        f"Клиент: {full_name or 'без имени'} ({username_text})\n"
        f"chat_id: {client_chat_id}\n"
        f"Ссылка: {link}\n"
        f"business_connection_id: {business_connection_id}\n"
        f"need_human={need_human}, negative={negative}, urgency={urgency}\n"
        f"Причина: {reason or '-'}\n"
        f"Текст: {text[:1200]}\n"
        f"Статус лида: {lead_state}"
    )
    await bot.send_message(chat_id=admin_chat_id, text=alert_text)


async def _handle_lead_flow(
    bot: Bot,
    db: Database,
    config: Config,
    rag_store: RAGStore,
    business_connection_id: str,
    client_chat_id: int,
    client_text: str,
    lead: LeadInfo,
) -> None:
    text = (client_text or "").strip()
    step = lead.step

    if step == STEP_NEED:
        value = _normalize_need(text)
        if value not in ALLOWED_NEED:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Подскажите, что вас интересует: бот / сайт / автоматизация / другое?",
                reply_markup=need_keyboard(),
            )
            return
        await db.update_lead_fields(business_connection_id, client_chat_id, need=value, step=STEP_BUDGET)
        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            "Отлично. Скажите, пожалуйста, какой бюджет комфортен?",
            reply_markup=budget_keyboard(),
        )
        return

    if step == STEP_BUDGET:
        value = _normalize_budget(text)
        if value not in ALLOWED_BUDGET:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Выберите бюджет из вариантов ниже 🙂",
                reply_markup=budget_keyboard(),
            )
            return
        await db.update_lead_fields(business_connection_id, client_chat_id, budget=value, step=STEP_DEADLINE)
        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            "Понял. По срокам как удобно?",
            reply_markup=deadline_keyboard(),
        )
        return

    if step == STEP_DEADLINE:
        value = _normalize_deadline(text)
        if value not in ALLOWED_DEADLINE:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Выберите срок из вариантов ниже 🙂",
                reply_markup=deadline_keyboard(),
            )
            return
        await db.update_lead_fields(business_connection_id, client_chat_id, deadline=value, step=STEP_CONTACT_METHOD)
        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            "Как удобнее связаться для уточнения деталей?",
            reply_markup=contact_keyboard(),
        )
        return

    if step == STEP_CONTACT_METHOD:
        value = _normalize_contact(text)
        if value not in ALLOWED_CONTACT:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Выберите вариант связи 🙂",
                reply_markup=contact_keyboard(),
            )
            return

        await db.update_lead_fields(business_connection_id, client_chat_id, contact_method=value)
        if value == "по телефону":
            await db.update_lead_fields(business_connection_id, client_chat_id, step=STEP_PHONE)
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Ок. Напишите, пожалуйста, номер телефона (в любом формате).",
                reply_markup=remove_keyboard(),
            )
            return

        if value == "созвон":
            await db.update_lead_fields(business_connection_id, client_chat_id, step=STEP_CALL_TIME)
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Отлично. Напишите, пожалуйста, удобное время для созвона (например: сегодня после 18:00).",
                reply_markup=remove_keyboard(),
            )
            return

        await db.update_lead_fields(business_connection_id, client_chat_id, step=STEP_DONE)
        await _finalize_lead(bot, db, config, business_connection_id, client_chat_id)
        return

    if step == STEP_PHONE:
        phone = _extract_phone(text)
        if not phone:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Не вижу номер. Пришлите, пожалуйста, телефон ещё раз 🙂",
                reply_markup=remove_keyboard(),
            )
            return
        await db.update_lead_fields(business_connection_id, client_chat_id, phone=phone, step=STEP_DONE)
        await _finalize_lead(bot, db, config, business_connection_id, client_chat_id)
        return

    if step == STEP_CALL_TIME:
        call_time = text[:200] if text else None
        if not call_time:
            await _send_business_message(
                bot,
                business_connection_id,
                client_chat_id,
                "Подскажите, пожалуйста, удобное время для созвона 🙂",
                reply_markup=remove_keyboard(),
            )
            return
        await db.update_lead_fields(business_connection_id, client_chat_id, call_time=call_time, step=STEP_DONE)
        await _finalize_lead(bot, db, config, business_connection_id, client_chat_id)
        return

    await _handle_rag_entry(
        bot=bot,
        db=db,
        config=config,
        rag_store=rag_store,
        business_connection_id=business_connection_id,
        client_chat_id=client_chat_id,
        client_text=client_text,
        lead=lead,
    )


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
    is_first_touch = (lead.step == STEP_WELCOME)

    # Если это первое касание и клиент написал просто "привет" — не тратим RAG, а просим сформулировать вопрос
    if is_first_touch and _is_greeting_only(question):
        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            (
                "Привет! 👋 Я AI-консультант AI-Системы.\n"
                "Подскажите, пожалуйста, какой у вас вопрос? Можно в 1–2 предложениях 🙂"
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

    retrieved = await rag_store.search(question, 6)
    if not retrieved:
        hello = ""
        if is_first_touch:
            hello = "Привет! 👋 Я AI-консультант AI-Системы.\n\n"

        await _send_business_message(
            bot,
            business_connection_id,
            client_chat_id,
            (
                hello
                + "Извините, не совсем понимаю, о чем речь. "
                + "Уточните, пожалуйста, что именно вы хотите: бот / сайт / автоматизация / другое?"
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

    if is_first_touch:
        answer = (
            "Привет! 👋 Я AI-консультант AI-Системы.\n"
            "Спасибо за сообщение — сейчас подскажу.\n\n"
            + answer
            + "\n\nЧтобы точнее сориентировать по срокам и бюджету: что вам нужно — бот / сайт / автоматизация / другое?"
        )

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
        reply_markup=reply_markup,
        business_connection_id=business_connection_id,
    )


async def _ensure_connection_info(bot: Bot, db: Database, business_connection_id: str) -> None:
    existing = await db.get_connection(business_connection_id)
    if existing and existing.owner_user_id:
        return

    try:
        info = await bot.get_business_connection(business_connection_id)
        await db.upsert_connection(
            business_connection_id=info.id,
            owner_user_id=info.user.id if info.user else None,
            owner_user_chat_id=info.user_chat_id,
            can_reply=bool(info.can_reply),
        )
        if info.user_chat_id:
            await db.set_admin_chat_id(info.user_chat_id)
    except Exception:
        logger.exception("Failed to fetch business_connection info bcid=%s", business_connection_id)


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

    username_text = f"@{username}" if username else "нет username"
    link = _admin_contact_link(username, client_chat_id)

    notify_text = (
        "🆕 НОВЫЙ КЛИЕНТ\n"
        f"Клиент: {full_name or 'без имени'} ({username_text})\n"
        f"chat_id: {client_chat_id}\n"
        f"Сообщение: {text[:1200]}\n"
        f"Ссылка: {link}\n"
        f"bcid: {business_connection_id}"
    )
    await bot.send_message(chat_id=admin_chat_id, text=notify_text)


async def _notify_cannot_reply(
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
        logger.warning("Cannot send can_reply warning: admin chat id is unknown")
        return

    username_text = f"@{username}" if username else "нет username"
    link = _admin_contact_link(username, client_chat_id)

    msg = (
        "⚠️ НЕТ ПРАВА ОТВЕЧАТЬ через Business API (can_reply=false)\n"
        f"Клиент: {full_name or 'без имени'} ({username_text})\n"
        f"chat_id: {client_chat_id}\n"
        f"bcid: {business_connection_id}\n"
        f"Сообщение: {text[:1200]}\n"
        f"Ссылка: {link}\n\n"
        "👉 Проверь в Telegram Business права бота (Reply/Manage messages)."
    )
    await bot.send_message(chat_id=admin_chat_id, text=msg)


def _unique_urls(urls: list[str | None]) -> list[str]:
    out: list[str] = []
    seen = set()
    for u in urls:
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _normalize_need(text: str) -> str:
    t = (text or "").strip().lower()
    if "бот" in t:
        return "бот"
    if "сайт" in t:
        return "сайт"
    if "авто" in t:
        return "автоматизация"
    return "другое"


def _normalize_budget(text: str) -> str:
    t = (text or "").strip().lower()
    if "до" in t or "30" in t and "80" not in t:
        return "до 30k"
    if "30" in t and "80" in t:
        return "30–80k"
    if "80" in t and "150" in t:
        return "80–150k"
    if "150" in t or "+" in t:
        return "150k+"
    return t


def _normalize_deadline(text: str) -> str:
    t = (text or "").strip().lower()
    if "1–3" in t or "срочно" in t or "дня" in t:
        return "срочно 1–3 дня"
    if "1–2" in t or "нед" in t:
        return "1–2 недели"
    if "месяц" in t:
        return "в течение месяца"
    if "не гор" in t:
        return "не горит"
    return t


def _normalize_contact(text: str) -> str:
    t = (text or "").strip().lower()
    if "тел" in t:
        return "по телефону"
    if "соз" in t:
        return "созвон"
    return "в Telegram"


def _extract_phone(text: str) -> str | None:
    if not text:
        return None
    m = re.search(r"(\+?\d[\d\s\-\(\)]{7,}\d)", text)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()


def _lead_state_text(lead: LeadInfo | None) -> str:
    if not lead:
        return "-"
    return json.dumps(
        {
            "step": lead.step,
            "need": lead.need,
            "budget": lead.budget,
            "deadline": lead.deadline,
            "contact_method": lead.contact_method,
            "phone": lead.phone,
            "call_time": lead.call_time,
        },
        ensure_ascii=False,
    )


async def _finalize_lead(
    bot: Bot,
    db: Database,
    config: Config,
    business_connection_id: str,
    client_chat_id: int,
) -> None:
    lead = await db.get_lead(business_connection_id, client_chat_id)
    await _send_business_message(
        bot,
        business_connection_id,
        client_chat_id,
        "Спасибо! ✅ Я передал данные менеджеру. Он свяжется с вами для уточнения деталей.",
        reply_markup=remove_keyboard(),
    )

    admin_chat_id = await db.resolve_admin_chat_id(business_connection_id, config.admin_chat_id)
    if admin_chat_id and lead:
        await bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "✅ ЛИД СОБРАН\n"
                f"chat_id={client_chat_id}\n"
                f"Данные: {_lead_state_text(lead)}"
            ),
        )
