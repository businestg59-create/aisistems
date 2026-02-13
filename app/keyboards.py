from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove

MANAGER_BUTTON = "👤 Позвать менеджера"


def need_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="бот"), KeyboardButton(text="сайт")],
            [KeyboardButton(text="автоматизация"), KeyboardButton(text="другое")],
            [KeyboardButton(text=MANAGER_BUTTON)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def budget_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="до 30k"), KeyboardButton(text="30–80k")],
            [KeyboardButton(text="80–150k"), KeyboardButton(text="150k+")],
            [KeyboardButton(text=MANAGER_BUTTON)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def deadline_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="срочно 1–3 дня")],
            [KeyboardButton(text="1–2 недели")],
            [KeyboardButton(text="в течение месяца")],
            [KeyboardButton(text="не горит")],
            [KeyboardButton(text=MANAGER_BUTTON)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="в Telegram"), KeyboardButton(text="по телефону")],
            [KeyboardButton(text="созвон")],
            [KeyboardButton(text=MANAGER_BUTTON)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def remove_keyboard() -> ReplyKeyboardRemove:
    return ReplyKeyboardRemove()
