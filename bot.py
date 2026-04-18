"""
Telegram-бот для учёта расходов.
Запуск:  BOT_TOKEN=xxx python bot.py   (или положить токен в .env)
"""

import asyncio
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

# ---------- Конфиг ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "expenses.db")

PRO_BUTTON_TEXT = "⭐ Купить Pro навсегда"
PRO_PRICE_STARS = 1  # минимально возможная цена в Telegram Stars
PRO_PAYLOAD = "pro_forever_v1"

DEFAULT_CATEGORIES = [
    ("🍔", "Еда"),
    ("☕️", "Кофе"),
    ("🛒", "Продукты"),
    ("🚕", "Такси"),
    ("🚌", "Транспорт"),
    ("🏠", "Дом"),
    ("💊", "Здоровье"),
    ("🎉", "Развлечения"),
    ("👕", "Одежда"),
    ("💼", "Прочее"),
]


# ---------- База данных ----------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def db_init() -> None:
    with closing(db_connect()) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS categories (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                emoji   TEXT NOT NULL DEFAULT '💰',
                name    TEXT NOT NULL,
                UNIQUE(user_id, name)
            );
            CREATE TABLE IF NOT EXISTS expenses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                category_id INTEGER,
                amount      REAL NOT NULL,
                comment     TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL
            );
            CREATE INDEX IF NOT EXISTS idx_exp_user_created
                ON expenses(user_id, created_at);
            """
        )
        # Миграция: колонки подписки Pro
        existing = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "is_pro" not in existing:
            conn.execute("ALTER TABLE users ADD COLUMN is_pro INTEGER NOT NULL DEFAULT 0")
        if "pro_since" not in existing:
            conn.execute("ALTER TABLE users ADD COLUMN pro_since TEXT")
        if "pro_charge_id" not in existing:
            conn.execute("ALTER TABLE users ADD COLUMN pro_charge_id TEXT")


def is_user_pro(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT is_pro FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
    return bool(row and row[0])


def mark_user_pro(user_id: int, charge_id: str) -> None:
    with closing(db_connect()) as conn, conn:
        conn.execute(
            "UPDATE users SET is_pro = 1, "
            "pro_since = datetime('now'), pro_charge_id = ? "
            "WHERE user_id = ?",
            (charge_id, user_id),
        )


def ensure_user(user_id: int, username: Optional[str]) -> None:
    with closing(db_connect()) as conn, conn:
        existed = conn.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not existed:
            conn.execute(
                "INSERT INTO users(user_id, username) VALUES (?, ?)",
                (user_id, username or ""),
            )
            conn.executemany(
                "INSERT INTO categories(user_id, emoji, name) VALUES (?, ?, ?)",
                [(user_id, e, n) for e, n in DEFAULT_CATEGORIES],
            )
        elif username:
            conn.execute(
                "UPDATE users SET username = ? WHERE user_id = ?",
                (username, user_id),
            )


def list_categories(user_id: int):
    with closing(db_connect()) as conn:
        return conn.execute(
            "SELECT id, emoji, name FROM categories WHERE user_id = ? ORDER BY id",
            (user_id,),
        ).fetchall()


def add_category(user_id: int, emoji: str, name: str) -> bool:
    try:
        with closing(db_connect()) as conn, conn:
            conn.execute(
                "INSERT INTO categories(user_id, emoji, name) VALUES (?, ?, ?)",
                (user_id, emoji or "💰", name),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def delete_category(cat_id: int, user_id: int) -> None:
    with closing(db_connect()) as conn, conn:
        conn.execute(
            "DELETE FROM categories WHERE id = ? AND user_id = ?",
            (cat_id, user_id),
        )


def save_expense(user_id: int, category_id: Optional[int],
                 amount: float, comment: str) -> int:
    with closing(db_connect()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO expenses(user_id, category_id, amount, comment) "
            "VALUES (?, ?, ?, ?)",
            (user_id, category_id, amount, comment),
        )
        return cur.lastrowid


def set_expense_category(exp_id: int, user_id: int, cat_id: int):
    with closing(db_connect()) as conn, conn:
        conn.execute(
            "UPDATE expenses SET category_id = ? WHERE id = ? AND user_id = ?",
            (cat_id, exp_id, user_id),
        )
        return conn.execute(
            "SELECT e.amount, e.comment, c.emoji, c.name "
            "FROM expenses e JOIN categories c ON c.id = e.category_id "
            "WHERE e.id = ?",
            (exp_id,),
        ).fetchone()


def delete_expense(exp_id: int, user_id: int) -> None:
    with closing(db_connect()) as conn, conn:
        conn.execute(
            "DELETE FROM expenses WHERE id = ? AND user_id = ?",
            (exp_id, user_id),
        )


def get_stats(user_id: int, since: Optional[datetime] = None):
    q = (
        "SELECT c.emoji, c.name, SUM(e.amount) AS total, COUNT(*) AS cnt "
        "FROM expenses e "
        "LEFT JOIN categories c ON c.id = e.category_id "
        "WHERE e.user_id = ?"
    )
    args = [user_id]
    if since:
        q += " AND e.created_at >= ?"
        args.append(since.strftime("%Y-%m-%d %H:%M:%S"))
    q += " GROUP BY c.id ORDER BY total DESC"
    with closing(db_connect()) as conn:
        return conn.execute(q, args).fetchall()


# ---------- Парсер сообщения ----------
AMOUNT_RE = re.compile(r"^(.*?)\s+(-?\d+(?:[.,]\d+)?)\s*$", re.DOTALL)


def parse_expense(text: str):
    """Возвращает (comment, amount) или None."""
    text = text.strip()
    m = AMOUNT_RE.match(text)
    if m:
        return m.group(1).strip(), float(m.group(2).replace(",", "."))
    try:
        return "", float(text.replace(",", "."))
    except ValueError:
        return None


# ---------- FSM ----------
class Form(StatesGroup):
    new_category = State()


# ---------- Клавиатуры ----------
def categories_kb(user_id: int, expense_id: int) -> InlineKeyboardMarkup:
    cats = list_categories(user_id)
    buttons, row = [], []
    for cid, emoji, name in cats:
        row.append(
            InlineKeyboardButton(
                text=f"{emoji} {name}",
                callback_data=f"cat:{expense_id}:{cid}",
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append(
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel:{expense_id}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def stats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сегодня", callback_data="stats:today"),
                InlineKeyboardButton(text="Неделя", callback_data="stats:week"),
            ],
            [
                InlineKeyboardButton(text="Месяц", callback_data="stats:month"),
                InlineKeyboardButton(text="Всё время", callback_data="stats:all"),
            ],
        ]
    )


def pro_inline_kb(user_id: int):
    """Inline-кнопка Pro под сообщением. None — если юзер уже Pro."""
    if is_user_pro(user_id):
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=PRO_BUTTON_TEXT, callback_data="buy_pro")
        ]]
    )


def categories_manage_kb(user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for cid, emoji, name in list_categories(user_id):
        buttons.append(
            [
                InlineKeyboardButton(text=f"{emoji} {name}", callback_data="noop"),
                InlineKeyboardButton(text="🗑", callback_data=f"delcat:{cid}"),
            ]
        )
    buttons.append(
        [InlineKeyboardButton(text="➕ Добавить категорию", callback_data="addcat")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ---------- Хендлеры ----------
router = Router()


@router.message(CommandStart())
async def on_start(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "Привет! Я бот для учёта расходов.\n\n"
        "Просто напиши: <b>кофе 300</b> — и я спрошу категорию.\n\n"
        "Команды:\n"
        "/stats — статистика\n"
        "/categories — управление категориями\n"
        "/help — помощь",
        reply_markup=pro_inline_kb(msg.from_user.id),
    )


@router.message(Command("help"))
async def on_help(msg: Message):
    await msg.answer(
        "Формат записи расхода:\n"
        "<code>комментарий сумма</code>\n\n"
        "Примеры:\n"
        "• <code>кофе 300</code>\n"
        "• <code>такси домой 1500</code>\n"
        "• <code>450</code> (без комментария)\n\n"
        "Команды:\n"
        "/stats — статистика расходов\n"
        "/categories — категории\n"
        "/cancel — отменить текущее действие"
    )


@router.message(Command("cancel"))
async def on_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Отменено.")


# ----- stats -----
@router.message(Command("stats"))
async def on_stats(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer("📊 Выбери период:", reply_markup=stats_kb())


@router.callback_query(F.data.startswith("stats:"))
async def cb_stats(cq: CallbackQuery):
    period = cq.data.split(":", 1)[1]
    now = datetime.now()
    if period == "today":
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
        title = "за сегодня"
    elif period == "week":
        since = now - timedelta(days=7)
        title = "за 7 дней"
    elif period == "month":
        since = now - timedelta(days=30)
        title = "за 30 дней"
    else:
        since, title = None, "за всё время"

    rows = get_stats(cq.from_user.id, since)
    if not rows:
        text = f"📊 Расходов {title} нет."
    else:
        total = sum((r[2] or 0) for r in rows)
        lines = [f"📊 <b>Статистика {title}</b>", ""]
        for emoji, name, tot, cnt in rows:
            label = f"{emoji or '❔'} {name or 'Без категории'}"
            lines.append(f"{label}: <b>{tot:g}</b> ({cnt})")
        lines += ["", f"Итого: <b>{total:g}</b>"]
        text = "\n".join(lines)
    try:
        await cq.message.edit_text(text, reply_markup=stats_kb())
    except Exception:
        await cq.message.answer(text, reply_markup=stats_kb())
    await cq.answer()


# ----- categories -----
@router.message(Command("categories"))
async def on_categories(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "🗂 Твои категории:",
        reply_markup=categories_manage_kb(msg.from_user.id),
    )


@router.callback_query(F.data == "addcat")
async def cb_add_cat(cq: CallbackQuery, state: FSMContext):
    await state.set_state(Form.new_category)
    await cq.message.answer(
        "Пришли новую категорию в формате: <code>🎮 Игры</code>\n"
        "Эмодзи необязателен — можно просто название.\n\n"
        "Или /cancel — чтобы отменить."
    )
    await cq.answer()


@router.callback_query(F.data.startswith("delcat:"))
async def cb_delete_category(cq: CallbackQuery):
    cat_id = int(cq.data.split(":", 1)[1])
    delete_category(cat_id, cq.from_user.id)
    try:
        await cq.message.edit_text(
            "🗂 Твои категории:",
            reply_markup=categories_manage_kb(cq.from_user.id),
        )
    except Exception:
        pass
    await cq.answer("Удалено")


@router.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery):
    await cq.answer()


@router.message(Form.new_category)
async def on_new_category(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    if not text:
        await msg.answer("Название не может быть пустым.")
        return
    parts = text.split(maxsplit=1)
    if len(parts) == 2 and not parts[0][0].isalnum():
        emoji, name = parts[0], parts[1]
    else:
        emoji, name = "💰", text
    if add_category(msg.from_user.id, emoji, name):
        await msg.answer(
            f"✅ Категория добавлена: {emoji} {name}",
            reply_markup=categories_manage_kb(msg.from_user.id),
        )
    else:
        await msg.answer("⚠️ Такая категория уже существует.")
    await state.clear()


# ----- расходы -----
@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel_expense(cq: CallbackQuery):
    exp_id = int(cq.data.split(":", 1)[1])
    delete_expense(exp_id, cq.from_user.id)
    await cq.message.edit_text("❌ Отменено.")
    await cq.answer()


@router.callback_query(F.data.startswith("cat:"))
async def cb_pick_category(cq: CallbackQuery):
    _, exp_id, cat_id = cq.data.split(":")
    row = set_expense_category(int(exp_id), cq.from_user.id, int(cat_id))
    if row:
        amount, comment, emoji, name = row
        text = f"✅ Записано: {emoji} {name} — <b>{amount:g}</b>"
        if comment:
            text += f"\n💬 {comment}"
    else:
        text = "✅ Записано."
    await cq.message.edit_text(text)
    await cq.answer("Сохранено")


# ----- Pro-подписка (Telegram Stars) -----
@router.callback_query(F.data == "buy_pro")
async def cb_buy_pro(cq: CallbackQuery):
    ensure_user(cq.from_user.id, cq.from_user.username)
    if is_user_pro(cq.from_user.id):
        await cq.answer("У тебя уже есть Pro ✨", show_alert=True)
        return
    await cq.bot.send_invoice(
        chat_id=cq.message.chat.id,
        title="Pro-подписка навсегда",
        description=(
            "Разовая покупка. Поддержка разработки и доступ ко всем "
            "будущим Pro-функциям бота — навсегда."
        ),
        payload=PRO_PAYLOAD,
        provider_token="",  # пусто для Telegram Stars
        currency="XTR",
        prices=[LabeledPrice(label="Pro навсегда", amount=PRO_PRICE_STARS)],
    )
    await cq.answer()


@router.pre_checkout_query()
async def on_pre_checkout(query: PreCheckoutQuery):
    # Подтверждаем все платежи. Проверок тут не надо — цена фиксированная.
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def on_successful_payment(msg: Message):
    payment = msg.successful_payment
    charge_id = payment.telegram_payment_charge_id or payment.provider_payment_charge_id or ""
    mark_user_pro(msg.from_user.id, charge_id)
    await msg.answer(
        "🎉 <b>Оплата прошла!</b>\n\n"
        "Теперь у тебя Pro-подписка навсегда. "
        "Новые Pro-функции будут добавляться — ты получишь их автоматически.",
        reply_markup=ReplyKeyboardRemove(),  # на случай старой reply-клавиатуры
    )


@router.message(F.text)
async def on_text(msg: Message, state: FSMContext):
    if await state.get_state():
        return  # в FSM — другие хендлеры разберутся
    ensure_user(msg.from_user.id, msg.from_user.username)
    parsed = parse_expense(msg.text)
    if not parsed:
        await msg.answer(
            "Не понял 🙈 Напиши в формате <code>кофе 300</code> или /help"
        )
        return
    comment, amount = parsed
    if amount <= 0:
        await msg.answer("Сумма должна быть больше нуля.")
        return
    exp_id = save_expense(msg.from_user.id, None, amount, comment)
    preview = f"💸 <b>{amount:g}</b>"
    if comment:
        preview += f" — {comment}"
    preview += "\n\nВыбери категорию:"
    await msg.answer(preview, reply_markup=categories_kb(msg.from_user.id, exp_id))


# ---------- Запуск ----------
async def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN не задан. Положи его в .env или запусти как "
            "BOT_TOKEN=xxx python bot.py"
        )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db_init()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
