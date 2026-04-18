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
from aiogram.filters import Command, CommandObject, CommandStart
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

# Админы: список Telegram user_id через запятую в переменной ADMIN_IDS.
# По умолчанию — владелец бота.
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "185202211").replace(" ", "").split(",") if x
}


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

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

            CREATE TABLE IF NOT EXISTS auto_cat (
                user_id     INTEGER NOT NULL,
                keyword     TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                hits        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, keyword, category_id)
            );

            CREATE TABLE IF NOT EXISTS limits (
                user_id       INTEGER NOT NULL,
                category_id   INTEGER NOT NULL,
                monthly_limit REAL NOT NULL,
                PRIMARY KEY (user_id, category_id)
            );

            CREATE TABLE IF NOT EXISTS recurring (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       INTEGER NOT NULL,
                name          TEXT NOT NULL,
                amount        REAL NOT NULL,
                category_id   INTEGER NOT NULL,
                day_of_month  INTEGER NOT NULL,
                last_run_date TEXT
            );
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


# ---------- Pro: автокатегоризация ----------
def _keyword(comment: str) -> str:
    """Первое слово комментария, lowercased, без пунктуации."""
    import string
    s = (comment or "").strip().lower()
    if not s:
        return ""
    first = s.split()[0]
    return first.strip(string.punctuation + "«»—–")


AUTO_THRESHOLD = 3  # сколько раз надо выбрать категорию для слова, чтобы включилось авто


def auto_category_for(user_id: int, comment: str):
    """Возвращает category_id если по комменту уже известна авто-категория, иначе None."""
    kw = _keyword(comment)
    if not kw:
        return None
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT category_id, hits FROM auto_cat "
            "WHERE user_id = ? AND keyword = ? "
            "ORDER BY hits DESC LIMIT 1",
            (user_id, kw),
        ).fetchone()
    if row and row[1] >= AUTO_THRESHOLD:
        return row[0]
    return None


def bump_auto_counter(user_id: int, comment: str, category_id: int) -> None:
    kw = _keyword(comment)
    if not kw:
        return
    with closing(db_connect()) as conn, conn:
        conn.execute(
            "INSERT INTO auto_cat(user_id, keyword, category_id, hits) VALUES (?, ?, ?, 1) "
            "ON CONFLICT(user_id, keyword, category_id) DO UPDATE SET hits = hits + 1",
            (user_id, kw, category_id),
        )


# ---------- Pro: лимиты ----------
def set_limit(user_id: int, category_id: int, amount: float) -> None:
    with closing(db_connect()) as conn, conn:
        if amount <= 0:
            conn.execute(
                "DELETE FROM limits WHERE user_id = ? AND category_id = ?",
                (user_id, category_id),
            )
        else:
            conn.execute(
                "INSERT INTO limits(user_id, category_id, monthly_limit) VALUES (?, ?, ?) "
                "ON CONFLICT(user_id, category_id) DO UPDATE SET monthly_limit = excluded.monthly_limit",
                (user_id, category_id, amount),
            )


def list_limits(user_id: int):
    """Возвращает [(category_id, emoji, name, limit, spent)]"""
    start_of_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    with closing(db_connect()) as conn:
        rows = conn.execute(
            "SELECT c.id, c.emoji, c.name, l.monthly_limit, "
            "  COALESCE(("
            "    SELECT SUM(e.amount) FROM expenses e "
            "    WHERE e.user_id = l.user_id AND e.category_id = l.category_id "
            "      AND e.created_at >= ?"
            "  ), 0) AS spent "
            "FROM limits l JOIN categories c ON c.id = l.category_id "
            "WHERE l.user_id = ? ORDER BY c.id",
            (start_of_month.strftime("%Y-%m-%d %H:%M:%S"), user_id),
        ).fetchall()
    return rows


def month_spent(user_id: int, category_id: int) -> float:
    start_of_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM expenses "
            "WHERE user_id = ? AND category_id = ? AND created_at >= ?",
            (user_id, category_id, start_of_month.strftime("%Y-%m-%d %H:%M:%S")),
        ).fetchone()
    return row[0] or 0.0


def get_limit(user_id: int, category_id: int):
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT monthly_limit FROM limits WHERE user_id = ? AND category_id = ?",
            (user_id, category_id),
        ).fetchone()
    return row[0] if row else None


def limit_warning(user_id: int, category_id: int) -> Optional[str]:
    """Текст предупреждения после добавления траты, или None."""
    lim = get_limit(user_id, category_id)
    if not lim:
        return None
    spent = month_spent(user_id, category_id)
    pct = spent / lim * 100
    if spent >= lim:
        over = spent - lim
        return f"🚨 Лимит превышен на <b>{over:g}</b> ({pct:.0f}% от {lim:g})"
    if pct >= 80:
        left = lim - spent
        return f"⚠️ Потрачено {pct:.0f}% лимита. Осталось <b>{left:g}</b>"
    return None


# ---------- Pro: регулярные платежи ----------
def add_recurring(user_id: int, name: str, amount: float,
                  category_id: int, day: int) -> int:
    with closing(db_connect()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO recurring(user_id, name, amount, category_id, day_of_month) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, name, amount, category_id, day),
        )
        return cur.lastrowid


def list_recurring(user_id: int):
    with closing(db_connect()) as conn:
        return conn.execute(
            "SELECT r.id, r.name, r.amount, c.emoji, c.name, r.day_of_month "
            "FROM recurring r JOIN categories c ON c.id = r.category_id "
            "WHERE r.user_id = ? ORDER BY r.day_of_month, r.id",
            (user_id,),
        ).fetchall()


def delete_recurring(rec_id: int, user_id: int) -> bool:
    with closing(db_connect()) as conn, conn:
        cur = conn.execute(
            "DELETE FROM recurring WHERE id = ? AND user_id = ?",
            (rec_id, user_id),
        )
        return cur.rowcount > 0


def find_category_by_name(user_id: int, name: str):
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT id, emoji, name FROM categories "
            "WHERE user_id = ? AND lower(name) = lower(?)",
            (user_id, name),
        ).fetchone()
    return row


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


async def require_pro(msg: Message) -> bool:
    """Возвращает True если юзер Pro. Иначе шлёт offer и возвращает False."""
    if is_user_pro(msg.from_user.id):
        return True
    await msg.answer(
        "🔒 Эта фича доступна только с <b>Pro</b>-подпиской.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=PRO_BUTTON_TEXT, callback_data="buy_pro")
        ]]),
    )
    return False


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
    base = (
        "Привет! Я бот для учёта расходов.\n\n"
        "Просто напиши: <b>кофе 300</b> — и я спрошу категорию.\n\n"
        "<b>Команды:</b>\n"
        "/stats — статистика\n"
        "/categories — управление категориями\n"
        "/help — помощь"
    )
    if is_user_pro(msg.from_user.id):
        base += (
            "\n\n<b>⭐ Pro:</b>\n"
            "/limits — лимиты по категориям\n"
            "/recs — регулярные платежи\n"
            "/export — выгрузить CSV\n"
            "🧠 Автокатегоризация работает сама после 3 одинаковых выборов"
        )
    await msg.answer(base, reply_markup=pro_inline_kb(msg.from_user.id))


@router.message(Command("help"))
async def on_help(msg: Message):
    is_pro = is_user_pro(msg.from_user.id)
    base = (
        "Формат записи расхода:\n"
        "<code>комментарий сумма</code>\n\n"
        "Примеры:\n"
        "• <code>кофе 300</code>\n"
        "• <code>такси домой 1500</code>\n"
        "• <code>450</code> (без комментария)\n\n"
        "<b>Основное:</b>\n"
        "/stats — статистика расходов\n"
        "/categories — категории\n"
        "/cancel — отменить текущее действие"
    )
    pro = (
        "\n\n<b>⭐ Pro:</b>\n"
        "🧠 <b>Автокатегоризация</b> — после 3 одинаковых выборов слово запоминается\n"
        "💰 Лимиты:\n"
        "• <code>/limit Еда 15000</code> — поставить лимит\n"
        "• <code>/limit Еда 0</code> — удалить\n"
        "• /limits — все лимиты с прогрессом\n"
        "🔁 Регулярные:\n"
        "• <code>/rec аренда 45000 Жильё 1</code> — каждое 1-е число\n"
        "• /recs — список\n"
        "• <code>/delrec_3</code> — удалить\n"
        "📄 /export — скачать CSV со всеми расходами"
    )
    if is_pro:
        await msg.answer(base + pro)
    else:
        await msg.answer(
            base + "\n\n🔒 Ещё больше возможностей с Pro: автокатегоризация, "
            "лимиты, регулярные платежи, экспорт в CSV.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=PRO_BUTTON_TEXT, callback_data="buy_pro")
            ]]),
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
    exp_id, cat_id = int(exp_id), int(cat_id)
    row = set_expense_category(exp_id, cq.from_user.id, cat_id)
    if row:
        amount, comment, emoji, name = row
        # Pro: запоминаем выбор для автокатегоризации
        if is_user_pro(cq.from_user.id) and comment:
            bump_auto_counter(cq.from_user.id, comment, cat_id)
        text = f"✅ Записано: {emoji} {name} — <b>{amount:g}</b>"
        if comment:
            text += f"\n💬 {comment}"
        # Pro: предупреждение о лимите
        if is_user_pro(cq.from_user.id):
            warn = limit_warning(cq.from_user.id, cat_id)
            if warn:
                text += f"\n\n{warn}"
    else:
        text = "✅ Записано."
    await cq.message.edit_text(text)
    await cq.answer("Сохранено")


@router.callback_query(F.data.startswith("recat:"))
async def cb_recategorize(cq: CallbackQuery):
    """Изменить категорию уже записанной авто-траты."""
    exp_id = int(cq.data.split(":", 1)[1])
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT amount, comment FROM expenses WHERE id = ? AND user_id = ?",
            (exp_id, cq.from_user.id),
        ).fetchone()
    if not row:
        await cq.answer("Не найдено", show_alert=True)
        return
    amount, comment = row
    preview = f"💸 <b>{amount:g}</b>"
    if comment:
        preview += f" — {comment}"
    preview += "\n\nВыбери категорию:"
    await cq.message.edit_text(
        preview, reply_markup=categories_kb(cq.from_user.id, exp_id)
    )
    await cq.answer()


# ----- Админка -----
def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm:users")],
    ])


def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:main")]
    ])


def admin_users_kb() -> InlineKeyboardMarkup:
    with closing(db_connect()) as conn:
        rows = conn.execute(
            "SELECT user_id, username, is_pro FROM users ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    buttons = []
    for uid, uname, is_pro in rows:
        label = f"{'⭐' if is_pro else '·'} @{uname or uid} ({uid})"
        buttons.append([
            InlineKeyboardButton(text=label, callback_data="noop"),
            InlineKeyboardButton(
                text="🔽 Free" if is_pro else "🔼 Pro",
                callback_data=f"adm:toggle:{uid}",
            ),
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _admin_stats_text() -> str:
    with closing(db_connect()) as conn:
        users_total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        pro_total = conn.execute("SELECT COUNT(*) FROM users WHERE is_pro = 1").fetchone()[0]
        exp_cnt, exp_sum = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM expenses"
        ).fetchone()
    stars_earned = pro_total * PRO_PRICE_STARS
    return (
        "📊 <b>Глобальная статистика</b>\n\n"
        f"👥 Пользователей: <b>{users_total}</b>\n"
        f"⭐ Pro-подписок: <b>{pro_total}</b>\n"
        f"💫 Звёзд заработано: <b>{stars_earned}</b>\n\n"
        f"💸 Всего расходов записано: <b>{exp_cnt}</b>\n"
        f"💰 Суммарный объём: <b>{exp_sum:g}</b>"
    )


@router.message(Command("admin"))
async def on_admin(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_main_kb())


@router.callback_query(F.data.startswith("adm:"))
async def cb_admin(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    parts = cq.data.split(":")
    action = parts[1]

    if action == "main":
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_main_kb())
    elif action == "stats":
        await cq.message.edit_text(_admin_stats_text(), reply_markup=admin_back_kb())
    elif action == "users":
        await cq.message.edit_text(
            "👥 <b>Пользователи</b> (последние 50)\nКнопка справа — переключить Pro/Free.",
            reply_markup=admin_users_kb(),
        )
    elif action == "toggle" and len(parts) == 3:
        uid = int(parts[2])
        with closing(db_connect()) as conn, conn:
            cur = conn.execute("SELECT is_pro FROM users WHERE user_id = ?", (uid,)).fetchone()
            if cur:
                new_val = 0 if cur[0] else 1
                if new_val:
                    conn.execute(
                        "UPDATE users SET is_pro = 1, pro_since = datetime('now'), "
                        "pro_charge_id = 'admin_grant' WHERE user_id = ?",
                        (uid,),
                    )
                else:
                    conn.execute(
                        "UPDATE users SET is_pro = 0, pro_since = NULL, "
                        "pro_charge_id = NULL WHERE user_id = ?",
                        (uid,),
                    )
        await cq.message.edit_reply_markup(reply_markup=admin_users_kb())
        await cq.answer("Обновлено")
        return
    await cq.answer()


# ----- Pro: лимиты -----
@router.message(Command("limit"))
async def on_limit(msg: Message, command: CommandObject):
    if not await require_pro(msg):
        return
    args = (command.args or "").strip()
    # Последний токен — число, всё до него — название категории
    m = re.match(r"^(.*?)\s+(-?\d+(?:[.,]\d+)?)\s*$", args)
    if not m:
        await msg.answer(
            "Использование:\n"
            "<code>/limit Еда 15000</code> — лимит 15000/мес на «Еда»\n"
            "<code>/limit Еда 0</code> — удалить лимит"
        )
        return
    name = m.group(1).strip()
    amount = float(m.group(2).replace(",", "."))
    cat = find_category_by_name(msg.from_user.id, name)
    if not cat:
        await msg.answer(f"Категория «{name}» не найдена. Смотри /categories")
        return
    cat_id, emoji, cname = cat
    set_limit(msg.from_user.id, cat_id, amount)
    if amount <= 0:
        await msg.answer(f"🗑 Лимит для {emoji} {cname} удалён.")
    else:
        await msg.answer(f"💰 Лимит для {emoji} {cname}: <b>{amount:g}</b>/мес")


@router.message(Command("limits"))
async def on_limits(msg: Message):
    if not await require_pro(msg):
        return
    rows = list_limits(msg.from_user.id)
    if not rows:
        await msg.answer(
            "Лимитов пока нет. Поставь:\n<code>/limit Еда 15000</code>"
        )
        return
    lines = ["💰 <b>Лимиты на этот месяц</b>", ""]
    for cid, emoji, name, lim, spent in rows:
        pct = spent / lim * 100 if lim else 0
        if pct >= 100:
            dot = "🔴"
        elif pct >= 80:
            dot = "🟡"
        else:
            dot = "🟢"
        lines.append(
            f"{dot} {emoji} {name}: <b>{spent:g}</b> / {lim:g} ({pct:.0f}%)"
        )
    await msg.answer("\n".join(lines))


# ----- Pro: регулярные платежи -----
@router.message(Command("rec"))
async def on_rec_add(msg: Message, command: CommandObject):
    if not await require_pro(msg):
        return
    args = (command.args or "").strip()
    # формат: <имя> <сумма> <категория> <день>
    # День и сумма — числа, категория должна существовать. Парсим с конца.
    # Берём последний токен как day, предпоследний — имя категории (1 слово), дальше сумма с конца, остальное — имя.
    tokens = args.split()
    if len(tokens) < 4:
        await msg.answer(
            "Использование:\n"
            "<code>/rec аренда 45000 Жильё 1</code>\n"
            "= <имя> <сумма> <категория одним словом> <день месяца 1-28>"
        )
        return
    try:
        day = int(tokens[-1])
        cat_name = tokens[-2]
        amount = float(tokens[-3].replace(",", "."))
        name = " ".join(tokens[:-3])
    except ValueError:
        await msg.answer("Не смог разобрать. Формат: <code>/rec имя сумма категория день</code>")
        return
    if not (1 <= day <= 28):
        await msg.answer("День месяца должен быть от 1 до 28.")
        return
    if amount <= 0:
        await msg.answer("Сумма должна быть больше нуля.")
        return
    cat = find_category_by_name(msg.from_user.id, cat_name)
    if not cat:
        await msg.answer(f"Категория «{cat_name}» не найдена. Смотри /categories")
        return
    cat_id, emoji, cname = cat
    rec_id = add_recurring(msg.from_user.id, name, amount, cat_id, day)
    await msg.answer(
        f"🔁 Добавлено #{rec_id}: <b>{name}</b> — {amount:g} → {emoji} {cname}, "
        f"каждое {day}-е число."
    )


@router.message(Command("recs"))
async def on_recs(msg: Message):
    if not await require_pro(msg):
        return
    rows = list_recurring(msg.from_user.id)
    if not rows:
        await msg.answer(
            "Регулярных платежей нет. Добавь:\n"
            "<code>/rec аренда 45000 Жильё 1</code>"
        )
        return
    lines = ["🔁 <b>Регулярные платежи</b>", ""]
    for rid, name, amount, emoji, cname, day in rows:
        lines.append(
            f"#{rid}: <b>{name}</b> — {amount:g} → {emoji} {cname}, "
            f"{day}-е число\n   удалить: /delrec_{rid}"
        )
    await msg.answer("\n\n".join([lines[0], "\n\n".join(lines[2:])]))


@router.message(F.text.regexp(r"^/delrec_(\d+)$"))
async def on_delrec(msg: Message):
    if not await require_pro(msg):
        return
    rec_id = int(msg.text.split("_", 1)[1])
    if delete_recurring(rec_id, msg.from_user.id):
        await msg.answer(f"🗑 Регулярный платёж #{rec_id} удалён.")
    else:
        await msg.answer("Не найдено.")


# ----- Pro: экспорт CSV -----
@router.message(Command("export"))
async def on_export(msg: Message):
    if not await require_pro(msg):
        return
    import csv
    import io
    from aiogram.types import BufferedInputFile

    with closing(db_connect()) as conn:
        rows = conn.execute(
            "SELECT e.created_at, e.amount, COALESCE(c.name, ''), "
            "       COALESCE(c.emoji, ''), e.comment "
            "FROM expenses e LEFT JOIN categories c ON c.id = e.category_id "
            "WHERE e.user_id = ? ORDER BY e.created_at",
            (msg.from_user.id,),
        ).fetchall()

    if not rows:
        await msg.answer("Расходов пока нет — нечего экспортировать.")
        return

    buf = io.StringIO()
    # BOM для корректного открытия в Excel с кириллицей
    buf.write("\ufeff")
    writer = csv.writer(buf, delimiter=";")
    writer.writerow(["Дата", "Сумма", "Категория", "Эмодзи", "Комментарий"])
    for created_at, amount, cname, emoji, comment in rows:
        writer.writerow([created_at, amount, cname, emoji, comment])

    data = buf.getvalue().encode("utf-8")
    filename = f"expenses_{datetime.now():%Y%m%d_%H%M%S}.csv"
    await msg.answer_document(
        BufferedInputFile(data, filename=filename),
        caption=f"📄 Экспорт: {len(rows)} записей",
    )


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
    # Pro: автокатегоризация — если бот уже знает слово, сразу сохраняем
    if is_user_pro(msg.from_user.id):
        auto_cat_id = auto_category_for(msg.from_user.id, comment)
        if auto_cat_id is not None:
            exp_id = save_expense(msg.from_user.id, auto_cat_id, amount, comment)
            with closing(db_connect()) as conn:
                row = conn.execute(
                    "SELECT emoji, name FROM categories WHERE id = ?", (auto_cat_id,)
                ).fetchone()
            emoji, name = row if row else ("❔", "Без категории")
            text = f"✅ Записано: {emoji} {name} — <b>{amount:g}</b> <i>(авто)</i>"
            if comment:
                text += f"\n💬 {comment}"
            warn = limit_warning(msg.from_user.id, auto_cat_id)
            if warn:
                text += f"\n\n{warn}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Изменить категорию", callback_data=f"recat:{exp_id}")
            ]])
            await msg.answer(text, reply_markup=kb)
            return

    exp_id = save_expense(msg.from_user.id, None, amount, comment)
    preview = f"💸 <b>{amount:g}</b>"
    if comment:
        preview += f" — {comment}"
    preview += "\n\nВыбери категорию:"
    await msg.answer(preview, reply_markup=categories_kb(msg.from_user.id, exp_id))


# ---------- Запуск ----------
async def recurring_scheduler(bot: Bot):
    """Фоновая задача: раз в час проверяет регулярные платежи и записывает их."""
    while True:
        try:
            today = datetime.now()
            today_date = today.strftime("%Y-%m-%d")
            day = today.day
            with closing(db_connect()) as conn:
                due = conn.execute(
                    "SELECT r.id, r.user_id, r.name, r.amount, r.category_id, "
                    "       c.emoji, c.name "
                    "FROM recurring r JOIN categories c ON c.id = r.category_id "
                    "WHERE r.day_of_month = ? "
                    "  AND (r.last_run_date IS NULL OR r.last_run_date < ?)",
                    (day, today_date),
                ).fetchall()
            for rid, uid, name, amount, cat_id, emoji, cname in due:
                save_expense(uid, cat_id, amount, name)
                with closing(db_connect()) as conn, conn:
                    conn.execute(
                        "UPDATE recurring SET last_run_date = ? WHERE id = ?",
                        (today_date, rid),
                    )
                try:
                    text = (
                        f"🔁 Автосписание: <b>{name}</b> — {amount:g} → "
                        f"{emoji} {cname}"
                    )
                    warn = limit_warning(uid, cat_id)
                    if warn:
                        text += f"\n\n{warn}"
                    await bot.send_message(uid, text)
                except Exception as e:
                    logging.warning("Не удалось уведомить %s: %s", uid, e)
        except Exception as e:
            logging.exception("recurring_scheduler error: %s", e)
        await asyncio.sleep(3600)  # раз в час


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
    asyncio.create_task(recurring_scheduler(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
