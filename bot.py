import os
import json
import asyncio
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
import httpx
from bs4 import BeautifulSoup
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import BotCommand, BotCommandScopeChat
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).parent / "data")))
SCHEDULE_PATH = Path(__file__).parent / "data" / "schedule.json"
RC_CACHE_PATH = DATA_DIR / "rc_schedule.json"
RC_URL = "https://racecontrol.gg"

bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.update.outer_middleware()
async def dedup_middleware(handler, event, data):
    global _last_update_id
    update_id = getattr(event, "update_id", None)
    if update_id is not None:
        if update_id <= _last_update_id:
            return None
        _last_update_id = update_id
    return await handler(event, data)
scheduler = AsyncIOScheduler(timezone="UTC")
DB_POOL = None
_last_update_id = 0

MONTHS = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
          "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

T = {
    "ru": {
        "start": "Привет! Я бот расписания Le Mans Ultimate.\n(данные с RaceControl.gg)\n\n",
        "next": "🏁 Ближайшие гонки по категориям:",
        "schedule": "📅 Расписание Daily:",
        "no_events": "Событий не найдено. Попробуйте /refresh",
        "no_week": "На ближайшие 7 дней событий нет. Попробуйте /refresh",
        "refreshing": "🔄 Расписание обновляется, попробуйте через минуту.",
        "refresh_ok": "✅ Загружено событий: {n}",
        "refresh_fail": "❌ Не удалось получить данные. Попробуйте позже.",
        "subscribed": "✅ Вы подписаны на еженедельную рассылку!\nКаждый вторник в 10:30 UTC буду присылать\nрасписание гонок на ближайшую неделю.\n\nОтписаться: /unsubscribe",
        "unsubscribed": "❌ Вы отписаны от уведомлений.",
        "help": "Бот расписания LMU (данные RaceControl.gg).",
        "tz_current": "Ваш часовой пояс: {tz}\n\nВыберите из списка или введите вручную:",
        "tz_set": "✅ Часовой пояс установлен: {tz}",
        "tz_invalid": "❌ Неверный часовой пояс. Пример: Europe/Kyiv или +3",
        "weekly_empty": "📅 На ближайшую неделю событий нет.",
        "weekly_title": "📅 Расписание LMU Daily:",
        "dur": "⏱ Длительность: {dur}",
        "start_in": "через {h}ч {m}мин",
    },
    "uk": {
        "start": "Привіт! Я бот розкладу Le Mans Ultimate.\n(дані з RaceControl.gg)\n\n",
        "next": "🏁 Найближчі гонки за категоріями:",
        "schedule": "📅 Розклад Daily:",
        "no_events": "Подій не знайдено. Спробуйте /refresh",
        "no_week": "На найближчі 7 днів подій немає. Спробуйте /refresh",
        "refreshing": "🔄 Розклад оновлюється, спробуйте через хвилину.",
        "refresh_ok": "✅ Завантажено подій: {n}",
        "refresh_fail": "❌ Не вдалося отримати дані. Спробуйте пізніше.",
        "subscribed": "✅ Ви підписані на щотижневу розсилку!\nЩовівторка о 10:30 UTC буду надсилати\nрозклад гонок на найближчий тиждень.\n\nВідписатися: /unsubscribe",
        "unsubscribed": "❌ Ви відписані від сповіщень.",
        "help": "Бот розкладу LMU (дані RaceControl.gg).",
        "tz_current": "Ваш часовий пояс: {tz}\n\nОберіть зі списку або введіть вручну:",
        "tz_set": "✅ Часовий пояс встановлено: {tz}",
        "tz_invalid": "❌ Невірний часовий пояс. Приклад: Europe/Kyiv або +3",
        "weekly_empty": "📅 На найближчий тиждень подій немає.",
        "weekly_title": "📅 Розклад LMU Daily:",
        "dur": "⏱ Тривалість: {dur}",
        "start_in": "через {h}год {m}хв",
    },
    "en": {
        "start": "Hi! I'm the Le Mans Ultimate schedule bot.\n(data from RaceControl.gg)\n\n",
        "next": "🏁 Nearest races by category:",
        "schedule": "📅 Weekly schedule Daily:",
        "no_events": "No events found. Try /refresh",
        "no_week": "No events in the next 7 days. Try /refresh",
        "refreshing": "🔄 Schedule is updating, try again in a minute.",
        "refresh_ok": "✅ Loaded events: {n}",
        "refresh_fail": "❌ Failed to fetch data. Try later.",
        "subscribed": "✅ You are subscribed to the weekly digest!\nEvery Tuesday at 10:30 UTC I'll send\nthe race schedule for the upcoming week.\n\nUnsubscribe: /unsubscribe",
        "unsubscribed": "❌ You are unsubscribed from notifications.",
        "help": "LMU schedule bot (data from RaceControl.gg).",
        "tz_current": "Your timezone: {tz}\n\nChoose from the list or enter manually:",
        "tz_set": "✅ Timezone set: {tz}",
        "tz_invalid": "❌ Invalid timezone. Example: Europe/Kyiv or +3",
        "weekly_empty": "📅 No events in the upcoming week.",
        "weekly_title": "📅 LMU Weekly Schedule:",
        "dur": "⏱ Duration: {dur}",
        "start_in": "in {h}h {m}min",
    },
}


def tr(key, lang="ru", **kwargs):
    lang = lang if lang in T else "ru"
    text = T[lang].get(key, T["ru"].get(key, key))
    if kwargs:
        text = text.format(**kwargs)
    return text


# ---------- Data ----------
def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_events():
    if RC_CACHE_PATH.exists():
        try:
            with open(RC_CACHE_PATH, encoding="utf-8") as f:
                data = json.load(f)
            if data:
                return data
        except Exception:
            pass
    with open(SCHEDULE_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------- DB ----------
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id BIGINT PRIMARY KEY,
                notify_24h INTEGER DEFAULT 1,
                notify_1h INTEGER DEFAULT 1,
                notify_start INTEGER DEFAULT 1,
                daily INTEGER DEFAULT 1,
                tz TEXT DEFAULT 'UTC',
                offset_minutes INTEGER DEFAULT 0,
                lang TEXT DEFAULT 'ru'
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sent (
                event_id TEXT,
                chat_id BIGINT,
                kind TEXT,
                PRIMARY KEY (event_id, chat_id, kind)
            )
        """)
        # migration
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id BIGINT PRIMARY KEY,
                first_seen TIMESTAMPTZ DEFAULT NOW(),
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ DEFAULT NOW()")
        await conn.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'ru'")
        await conn.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS feedback_mode INTEGER DEFAULT 0")


async def subscribe(chat_id):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO subscribers (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING",
            chat_id
        )


async def track_user(chat_id):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO UPDATE SET last_seen=NOW()",
            chat_id
        )


async def unsubscribe(chat_id):
    await subscribe(chat_id)
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE subscribers SET daily=0 WHERE chat_id=$1", chat_id)


async def is_subscribed(chat_id):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM subscribers WHERE chat_id=$1", chat_id)
    return row is not None


async def all_subscribers():
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch(
            "SELECT chat_id, notify_24h, notify_1h, notify_start, daily, tz, offset_minutes, lang FROM subscribers"
        )
    return [tuple(r.values()) for r in rows]


async def get_tz(chat_id):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT tz FROM subscribers WHERE chat_id=$1", chat_id)
    return row["tz"] if row and row["tz"] else "UTC"


async def get_offset_minutes(chat_id):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT offset_minutes FROM subscribers WHERE chat_id=$1", chat_id)
    return row["offset_minutes"] if row else 0


async def get_lang(chat_id):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT lang FROM subscribers WHERE chat_id=$1", chat_id)
    return (row["lang"] if row and row["lang"] else "ru")


async def set_lang(chat_id, lang):
    await subscribe(chat_id)
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE subscribers SET lang=$1 WHERE chat_id=$2", lang, chat_id)


async def get_feedback_mode(chat_id):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT feedback_mode FROM subscribers WHERE chat_id=$1", chat_id)
    return bool(row and row["feedback_mode"])


async def set_feedback_mode(chat_id, mode):
    await subscribe(chat_id)
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE subscribers SET feedback_mode=$1 WHERE chat_id=$2", 1 if mode else 0, chat_id)


async def set_tz(chat_id, tz=None, offset_minutes=0):
    await subscribe(chat_id)
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE subscribers SET tz=$1, offset_minutes=$2 WHERE chat_id=$3",
            tz or "UTC", offset_minutes, chat_id
        )


def valid_tz(tz):
    if tz is None:
        return False
    try:
        ZoneInfo(tz)
        return True
    except Exception:
        return parse_offset(tz) is not None


def parse_offset(s):
    s = s.strip().replace("UTC", "").replace("utc", "").strip()
    m = re.match(r"^([+-])(\d{1,2})(?::?(\d{2}))?$", s)
    if not m:
        return None
    sign = 1 if m.group(1) == "+" else -1
    hours = int(m.group(2))
    mins = int(m.group(3)) if m.group(3) else 0
    return sign * (hours * 60 + mins)


def get_tzinfo(tz_name, offset_minutes=0):
    if tz_name == "UTC" and offset_minutes:
        return timezone(timedelta(minutes=offset_minutes))
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone(timedelta(minutes=offset_minutes))


def tz_label(tz_name, offset_minutes=0):
    if tz_name and tz_name != "UTC":
        return tz_name
    sign = "+" if offset_minutes >= 0 else ""
    h = offset_minutes // 60
    m = abs(offset_minutes) % 60
    if m:
        return f"UTC{sign}{h}:{m:02d}"
    return f"UTC{sign}{h}"


async def get_tzinfo_for(chat_id):
    tz = await get_tz(chat_id)
    off = await get_offset_minutes(chat_id)
    return get_tzinfo(tz, off), tz, off


async def already_sent(event_id, chat_id, kind):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM sent WHERE event_id=$1 AND chat_id=$2 AND kind=$3",
            event_id, chat_id, kind
        )
    return row is not None


async def mark_sent(event_id, chat_id, kind):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO sent (event_id, chat_id, kind) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
            event_id, chat_id, kind
        )


async def cleanup_inactive(days=90):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "DELETE FROM subscribers WHERE chat_id IN (SELECT chat_id FROM users WHERE last_seen < NOW() - INTERVAL '%s days')" % days
        )
        await conn.execute(
            "DELETE FROM users WHERE last_seen < NOW() - INTERVAL '%s days'" % days
        )
    print(f"Cleaned up users inactive for {days} days")


# ---------- RaceControl parser ----------
async def fetch_racecontrol_events():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(RC_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", class_="scheduled-race-card")
    now = datetime.now(timezone.utc)
    events = []
    for card in cards:
        title_el = card.find("h4")
        title = title_el.get_text(strip=True) if title_el else "Unknown"
        img_el = card.find("img")
        image = img_el.get("src") if img_el else ""
        tier_el = card.find("span", class_="tier-badge")
        tier = tier_el.get_text(strip=True) if tier_el else ""
        track = ""
        duration = ""
        for header in card.find_all("div", class_="race-header"):
            spans = header.find_all("span")
            if len(spans) >= 2:
                label = spans[0].get_text(strip=True).lower()
                if "track" in label:
                    track = spans[-1].get_text(strip=True)
                if "duration" in label:
                    duration = spans[-1].get_text(strip=True)
        times = []
        marquee = card.find("div", class_="marquee-content")
        if marquee:
            for badge in marquee.find_all("span", class_="badge"):
                times.append(badge.get_text(strip=True))
        for t in times:
            m = re.match(r"(\d{1,2})\s+(\w{3})\s+at\s+(\d{1,2}):(\d{2})(am|pm)", t, re.I)
            if not m:
                continue
            day = int(m.group(1))
            mon = MONTHS.get(m.group(2).lower()[:3])
            if not mon:
                continue
            hour = int(m.group(3))
            minute = int(m.group(4))
            ap = m.group(5).lower()
            if ap == "pm" and hour != 12:
                hour += 12
            if ap == "am" and hour == 12:
                hour = 0
            year = now.year
            try:
                dt = datetime(year, mon, day, hour, minute, tzinfo=timezone.utc)
            except ValueError:
                continue
            if dt < now - timedelta(days=1):
                try:
                    dt = datetime(year + 1, mon, day, hour, minute, tzinfo=timezone.utc)
                except ValueError:
                    continue
            events.append({
                "id": f"rc-{title.lower().replace(' ', '-')}-{dt.strftime('%Y%m%d%H%M')}",
                "title": title,
                "series": "Daily Race",
                "track": track,
                "class": tier.capitalize() if tier else "",
                "duration": duration,
                "start_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sessions": [{"name": "Race", "start_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ")}],
                "url": RC_URL
            })
    return events


_last_rc_update = None


async def update_rc_cache(force=False):
    global _last_rc_update
    now = datetime.now(timezone.utc)
    # не чаще чем раз в час, чтобы не блокировали
    if not force and _last_rc_update and (now - _last_rc_update).total_seconds() < 3600:
        return 0
    try:
        events = await fetch_racecontrol_events()
        if events:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(RC_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(events, f, ensure_ascii=False, indent=2)
            _last_rc_update = now
            return len(events)
    except Exception as e:
        print("RC fetch error:", e)
    return 0


# ---------- Handlers ----------
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await track_user(msg.chat.id)
    lang = await get_lang(msg.chat.id)
    sub_status = "✅ " + ("Вы подписаны на рассылку" if lang == "ru" else "Ви підписані на розсилку" if lang == "uk" else "You are subscribed")
    if not await is_subscribed(msg.chat.id):
        sub_status = "❌ " + ("Вы не подписаны. /subscribe" if lang == "ru" else "Ви не підписані. /subscribe" if lang == "uk" else "Not subscribed. /subscribe")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏁 " + ("Ближайшие" if lang == "ru" else "Найближчі" if lang == "uk" else "Nearest"), callback_data="menu:next")],
        [InlineKeyboardButton(text="📅 " + ("Расписание Daily" if lang == "ru" else "Розклад Daily" if lang == "uk" else "Schedule Daily"), callback_data="menu:schedule")],
        [InlineKeyboardButton(text=("✅ Подписаться" if lang == "ru" else "✅ Підписатися" if lang == "uk" else "✅ Subscribe") if not await is_subscribed(msg.chat.id) else ("❌ Отписаться" if lang == "ru" else "❌ Відписатися" if lang == "uk" else "❌ Unsubscribe"), callback_data="menu:sub" if not await is_subscribed(msg.chat.id) else "menu:unsub")],
        [InlineKeyboardButton(text="⚙️ " + ("Настройки" if lang == "ru" else "Налаштування" if lang == "uk" else "Settings"), callback_data="menu:settings")],
        [InlineKeyboardButton(text="💡 " + ("Обратная связь" if lang == "ru" else "Зворотний зв'язок" if lang == "uk" else "Feedback"), callback_data="menu:feedback")],
    ])
    if ADMIN_CHAT_ID and str(msg.chat.id) == str(ADMIN_CHAT_ID):
        kb.inline_keyboard.append([InlineKeyboardButton(text="🔧 Admin", callback_data="menu:admin")])
    await msg.answer(
        tr("start", lang) + f"\n{sub_status}",
        reply_markup=kb
    )


@dp.callback_query(F.data == "menu:next")
async def cb_menu_next(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_next(callback.message)


@dp.callback_query(F.data == "menu:schedule")
async def cb_menu_schedule(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_schedule(callback.message)


@dp.callback_query(F.data == "menu:sub")
async def cb_menu_sub(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_subscribe(callback.message)


@dp.callback_query(F.data == "menu:unsub")
async def cb_menu_unsub(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_unsubscribe(callback.message)


@dp.callback_query(F.data == "menu:settings")
async def cb_menu_settings(callback: types.CallbackQuery):
    await callback.answer()
    lang = await get_lang(callback.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕐 " + ("Часовой пояс" if lang == "ru" else "Часовий пояс" if lang == "uk" else "Timezone"), callback_data="settings:tz")],
        [InlineKeyboardButton(text="🌐 " + ("Язык" if lang == "ru" else "Мова" if lang == "uk" else "Language"), callback_data="settings:lang")],
    ])
    await callback.message.answer("⚙️ " + ("Настройки" if lang == "ru" else "Налаштування" if lang == "uk" else "Settings"), reply_markup=kb)


@dp.callback_query(F.data == "settings:tz")
async def cb_settings_tz(callback: types.CallbackQuery):
    await callback.answer()
    lang = await get_lang(callback.from_user.id)
    tz = await get_tz(callback.from_user.id)
    off = await get_offset_minutes(callback.from_user.id)
    label = tz_label(tz, off)
    await callback.message.edit_text(
        ("Ваш часовой пояс: " if lang == "ru" else "Ваш часовий пояс: " if lang == "uk" else "Your timezone: ") + label + "\n\n" +
        ("Выберите из списка или введите вручную:" if lang == "ru" else "Оберіть зі списку або введіть вручну:" if lang == "uk" else "Choose from list or enter manually:"),
        reply_markup=tz_keyboard()
    )


@dp.callback_query(F.data == "settings:lang")
async def cb_settings_lang(callback: types.CallbackQuery):
    await callback.answer()
    lang = await get_lang(callback.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru")],
        [InlineKeyboardButton(text="🇺🇦 Українська", callback_data="lang:uk")],
        [InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en")],
    ])
    await callback.message.edit_text(
        "Текущий язык: " + lang if lang == "ru" else "Поточна мова: " + lang if lang == "uk" else "Current language: " + lang,
        reply_markup=kb
    )


@dp.callback_query(F.data == "menu:admin")
async def cb_menu_admin(callback: types.CallbackQuery):
    await callback.answer()
    await cmd_admin(callback.message)


@dp.callback_query(F.data == "menu:feedback")
async def cb_menu_feedback(callback: types.CallbackQuery):
    await callback.answer()
    lang = await get_lang(callback.from_user.id)
    await set_feedback_mode(callback.from_user.id, True)
    await callback.message.answer(
        "💡 " + ("Напишите ваше пожелание следующим сообщением." if lang == "ru"
                 else "💡 Напишіть ваше побажання наступним повідомленням." if lang == "uk"
                 else "💡 Please send your feedback as the next message.")
    )


TZ_OPTIONS = [
    ("UTC", "UTC"),
    ("Europe/Kyiv", "Киев"),
    ("Europe/Moscow", "Москва"),
    ("Europe/London", "Лондон"),
    ("Europe/Berlin", "Берлин"),
    ("Europe/Paris", "Париж"),
    ("Europe/Rome", "Рим"),
    ("Europe/Madrid", "Мадрид"),
    ("Europe/Warsaw", "Варшава"),
    ("Europe/Istanbul", "Стамбул"),
    ("America/New_York", "Нью-Йорк"),
    ("America/Chicago", "Чикаго"),
    ("America/Denver", "Денвер"),
    ("America/Los_Angeles", "Лос-Анджелес"),
    ("America/Toronto", "Торонто"),
    ("America/Sao_Paulo", "Сан-Паулу"),
    ("Asia/Tokyo", "Токио"),
    ("Asia/Shanghai", "Шанхай"),
    ("Asia/Dubai", "Дубай"),
    ("Asia/Kolkata", "Калькутта"),
    ("Asia/Singapore", "Сингапур"),
    ("Asia/Seoul", "Сеул"),
    ("Australia/Sydney", "Сидней"),
    ("Pacific/Auckland", "Окленд"),
]


def tz_keyboard():
    rows = []
    for tz, label in TZ_OPTIONS:
        rows.append([InlineKeyboardButton(text=f"{label} ({tz})", callback_data=f"tz:{tz}")])
    rows.append([InlineKeyboardButton(text="✏️ Ввести вручную (+3 / Europe/...)", callback_data="tz:manual")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("tz"))
async def cmd_tz(msg: types.Message):
    args = (msg.text or "").split(maxsplit=1)
    if len(args) >= 2:
        tz = args[1].strip()
        lang = await get_lang(msg.chat.id)
        if not valid_tz(tz):
            await msg.answer(tr("tz_invalid", lang))
            return
        lang = await get_lang(msg.chat.id)
        off = parse_offset(tz)
        if off is None:
            await set_tz(msg.chat.id, tz, 0)
            await msg.answer(tr("tz_set", lang, tz=tz))
        else:
            await set_tz(msg.chat.id, "UTC", off)
            sign = "+" if off >= 0 else ""
            await msg.answer(tr("tz_set", lang, tz=f"UTC{sign}{off//60}:{abs(off)%60:02d}"))
        return
    lang = await get_lang(msg.chat.id)
    tz = await get_tz(msg.chat.id)
    off = await get_offset_minutes(msg.chat.id)
    label = tz_label(tz, off)
    await msg.answer(
        tr("tz_current", lang, tz=label) + "\n",
        reply_markup=tz_keyboard()
    )


@dp.callback_query(F.data.startswith("tz:"))
async def cb_tz(callback: types.CallbackQuery):
    tz = callback.data.split(":", 1)[1]
    if tz == "manual":
        await cb_tz_manual(callback)
        return
    if not valid_tz(tz):
        await callback.answer("Неверный пояс", show_alert=True)
        return
    off = parse_offset(tz)
    lang = await get_lang(callback.from_user.id)
    if off is None:
        await set_tz(callback.from_user.id, tz, 0)
        await callback.message.edit_text(tr("tz_set", lang, tz=tz))
    else:
        await set_tz(callback.from_user.id, "UTC", off)
        sign = "+" if off >= 0 else ""
        await callback.message.edit_text(tr("tz_set", lang, tz=f"UTC{sign}{off//60}:{abs(off)%60:02d}"))
    await callback.answer()


@dp.callback_query(F.data == "tz:manual")
async def cb_tz_manual(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Введите часовой пояс командой:\n"
        "/tz +3  или  /tz Europe/Kyiv\n\n"
        "Поддерживаются IANA-зоны и смещения (+3, -5, +05:30)."
    )
    await callback.answer()


@dp.message(Command("next"))
async def cmd_next(msg: types.Message):
    now = datetime.now(timezone.utc)
    if not any(parse_dt(e["start_utc"]) > now for e in load_events()):
        asyncio.create_task(update_rc_cache())
        await msg.answer("🔄 Расписание обновляется, попробуйте /next через минуту.")
        return
    lang = await get_lang(msg.chat.id)
    tz_name = await get_tz(msg.chat.id)
    off = await get_offset_minutes(msg.chat.id)
    tz = get_tzinfo(tz_name, off)
    label = tz_label(tz_name, off)
    by_cat = {}
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if dt <= now:
            continue
        cat = (e.get("class") or "").strip() or "Other"
        if cat not in by_cat or dt < by_cat[cat][0]:
            by_cat[cat] = (dt, e)
    if not by_cat:
        await msg.answer("Событий не найдено. Попробуйте /refresh")
        return
    order = ["Beginner", "Intermediate", "Advanced"]
    medals = {"Beginner": "🥉", "Intermediate": "🥈", "Advanced": "🥇"}
    lines = [tr("next", lang)]
    for cat in order + [c for c in by_cat if c not in order]:
        if cat not in by_cat:
            continue
        dt, e = by_cat[cat]
        delta = dt - now
        hours = int(delta.total_seconds() // 3600)
        mins = int((delta.total_seconds() % 3600) // 60)
        medal = medals.get(cat, "🏅")
        dur = e.get("duration", "")
        dur_line = f"\n{tr('dur', lang, dur=dur)}" if dur else ""
        local = dt.astimezone(tz)
        lines.append(
            f"\n{medal} {cat} {medal}\n"
            f"{e['title']} — {e['track']}\n"
            f"⏰ {local.strftime('%H:%M')} ({label}) ({tr('start_in', lang, h=hours, m=mins)}){dur_line}"
        )
    await msg.answer("\n".join(lines))


@dp.message(Command("schedule"))
async def cmd_schedule(msg: types.Message):
    now = datetime.now(timezone.utc)
    if not any(parse_dt(e["start_utc"]) > now for e in load_events()):
        asyncio.create_task(update_rc_cache())
        await msg.answer("🔄 Расписание обновляется, попробуйте /schedule через минуту.")
        return
    lang = await get_lang(msg.chat.id)
    tz_name = await get_tz(msg.chat.id)
    off = await get_offset_minutes(msg.chat.id)
    tz = get_tzinfo(tz_name, off)
    label = tz_label(tz_name, off)
    by_cat = {}
    has_future_today = False
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        # показывать гонки за последние 24 часа + ближайшие 7 дней
        if (dt - now).total_seconds() < -86400:
            continue
        if (dt - now).total_seconds() > 7 * 86400:
            continue
        cat = (e.get("class") or "").strip() or "Other"
        key = (e.get("title"), e.get("track"))
        by_cat.setdefault(cat, {}).setdefault(key, []).append(dt)
        if dt.date() == now.date() and dt > now:
            has_future_today = True
    if not by_cat:
        await msg.answer(tr("no_week", lang))
        return
    # отладка: показать количество событий и будущих
    future_count = sum(1 for e in load_events() if parse_dt(e["start_utc"]) > now)
    await msg.answer(f"DEBUG: total={len(load_events())}, future={future_count}, has_future_today={has_future_today}")
    order = ["Beginner", "Intermediate", "Advanced"]
    medals = {"Beginner": "🥉", "Intermediate": "🥈", "Advanced": "🥇"}
    lines = [tr("schedule", lang)]
    for cat in order + [c for c in by_cat if c not in order]:
        if cat not in by_cat:
            continue
        medal = medals.get(cat, "🏅")
        lines.append(f"\n{medal} {cat} {medal}")
        races = sorted(by_cat[cat].items(), key=lambda x: min(t for t in x[1]))
        for idx, ((title, track), times) in enumerate(races):
            try:
                times_sorted = list(dict.fromkeys(t.astimezone(tz).strftime("%H:%M") for t in sorted(times)))
            except Exception as ex:
                await msg.answer(f"❌ Ошибка времени: {ex}")
                return
            dur = ""
            for e in load_events():
                if e.get("title") == title and e.get("track") == track:
                    dur = e.get("duration", "")
                    break
            dur_str = f" ({dur})" if dur else ""
            lines.append(f"• {title} — {track}{dur_str}")
            if has_future_today:
                lines.append(f"⏰ {', '.join(times_sorted)} ({label})")
            if idx < len(races) - 1:
                lines.append("")
        lines.append("")
    await msg.answer("\n".join(lines))


@dp.message(Command("subscribe"))
async def cmd_subscribe(msg: types.Message):
    try:
        await subscribe(msg.chat.id)
        lang = await get_lang(msg.chat.id)
        await msg.answer(tr("subscribed", lang))
    except Exception as ex:
        await msg.answer(f"❌ Ошибка подписки: {ex}")


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(msg: types.Message):
    await unsubscribe(msg.chat.id)
    await msg.answer("❌ Вы отписаны от уведомлений.")


@dp.message(Command("lang"))
async def cmd_lang(msg: types.Message):
    lang = await get_lang(msg.chat.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru")],
        [InlineKeyboardButton(text="🇺🇦 Українська", callback_data="lang:uk")],
        [InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en")],
    ])
    await msg.answer(f"Текущий язык: {lang}\nCurrent language: {lang}", reply_markup=kb)


def get_commands(lang, is_admin=False):
    if lang == "uk":
        cmds = [
            BotCommand(command="start", description="Запуск бота"),
            BotCommand(command="next", description="Найближчі гонки за категоріями"),
            BotCommand(command="schedule", description="Розклад Daily"),
            BotCommand(command="subscribe", description="Підписатися на розсилку"),
            BotCommand(command="unsubscribe", description="Відписатися"),
            BotCommand(command="tz", description="Часовий пояс"),
            BotCommand(command="lang", description="Мова"),
            BotCommand(command="help", description="Допомога"),
        ]
    elif lang == "en":
        cmds = [
            BotCommand(command="start", description="Start bot"),
            BotCommand(command="next", description="Nearest races by category"),
            BotCommand(command="schedule", description="Schedule Daily"),
            BotCommand(command="subscribe", description="Subscribe to weekly digest"),
            BotCommand(command="unsubscribe", description="Unsubscribe"),
            BotCommand(command="tz", description="Timezone"),
            BotCommand(command="lang", description="Language"),
            BotCommand(command="help", description="Help"),
        ]
    else:
        cmds = [
            BotCommand(command="start", description="Запуск бота"),
            BotCommand(command="next", description="Ближайшие гонки по категориям"),
            BotCommand(command="schedule", description="Расписание Daily"),
            BotCommand(command="subscribe", description="Подписаться на рассылку"),
            BotCommand(command="unsubscribe", description="Отписаться"),
            BotCommand(command="tz", description="Часовой пояс"),
            BotCommand(command="lang", description="Язык"),
        ]
    # В меню (☰) только /start — остальное через inline-кнопки
    desc = {"ru": "Запуск бота", "uk": "Запуск бота", "en": "Start bot"}
    return [BotCommand(command="start", description=desc.get(lang, "Запуск бота"))]


@dp.callback_query(F.data.startswith("lang:"))
async def cb_lang(callback: types.CallbackQuery):
    lang = callback.data.split(":", 1)[1]
    await set_lang(callback.from_user.id, lang)
    is_admin = bool(ADMIN_CHAT_ID) and str(callback.from_user.id) == str(ADMIN_CHAT_ID)
    try:
        await bot.set_my_commands(get_commands(lang, is_admin), scope=BotCommandScopeChat(chat_id=callback.from_user.id))
    except Exception:
        pass
    if lang == "en":
        txt = f"✅ Language set: {lang}"
    elif lang == "uk":
        txt = f"✅ Мову встановлено: {lang}"
    else:
        txt = f"✅ Язык установлен: {lang}"
    await callback.message.edit_text(txt)
    await callback.answer()


@dp.message(Command("refresh"))
async def cmd_refresh(msg: types.Message):
    if ADMIN_CHAT_ID and str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    lang = await get_lang(msg.chat.id)
    await msg.answer("🔄 " + ("Обновляю расписание..." if lang == "ru" else "Updating schedule..."))
    n = await update_rc_cache(force=True)
    if n:
        await msg.answer(tr("refresh_ok", lang, n=n))
    else:
        await msg.answer(tr("refresh_fail", lang))


@dp.message(Command("health"))
async def cmd_health(msg: types.Message):
    if not ADMIN_CHAT_ID or str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    lang = await get_lang(msg.chat.id)
    # Telegram API
    tg_ok = "✅"
    try:
        await bot.get_me()
    except Exception:
        tg_ok = "❌"
    # Supabase
    db_ok = "✅"
    last_update = "—"
    users = 0
    subs = 0
    events = 0
    try:
        async with DB_POOL.acquire() as conn:
            await conn.fetchval("SELECT 1")
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            subs = await conn.fetchval("SELECT COUNT(*) FROM subscribers")
        events = len(load_events())
        if _last_rc_update:
            tz_name = await get_tz(msg.chat.id)
            off = await get_offset_minutes(msg.chat.id)
            tz = get_tzinfo(tz_name, off)
            label = tz_label(tz_name, off)
            local = _last_rc_update.astimezone(tz)
            last_update = f"{local.strftime('%d.%m.%Y %H:%M')} ({label})"
    except Exception:
        db_ok = "❌"
    if lang == "ru":
        text = (
            "Проверка LMU Schedule Bot\n\n"
            f"{tg_ok} Telegram API: работает\n"
            f"{db_ok} Supabase / PostgreSQL: работает\n\n"
            f"👥 Пользователей: {users}\n"
            f"📨 Подписчиков: {subs}\n"
            f"✅ Расписание: {events} событий\n"
            f"🕒 Последнее обновление: {last_update}"
        )
    else:
        text = (
            "LMU Schedule Bot health check\n\n"
            f"{tg_ok} Telegram API: ok\n"
            f"{db_ok} Supabase / PostgreSQL: ok\n\n"
            f"👥 Users: {users}\n"
            f"📨 Subscribers: {subs}\n"
            f"✅ Schedule: {events} events\n"
            f"🕒 Last update: {last_update}"
        )
    await msg.answer(text)


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if not ADMIN_CHAT_ID or str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    lang = await get_lang(msg.chat.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить расписание" if lang == "ru" else "🔄 Refresh schedule", callback_data="admin:refresh")],
        [InlineKeyboardButton(text="📊 Проверка бота" if lang == "ru" else "📊 Health check", callback_data="admin:health")],
    ])
    await msg.answer("Админ-панель:" if lang == "ru" else "Admin panel:", reply_markup=kb)


@dp.callback_query(F.data == "admin:refresh")
async def cb_admin_refresh(callback: types.CallbackQuery):
    if not ADMIN_CHAT_ID or str(callback.from_user.id) != str(ADMIN_CHAT_ID):
        return
    lang = await get_lang(callback.from_user.id)
    await callback.message.edit_text("🔄 " + ("Обновляю..." if lang == "ru" else "Updating..."))
    n = await update_rc_cache(force=True)
    text = (tr("refresh_ok", lang, n=n) if n else tr("refresh_fail", lang))
    await callback.message.edit_text(text)
    await callback.answer()


@dp.callback_query(F.data == "admin:health")
async def cb_admin_health(callback: types.CallbackQuery):
    if not ADMIN_CHAT_ID or str(callback.from_user.id) != str(ADMIN_CHAT_ID):
        return
    lang = await get_lang(callback.from_user.id)
    tg_ok = "✅"
    try:
        await bot.get_me()
    except Exception:
        tg_ok = "❌"
    db_ok = "✅"
    last_update = "—"
    users = 0
    subs = 0
    events = 0
    try:
        async with DB_POOL.acquire() as conn:
            await conn.fetchval("SELECT 1")
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            subs = await conn.fetchval("SELECT COUNT(*) FROM subscribers")
        events = len(load_events())
        if _last_rc_update:
            tz_name = await get_tz(callback.from_user.id)
            off = await get_offset_minutes(callback.from_user.id)
            tz = get_tzinfo(tz_name, off)
            label = tz_label(tz_name, off)
            local = _last_rc_update.astimezone(tz)
            last_update = f"{local.strftime('%d.%m.%Y %H:%M')} ({label})"
    except Exception:
        db_ok = "❌"
    except Exception:
        db_ok = "❌"
    if lang == "ru":
        text = (
            "Проверка LMU Schedule Bot\n\n"
            f"{tg_ok} Telegram API: работает\n"
            f"{db_ok} Supabase / PostgreSQL: работает\n\n"
            f"👥 Пользователей: {users}\n"
            f"📨 Подписчиков: {subs}\n"
            f"✅ Расписание: {events} событий\n"
            f"🕒 Последнее обновление: {last_update}"
        )
    else:
        text = (
            "LMU Schedule Bot health check\n\n"
            f"{tg_ok} Telegram API: ok\n"
            f"{db_ok} Supabase / PostgreSQL: ok\n\n"
            f"👥 Users: {users}\n"
            f"📨 Subscribers: {subs}\n"
            f"✅ Schedule: {events} events\n"
            f"🕒 Last update: {last_update}"
        )
    await callback.message.edit_text(text)
    await callback.answer()


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: types.Message):
    if await get_feedback_mode(msg.chat.id):
        await set_feedback_mode(msg.chat.id, False)
        lang = await get_lang(msg.chat.id)
        if ADMIN_CHAT_ID:
            await bot.send_message(
                int(ADMIN_CHAT_ID),
                f"💡 Feedback from {msg.chat.id} ({msg.from_user.full_name}):\n{msg.text}"
            )
        await msg.answer("✅ " + ("Спасибо! Пожелание отправлено." if lang == "ru"
                                 else "✅ Дякуємо! Побажання надіслано." if lang == "uk"
                                 else "✅ Thanks! Feedback sent."))


@dp.message(Command("help"))
async def cmd_help(msg: types.Message):
    lang = await get_lang(msg.chat.id)
    await msg.answer(
        tr("help", lang) + "\n"
        "/next — " + tr("next", lang) + "\n"
        "/schedule — " + tr("schedule", lang) + "\n"
        "/subscribe — " + tr("subscribed", lang).split(chr(10))[0] + "\n"
        "/unsubscribe — " + tr("unsubscribed", lang) + "\n"
        "/tz — " + ("часовой пояс" if lang == "ru" else "timezone") + "\n"
        "/lang — " + ("язык" if lang == "ru" else "language") + "\n"
        "/refresh — " + ("обновить расписание" if lang == "ru" else "refresh schedule")
    )


# ---------- Notifications ----------
async def safe_send(chat_id, text):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        await unsubscribe(chat_id)


async def weekly_digest():
    now = datetime.now(timezone.utc)
    events = []
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if 0 <= (dt - now).total_seconds() <= 7 * 86400:
            events.append((dt, e))
    events.sort(key=lambda x: x[0])
    subs = await all_subscribers()
    if not subs:
        return
    for chat_id, n24, n1, ns, daily, tz_name, off, lang in subs:
        if not daily:
            continue
        tz = get_tzinfo(tz_name or "UTC", off or 0)
        label = tz_label(tz_name or "UTC", off or 0)
        lang = lang or "ru"
        if not events:
            text = tr("weekly_empty", lang)
        else:
            lines = [tr("weekly_title", lang)]
            for dt, e in events[:40]:
                local = dt.astimezone(tz)
                lines.append(f"• {local.strftime('%d.%m %H:%M')} ({label}) — {e['title']} ({e['track']}) [{e['class']}]")
            text = "\n".join(lines)
        await safe_send(chat_id, text)
        await asyncio.sleep(0.05)


# ---------- Web ----------
async def health(request):
    return web.Response(text="ok")


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    await init_db()
    # очистить старые команды (на случай кэша)
    try:
        await bot.delete_my_commands()
    except Exception:
        pass
    # только базовые команды для всех (refresh/stats только через /admin)
    await bot.set_my_commands(get_commands("ru", False))
    asyncio.create_task(update_rc_cache())

    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)

    scheduler.add_job(update_rc_cache, "cron", hour=0, minute=15)  # ежедневно в 00:15 UTC
    scheduler.add_job(update_rc_cache, "cron", day_of_week="tue", hour=10, minute=15)
    scheduler.add_job(weekly_digest, "cron", day_of_week="tue", hour=10, minute=30)
    scheduler.add_job(update_rc_cache, "interval", hours=6)
    scheduler.add_job(cleanup_inactive, "cron", hour=3, minute=0)  # ежедневно в 03:00 UTC
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())