import os
import json
import asyncio
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import aiosqlite
import httpx
from bs4 import BeautifulSoup
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import F
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))
DATA_DIR = Path(__file__).parent / "data"
SCHEDULE_PATH = DATA_DIR / "schedule.json"
RC_CACHE_PATH = DATA_DIR / "rc_schedule.json"
RC_URL = "https://racecontrol.gg"
DB_PATH = DATA_DIR / "bot.db"

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="UTC")

MONTHS = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
          "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}


# ---------- Data ----------
def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_events():
    # Prefer fresh RaceControl cache
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


def next_event():
    now = datetime.now(timezone.utc)
    events = sorted(load_events(), key=lambda e: e["start_utc"])
    for e in events:
        dt = parse_dt(e["start_utc"])
        if dt > now:
            return e, dt
    return None, None


def events_in_range(hours_from, hours_to):
    now = datetime.now(timezone.utc)
    lo = now + timedelta(hours=hours_from)
    hi = now + timedelta(hours=hours_to)
    res = []
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if lo <= dt <= hi:
            res.append((dt, e))
    res.sort(key=lambda x: x[0])
    return res


def today_events():
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    res = []
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if start <= dt < end:
            res.append((dt, e))
    res.sort(key=lambda x: x[0])
    return res


def tomorrow_events():
    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    res = []
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if start <= dt < end:
            res.append((dt, e))
    res.sort(key=lambda x: x[0])
    return res


# ---------- RaceControl parser ----------
async def fetch_racecontrol_events():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(RC_URL, headers={"User-Agent": "Mozilla/5.0"})
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
        # track: find race-header containing "Track"
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
        # times
        times = []
        marquee = card.find("div", class_="marquee-content")
        if marquee:
            for badge in marquee.find_all("span", class_="badge"):
                times.append(badge.get_text(strip=True))
        for t in times:
            # format: "19 Nov at 3:00pm"
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
                "start_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sessions": [{"name": "Race", "start_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ")}],
                "duration": duration,
                "image": image,
                "url": RC_URL
            })
    return events


async def update_rc_cache():
    try:
        events = await fetch_racecontrol_events()
        if events:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(RC_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(events, f, ensure_ascii=False, indent=2)
            return len(events)
    except Exception as e:
        print("RC fetch error:", e)
    return 0


# ---------- DB ----------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                notify_24h INTEGER DEFAULT 1,
                notify_1h INTEGER DEFAULT 1,
                notify_start INTEGER DEFAULT 1,
                daily INTEGER DEFAULT 1,
                tz TEXT DEFAULT 'UTC',
                offset_minutes INTEGER DEFAULT 0
            )
        """)
        # migration: add tz column if missing
        try:
            await db.execute("ALTER TABLE subscribers ADD COLUMN tz TEXT DEFAULT 'UTC'")
        except Exception:
            pass
        # migration: add offset_minutes column if missing
        try:
            await db.execute("ALTER TABLE subscribers ADD COLUMN offset_minutes INTEGER DEFAULT 0")
        except Exception:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent (
                event_id TEXT,
                chat_id INTEGER,
                kind TEXT,
                PRIMARY KEY (event_id, chat_id, kind)
            )
        """)
        await db.commit()


async def is_subscribed(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,))
        return await cur.fetchone() is not None


async def subscribe(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
        await db.commit()


async def unsubscribe(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
        await db.commit()


async def get_tz(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tz FROM subscribers WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
    if row and row[0]:
        return row[0]
    return "UTC"


async def get_offset_minutes(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT offset_minutes FROM subscribers WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
    return row[0] if row else 0


async def set_tz(chat_id, tz=None, offset_minutes=0):
    await subscribe(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscribers SET tz=?, offset_minutes=? WHERE chat_id=?",
                        (tz or "UTC", offset_minutes, chat_id))
        await db.commit()


def parse_offset(s):
    """Parse '+3', '-5', '+05:30', 'UTC+3' etc. Return minutes or None."""
    s = s.strip().replace("UTC", "").replace("utc", "").strip()
    m = re.match(r"^([+-])(\d{1,2})(?::?(\d{2}))?$", s)
    if not m:
        return None
    sign = 1 if m.group(1) == "+" else -1
    hours = int(m.group(2))
    mins = int(m.group(3)) if m.group(3) else 0
    return sign * (hours * 60 + mins)


def valid_tz(tz):
    if tz is None:
        return False
    try:
        ZoneInfo(tz)
        return True
    except Exception:
        return parse_offset(tz) is not None


def get_tzinfo(tz_name, offset_minutes=0):
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
    return get_tzinfo(tz, off)


async def all_subscribers():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id, notify_24h, notify_1h, notify_start, daily, tz, offset_minutes FROM subscribers")
        rows = await cur.fetchall()
    return rows


async def already_sent(event_id, chat_id, kind):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM sent WHERE event_id=? AND chat_id=? AND kind=?", (event_id, chat_id, kind))
        return await cur.fetchone() is not None


async def mark_sent(event_id, chat_id, kind):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO sent VALUES (?,?,?)", (event_id, chat_id, kind))
        await db.commit()


# ---------- Handlers ----------
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer(
        "Привет! Я бот расписания Le Mans Ultimate (данные с RaceControl.gg).\n\n"
        "Команды:\n"
        "/next — следующее событие\n"
        "/schedule — расписание недели\n"
        "/subscribe — подписаться на еженедельную рассылку\n"
        "/unsubscribe — отписаться\n"
        "/tz — установить часовой пояс\n"
        "/refresh — обновить расписание с RaceControl\n"
        "/help — справка"
    )


@dp.message(Command("next"))
async def cmd_next(msg: types.Message):
    now = datetime.now(timezone.utc)
    tz_name = await get_tz(msg.chat.id)
    off = await get_offset_minutes(msg.chat.id)
    tz = get_tzinfo(tz_name, off)
    label = tz_label(tz_name, off)
    # ближайшее событие по каждой категории
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
    # порядок категорий и медали
    order = ["Beginner", "Intermediate", "Advanced"]
    medals = {"Beginner": "🥉", "Intermediate": "🥈", "Advanced": "🥇"}
    lines = ["🏁 Ближайшие гонки по категориям:"]
    for cat in order + [c for c in by_cat if c not in order]:
        if cat not in by_cat:
            continue
        dt, e = by_cat[cat]
        delta = dt - now
        hours = int(delta.total_seconds() // 3600)
        mins = int((delta.total_seconds() % 3600) // 60)
        medal = medals.get(cat, "🏅")
        dur = e.get("duration", "")
        dur_line = f"\n⏱ Длительность: {dur}" if dur else ""
        local = dt.astimezone(tz)
        lines.append(
            f"\n{medal} {cat} {medal}\n"
            f"{e['title']} — {e['track']}\n"
            f"⏰ {local.strftime('%H:%M')} ({label}) (через {hours}ч {mins}мин){dur_line}"
        )
    await msg.answer("\n".join(lines))


@dp.message(Command("schedule"))
async def cmd_schedule(msg: types.Message):
    now = datetime.now(timezone.utc)
    tz_name = await get_tz(msg.chat.id)
    off = await get_offset_minutes(msg.chat.id)
    tz = get_tzinfo(tz_name, off)
    label = tz_label(tz_name, off)
    # группируем по категории -> гонка (title+track) -> список времён старта
    by_cat = {}
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if not (0 <= (dt - now).total_seconds() <= 7 * 86400):
            continue
        cat = (e.get("class") or "").strip() or "Other"
        key = (e.get("title"), e.get("track"))
        by_cat.setdefault(cat, {}).setdefault(key, []).append(dt)
    if not by_cat:
        await msg.answer("На ближайшие 7 дней событий нет. Попробуйте /refresh")
        return
    order = ["Beginner", "Intermediate", "Advanced"]
    medals = {"Beginner": "🥉", "Intermediate": "🥈", "Advanced": "🥇"}
    lines = ["📅 Расписание на неделю Daily:"]
    for cat in order + [c for c in by_cat if c not in order]:
        if cat not in by_cat:
            continue
        medal = medals.get(cat, "🏅")
        lines.append(f"\n{medal} {cat} {medal}")
        races = list(by_cat[cat].items())
        for idx, ((title, track), times) in enumerate(races):
            times_sorted = sorted(set(t.astimezone(tz).strftime("%H:%M") for t in times))
            dur = ""
            for e in load_events():
                if e.get("title") == title and e.get("track") == track:
                    dur = e.get("duration", "")
                    break
            dur_str = f" ({dur})" if dur else ""
            lines.append(f"• {title} — {track}{dur_str}")
            lines.append(f"⏰ {', '.join(times_sorted)} ({label})")
            if idx < len(races) - 1:
                lines.append("")
        lines.append("")
    await msg.answer("\n".join(lines))


@dp.message(Command("subscribe"))
async def cmd_subscribe(msg: types.Message):
    await subscribe(msg.chat.id)
    await msg.answer(
        "✅ Вы подписаны на еженедельную рассылку!\n"
        "Каждый вторник в 10:30 UTC буду присылать\n"
        "расписание гонок на ближайшую неделю.\n\n"
        "Отписаться: /unsubscribe"
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
        if not valid_tz(tz):
            await msg.answer("❌ Неверный часовой пояс. Пример: Europe/Kyiv или +3")
            return
        off = parse_offset(tz)
        if off is None:
            await set_tz(msg.chat.id, tz, 0)
            await msg.answer(f"✅ Часовой пояс установлен: {tz}")
        else:
            await set_tz(msg.chat.id, "UTC", off)
            sign = "+" if off >= 0 else ""
            await msg.answer(f"✅ Часовой пояс установлен: UTC{sign}{off//60}:{abs(off)%60:02d}")
        return
    tz = await get_tz(msg.chat.id)
    await msg.answer(
        f"Ваш часовой пояс: {tz}\n\n"
        "Выберите из списка или введите вручную:\n"
        "/tz +3  или  /tz Europe/Kyiv",
        reply_markup=tz_keyboard()
    )


@dp.callback_query(F.data.startswith("tz:"))
async def cb_tz(callback: types.CallbackQuery):
    tz = callback.data.split(":", 1)[1]
    if not valid_tz(tz):
        await callback.answer("Неверный пояс", show_alert=True)
        return
    off = parse_offset(tz)
    if off is None:
        await set_tz(callback.from_user.id, tz, 0)
        await callback.message.edit_text(f"✅ Часовой пояс установлен: {tz}")
    else:
        await set_tz(callback.from_user.id, "UTC", off)
        sign = "+" if off >= 0 else ""
        await callback.message.edit_text(f"✅ Часовой пояс установлен: UTC{sign}{off//60}:{abs(off)%60:02d}")
    await callback.answer()


@dp.callback_query(F.data == "tz:manual")
async def cb_tz_manual(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Введите часовой пояс командой:\n"
        "/tz +3  или  /tz Europe/Kyiv\n\n"
        "Поддерживаются IANA-зоны и смещения (+3, -5, +05:30)."
    )
    await callback.answer()


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(msg: types.Message):
    await unsubscribe(msg.chat.id)
    await msg.answer("❌ Вы отписаны от уведомлений.")


@dp.message(Command("refresh"))
async def cmd_refresh(msg: types.Message):
    await msg.answer("🔄 Обновляю расписание с RaceControl.gg...")
    n = await update_rc_cache()
    if n:
        await msg.answer(f"✅ Загружено событий: {n}")
    else:
        await msg.answer("❌ Не удалось получить данные. Попробуйте позже.")


@dp.message(Command("help"))
async def cmd_help(msg: types.Message):
    await msg.answer(
        "Бот расписания LMU (данные RaceControl.gg).\n"
        "/next — следующее событие\n"
        "/schedule — расписание недели\n"
        "/subscribe — подписаться на еженедельную рассылку\n"
        "/unsubscribe — отписаться\n"
        "/tz — установить часовой пояс\n"
        "/refresh — обновить расписание"
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
    for chat_id, n24, n1, ns, daily, tz_name, off in subs:
        if not daily:
            continue
        tz = get_tzinfo(tz_name or "UTC", off or 0)
        label = tz_label(tz_name or "UTC", off or 0)
        if not events:
            text = "📅 На ближайшую неделю событий нет."
        else:
            lines = ["📅 Расписание LMU на неделю Daily:"]
            for dt, e in events[:40]:
                local = dt.astimezone(tz)
                lines.append(f"• {local.strftime('%d.%m %H:%M')} ({label}) — {e['title']} ({e['track']}) [{e['class']}]")
            text = "\n".join(lines)
        await safe_send(chat_id, text)


# ---------- Web ----------
async def health(request):
    return web.Response(text="ok")


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    await init_db()
    # initial fetch in background (don't block startup)
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

    # RaceControl weekly schedule updates every Tuesday 10:00 UTC
    scheduler.add_job(update_rc_cache, "cron", day_of_week="tue", hour=10, minute=0)
    scheduler.add_job(weekly_digest, "cron", day_of_week="tue", hour=10, minute=30)
    scheduler.add_job(update_rc_cache, "interval", hours=6)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())