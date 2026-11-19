import os
import json
import asyncio
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite
import httpx
from bs4 import BeautifulSoup
from aiohttp import web
from aiogram import Bot, Dispatcher, types
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
    res.sort()
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
    res.sort()
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
    res.sort()
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
        tier_el = card.find("span", class_="tier-badge")
        tier = tier_el.get_text(strip=True) if tier_el else ""
        # track: find race-header containing "Track"
        track = ""
        for header in card.find_all("div", class_="race-header"):
            spans = header.find_all("span")
            if len(spans) >= 2 and "track" in spans[0].get_text(strip=True).lower():
                track = spans[-1].get_text(strip=True)
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
                daily INTEGER DEFAULT 1
            )
        """)
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


async def all_subscribers():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id, notify_24h, notify_1h, notify_start, daily FROM subscribers")
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
        "/schedule — ближайшие 7 дней\n"
        "/subscribe — подписаться на уведомления\n"
        "/unsubscribe — отписаться\n"
        "/refresh — обновить расписание с RaceControl\n"
        "/help — справка"
    )


@dp.message(Command("next"))
async def cmd_next(msg: types.Message):
    e, dt = next_event()
    if not e:
        await msg.answer("Событий не найдено. Попробуйте /refresh")
        return
    delta = dt - datetime.now(timezone.utc)
    hours = int(delta.total_seconds() // 3600)
    mins = int((delta.total_seconds() % 3600) // 60)
    await msg.answer(
        f"🏁 {e['title']}\n"
        f"Трасса: {e['track']} | Класс: {e['class']}\n"
        f"Старт (UTC): {e['start_utc']}\n"
        f"Осталось: {hours}ч {mins}мин"
    )


@dp.message(Command("schedule"))
async def cmd_schedule(msg: types.Message):
    now = datetime.now(timezone.utc)
    events = []
    for e in load_events():
        dt = parse_dt(e["start_utc"])
        if 0 <= (dt - now).total_seconds() <= 7 * 86400:
            events.append((dt, e))
    events.sort()
    if not events:
        await msg.answer("На ближайшие 7 дней событий нет. Попробуйте /refresh")
        return
    lines = ["📅 Ближайшие события (RaceControl):"]
    for dt, e in events[:30]:
        lines.append(f"• {dt.strftime('%d.%m %H:%M')} UTC — {e['title']} ({e['track']}) [{e['class']}]")
    await msg.answer("\n".join(lines))


@dp.message(Command("subscribe"))
async def cmd_subscribe(msg: types.Message):
    await subscribe(msg.chat.id)
    await msg.answer(
        "✅ Вы подписаны на уведомления!\n"
        "• ежедневный дайджест в 09:00 UTC\n"
        "• за 24 часа до старта\n"
        "• за 1 час до старта\n\n"
        "Отписаться: /unsubscribe"
    )


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
        "/schedule — ближайшие 7 дней\n"
        "/subscribe — подписаться на уведомления\n"
        "/unsubscribe — отписаться\n"
        "/refresh — обновить расписание"
    )


# ---------- Notifications ----------
async def safe_send(chat_id, text):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        await unsubscribe(chat_id)


async def daily_digest():
    today = today_events()
    tomorrow = tomorrow_events()
    subs = await all_subscribers()
    if not subs:
        return
    lines = ["📅 Дайджест расписания LMU (RaceControl)"]
    if today:
        lines.append("\n🏁 Сегодня:")
        for dt, e in today[:20]:
            lines.append(f"• {dt.strftime('%H:%M')} UTC — {e['title']} ({e['track']})")
    if tomorrow:
        lines.append("\n📆 Завтра:")
        for dt, e in tomorrow[:20]:
            lines.append(f"• {dt.strftime('%H:%M')} UTC — {e['title']} ({e['track']})")
    if not today and not tomorrow:
        lines.append("\nНа сегодня и завтра событий нет.")
    text = "\n".join(lines)
    for chat_id, n24, n1, ns, daily in subs:
        if daily:
            await safe_send(chat_id, text)


async def check_upcoming():
    for dt, e in events_in_range(23, 25):
        subs = await all_subscribers()
        for chat_id, n24, n1, ns, daily in subs:
            if not n24:
                continue
            if await already_sent(e["id"], chat_id, "24h"):
                continue
            await safe_send(chat_id, f"⏰ Через 24 часа: {e['title']} ({e['track']})\nСтарт: {e['start_utc']} UTC")
            await mark_sent(e["id"], chat_id, "24h")
    for dt, e in events_in_range(0.5, 1.5):
        subs = await all_subscribers()
        for chat_id, n24, n1, ns, daily in subs:
            if not n1:
                continue
            if await already_sent(e["id"], chat_id, "1h"):
                continue
            await safe_send(chat_id, f"🚨 Через 1 час: {e['title']} ({e['track']})\nСтарт: {e['start_utc']} UTC")
            await mark_sent(e["id"], chat_id, "1h")


# ---------- Web ----------
async def health(request):
    return web.Response(text="ok")


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    await init_db()
    # initial fetch
    await update_rc_cache()

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

    scheduler.add_job(daily_digest, "cron", hour=9, minute=0)
    scheduler.add_job(check_upcoming, "interval", minutes=15)
    scheduler.add_job(update_rc_cache, "interval", hours=6)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())