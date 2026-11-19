import os
import json
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))
SCHEDULE_PATH = Path(__file__).parent / "data" / "schedule.json"
DB_PATH = Path(__file__).parent / "data" / "bot.db"

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="UTC")


# ---------- Data ----------
def load_events():
    with open(SCHEDULE_PATH, encoding="utf-8") as f:
        return json.load(f)


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


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
        "Привет! Я бот расписания Le Mans Ultimate.\n\n"
        "Команды:\n"
        "/next — следующее событие\n"
        "/schedule — ближайшие 7 дней\n"
        "/subscribe — подписаться на уведомления\n"
        "/unsubscribe — отписаться\n"
        "/help — справка"
    )


@dp.message(Command("next"))
async def cmd_next(msg: types.Message):
    e, dt = next_event()
    if not e:
        await msg.answer("Событий не найдено.")
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
        await msg.answer("На ближайшие 7 дней событий нет.")
        return
    lines = ["📅 Ближайшие события:"]
    for dt, e in events:
        lines.append(f"• {dt.strftime('%d.%m %H:%M')} UTC — {e['title']} ({e['track']})")
    await msg.answer("\n".join(lines))


@dp.message(Command("subscribe"))
async def cmd_subscribe(msg: types.Message):
    await subscribe(msg.chat.id)
    await msg.answer(
        "✅ Вы подписаны на уведомления!\n"
        "Буду присылать:\n"
        "• ежедневный дайджест в 09:00 UTC\n"
        "• за 24 часа до старта\n"
        "• за 1 час до старта\n\n"
        "Отписаться: /unsubscribe"
    )


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(msg: types.Message):
    await unsubscribe(msg.chat.id)
    await msg.answer("❌ Вы отписаны от уведомлений.")


@dp.message(Command("help"))
async def cmd_help(msg: types.Message):
    await msg.answer(
        "Бот расписания LMU.\n"
        "/next — следующее событие\n"
        "/schedule — ближайшие 7 дней\n"
        "/subscribe — подписаться на уведомления\n"
        "/unsubscribe — отписаться"
    )


# ---------- Notifications ----------
async def safe_send(chat_id, text):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        # пользователь заблокировал бота — удаляем
        await unsubscribe(chat_id)


async def daily_digest():
    today = today_events()
    tomorrow = tomorrow_events()
    subs = await all_subscribers()
    if not subs:
        return
    lines = ["📅 Дайджест расписания LMU"]
    if today:
        lines.append("\n🏁 Сегодня:")
        for dt, e in today:
            lines.append(f"• {dt.strftime('%H:%M')} UTC — {e['title']} ({e['track']})")
    if tomorrow:
        lines.append("\n📆 Завтра:")
        for dt, e in tomorrow:
            lines.append(f"• {dt.strftime('%H:%M')} UTC — {e['title']} ({e['track']})")
    if not today and not tomorrow:
        lines.append("\nНа сегодня и завтра событий нет.")
    text = "\n".join(lines)
    for chat_id, n24, n1, ns, daily in subs:
        if daily:
            await safe_send(chat_id, text)


async def check_upcoming():
    # 24h notifications
    for dt, e in events_in_range(23, 25):
        subs = await all_subscribers()
        for chat_id, n24, n1, ns, daily in subs:
            if not n24:
                continue
            if await already_sent(e["id"], chat_id, "24h"):
                continue
            await safe_send(chat_id, f"⏰ Через 24 часа: {e['title']} ({e['track']})\nСтарт: {e['start_utc']} UTC")
            await mark_sent(e["id"], chat_id, "24h")
    # 1h notifications
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
    await init_db()

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

    # scheduler
    scheduler.add_job(daily_digest, "cron", hour=9, minute=0)
    scheduler.add_job(check_upcoming, "interval", minutes=15)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())