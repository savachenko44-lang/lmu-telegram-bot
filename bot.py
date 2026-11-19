import os
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", "10000"))
SCHEDULE_PATH = Path(__file__).parent / "data" / "schedule.json"

bot = Bot(token=TOKEN)
dp = Dispatcher()


def load_events():
    with open(SCHEDULE_PATH, encoding="utf-8") as f:
        return json.load(f)


def next_event():
    now = datetime.now(timezone.utc)
    events = sorted(load_events(), key=lambda e: e["start_utc"])
    for e in events:
        dt = datetime.fromisoformat(e["start_utc"].replace("Z", "+00:00"))
        if dt > now:
            return e, dt
    return None, None


@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer(
        "Привет! Я бот расписания Le Mans Ultimate.\n\n"
        "Команды:\n"
        "/next — следующее событие\n"
        "/schedule — ближайшие 7 дней\n"
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
        dt = datetime.fromisoformat(e["start_utc"].replace("Z", "+00:00"))
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


@dp.message(Command("help"))
async def cmd_help(msg: types.Message):
    await msg.answer("Бот расписания LMU. /next, /schedule, /start.")


async def health(request):
    return web.Response(text="ok")


async def on_startup(app):
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)


async def main():
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    app.router.add_get("/", health)
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    asyncio.run(main())