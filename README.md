# LMU Telegram Bot

Бот расписания событий Le Mans Ultimate.

## Локальный запуск
1. Создайте бота через @BotFather, получите токен.
2. Скопируйте `.env.example` в `.env`, впишите токен.
3. Установите зависимости: `pip install -r requirements.txt`
4. Запустите: `python bot.py`
   - Для локальной разработки используйте polling: замените webhook на `dp.start_polling(bot)`.

## Деплой на Render
1. Запушьте репозиторий на GitHub.
2. Render → New Web Service → подключите репо.
3. Build: `pip install -r requirements.txt`, Start: `python bot.py`
4. Environment: `BOT_TOKEN`, `WEBHOOK_URL=https://<name>.onrender.com/webhook`
5. Настройте пинг каждые 10 мин через cron-job.org на `/`.

## Обновление расписания
Отредактируйте `data/schedule.json` и сделайте push — Render пересоберёт сервис.