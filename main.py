import os
import json
import asyncio
import csv
import sqlite3
import re
from io import StringIO
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from telegram.error import TelegramError
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_ID).strip().strip('"').strip("'")
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
    except ValueError:
        print(f"⚠️ Неправильный формат TELEGRAM_CHAT_ID")

GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
GOOGLE_SHEET_GID = os.getenv('GOOGLE_SHEET_GID', '0')
PORT = int(os.getenv('PORT', 10000))
TIMEZONE = pytz.timezone('Europe/Moscow')
DB_PATH = 'synodal.sqlite'

BOOK_NUMBERS = {'бытие': 1, 'бытия': 1, 'исход': 2, 'исхода': 2, 'левит': 3, 'левита': 3, 'числа': 4, 'чисел': 4, 'второзаконие': 5, 'второзакония': 5, 'иисус навин': 6, 'иисуса навина': 6, 'навин': 6, 'судьи': 7, 'судей': 7, 'руфь': 8, 'руфи': 8, '1 царств': 9, '2 царств': 10, '3 царств': 11, '4 царств': 12, '1 паралипоменон': 13, '2 паралипоменон': 14, 'ездра': 15, 'ездры': 15, 'неемия': 16, 'неемии': 16, 'есфирь': 17, 'есфири': 17, 'иов': 18, 'иова': 18, 'псалом': 19, 'псалтирь': 19, 'псалмы': 19, 'екклесиаст': 21, 'екклесиаста': 21, 'песнь песней': 22, 'исаия': 23, 'исаии': 23, 'иеремия': 24, 'иеремии': 24, 'плач': 25, 'иезекииль': 26, 'иезекииля': 26, 'даниил': 27, 'даниила': 27, 'осия': 28, 'осии': 28, 'иоиль': 29, 'иоиля': 29, 'амос': 30, 'амоса': 30, 'авдий': 31, 'авдия': 31, 'иона': 32, 'ионы': 32, 'михей': 33, 'михея': 33, 'наум': 34, 'наума': 34, 'аввакум': 35, 'аввакума': 35, 'софония': 36, 'софонии': 36, 'аггей': 37, 'аггея': 37, 'захария': 38, 'захарии': 38, 'малахия': 39, 'малахии': 39, 'матфей': 40, 'матфея': 40, 'от матфея': 40, 'марк': 41, 'марка': 41, 'от марка': 41, 'лука': 42, 'луки': 42, 'от луки': 42, 'иоанн': 43, 'иоанна': 43, 'от иоанна': 43, 'деяния': 44, 'деяний': 44, 'римлянам': 45, '1 коринфянам': 46, '2 коринфянам': 47, 'галатам': 48, 'ефесянам': 49, 'филиппийцам': 50, 'колоссянам': 51, '1 фессалоникийцам': 52, '2 фессалоникийцам': 53, '1 тимофею': 54, '2 тимофею': 55, 'титу': 56, 'филимону': 57, 'евреям': 58, 'иакова': 59, '1 петра': 60, '2 петра': 61, '1 иоанна': 62, '2 иоанна': 63, '3 иоанна': 64, 'иуды': 65, 'откровение': 66, 'откровения': 66, 'апокалипсис': 66}

MESSAGE_TEMPLATE = """🧸 Детям от 0 до 3 лет

{date_formatted}

<b>{ref}</b>

❤️ {verse_text}
({note})

<b>Основная мысль урока</b> (можно подчеркнуть при рассуждении над текстом Библии):

✅ {main_point}

<b>Прочитать текст урока:</b>
{lesson_url}"""

async def health_check(request):
    return web.Response(text="Bible Bot is running! ✅")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"🌐 Веб-сервер запущен на порту {PORT}", flush=True)
    return runner

def parse_bible_ref(ref):
    ref = ref.strip()
    match = re.match(r'^(.+?)\s+(\d+):(\d+)(?:-(\d+))?$', ref)
    if not match:
        return None
    book_name = match.group(1).strip().lower()
    chapter = int(match.group(2))
    verse_start = int(match.group(3))
    verse_end = int(match.group(4)) if match.group(4) else verse_start
    book_number = BOOK_NUMBERS.get(book_name)
    if not book_number:
        for key, value in BOOK_NUMBERS.items():
            if book_name in key or key in book_name:
                book_number = value
                break
    return (book_number, chapter, verse_start, verse_end) if book_number else None

def get_verse_from_db(ref):
    try:
        parsed = parse_bible_ref(ref)
        if not parsed:
            return None
        book_number, chapter, verse_start, verse_end = parsed
        if not os.path.exists(DB_PATH):
            print(f"❌ БД не найдена: {DB_PATH}", flush=True)
            return None
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        query = "SELECT text FROM verses WHERE book = ? AND chapter = ? AND verse BETWEEN ? AND ? ORDER BY verse"
        cursor.execute(query, (book_number, chapter, verse_start, verse_end))
        results = cursor.fetchall()
        conn.close()
        if not results:
            print(f"⚠️ Стих не найден: {ref}", flush=True)
            return None
        verse_text = ' '.join([row[0] for row in results])
        print(f"✅ Найден стих: {ref}", flush=True)
        return verse_text
    except Exception as e:
        print(f"❌ Ошибка БД для '{ref}': {e}", flush=True)
        return None

async def load_google_sheet_data():
    try:
        csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GOOGLE_SHEET_GID}"
        print(f"📊 Загрузка данных...", flush=True)
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(csv_url)
            response.raise_for_status()
            csv_reader = csv.DictReader(StringIO(response.text))
            for row in csv_reader:
                if row.get('status', '').strip() == 'active':
                    print(f"✅ Найдена активная неделя", flush=True)
                    return parse_week_data(row)
        return None
    except Exception as e:
        print(f"❌ Ошибка чтения таблицы: {e}", flush=True)
        return None

def parse_week_data(row):
    try:
        days_json_str = row.get('days_json', '').strip()
        if not days_json_str:
            return None
        days_data = json.loads(days_json_str)
        return {'start_date': row.get('start_date', ''), 'lesson_url': row.get('lesson_url', ''), 'main_point': row.get('main_point', ''), 'days': days_data}
    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}", flush=True)
        return None

def generate_messages_from_data(week_data):
    try:
        messages = []
        start_date = datetime.strptime(week_data['start_date'], '%d.%m.%Y')
        months = {1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'}
        weekdays = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
        for i, day_data in enumerate(week_data['days']):
            current_date = start_date + timedelta(days=i)
            date_formatted = f"{current_date.day} {months[current_date.month]} - {weekdays[current_date.weekday()]}"
            ref = day_data.get('ref', '')
            verse_text = get_verse_from_db(ref) or "[ТЕКСТ НЕ НАЙДЕН]"
            message = MESSAGE_TEMPLATE.format(date_formatted=date_formatted, ref=ref, verse_text=verse_text, note=day_data.get('note', ''), main_point=week_data['main_point'], lesson_url=week_data['lesson_url'])
            messages.append(message)
        return messages
    except Exception as e:
        print(f"❌ Ошибка генерации: {e}", flush=True)
        return None

async def send_telegram_message(message_text):
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message_text, parse_mode='HTML')
        print(f"✅ Сообщение отправлено", flush=True)
    except TelegramError as e:
        print(f"❌ Ошибка Telegram: {e}", flush=True)

async def daily_job():
    print(f"\n🔄 Запуск задачи", flush=True)
    week_data = await load_google_sheet_data()
    if not week_data:
        return
    messages = generate_messages_from_data(week_data)
    if not messages:
        return
    current_weekday = datetime.now(TIMEZONE).weekday()
    if current_weekday < len(messages):
        await send_telegram_message(messages[current_weekday])

async def main():
    print("🚀 ЗАПУСК БОТА", flush=True)
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_ID]):
        print("❌ Не все переменные установлены", flush=True)
        return
    runner = await start_web_server()
    await asyncio.sleep(3)
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(daily_job, 'cron', hour=5, minute=15)
    scheduler.start()
    print("✅ Планировщик запущен", flush=True)
    #print("\n🧪 Тест...", flush=True)
    #await daily_job()
    print("\n🎉 Бот работает!", flush=True)
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        await runner.cleanup()

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    asyncio.run(main())
