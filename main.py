import os
import json
import asyncio
import csv
import sqlite3
import re
from io import StringIO
from datetime import datetime, time, timedelta
import pytz
from telegram import Bot
from telegram.error import TelegramError
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# =============================================================================
# КОНФИГУРАЦИЯ - ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Преобразуем CHAT_ID в правильный формат
if TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_ID).strip().strip('"').strip("'")
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
    except ValueError:
        print(f"⚠️ Неправильный формат TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")

GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
GOOGLE_SHEET_GID = os.getenv('GOOGLE_SHEET_GID', '0')
PORT = int(os.getenv('PORT', 10000))

# Часовой пояс
TIMEZONE = pytz.timezone('Europe/Moscow')  # UTC+3

# Путь к SQLite базе данных
DB_PATH = 'synodal.sqlite'

# =============================================================================
# МАППИНГ НАЗВАНИЙ КНИГ БИБЛИИ
# =============================================================================

# Русские названия книг → английские abbreviations для БД
BOOK_NAMES = {
    # Ветхий Завет
    'бытие': 'Gen', 'бытия': 'Gen',
    'исход': 'Exod', 'исхода': 'Exod',
    'левит': 'Lev', 'левита': 'Lev',
    'числа': 'Num', 'чисел': 'Num',
    'второзаконие': 'Deut', 'второзакония': 'Deut',
    'иисус навин': 'Josh', 'иисуса навина': 'Josh',
    'судьи': 'Judg', 'судей': 'Judg',
    'руфь': 'Ruth', 'руфи': 'Ruth',
    '1 царств': '1Sam', '1-я царств': '1Sam', '1царств': '1Sam',
    '2 царств': '2Sam', '2-я царств': '2Sam', '2царств': '2Sam',
    '3 царств': '1Kgs', '3-я царств': '1Kgs', '3царств': '1Kgs',
    '4 царств': '2Kgs', '4-я царств': '2Kgs', '4царств': '2Kgs',
    '1 паралипоменон': '1Chr', '1-я паралипоменон': '1Chr',
    '2 паралипоменон': '2Chr', '2-я паралипоменон': '2Chr',
    'ездра': 'Ezra', 'ездры': 'Ezra',
    'неемия': 'Neh', 'неемии': 'Neh',
    'есфирь': 'Esth', 'есфири': 'Esth',
    'иов': 'Job', 'иова': 'Job',
    'псалом': 'Ps', 'псалтирь': 'Ps', 'псалмы': 'Ps', 'псалтырь': 'Ps',
    'притчи': 'Prov', 'притч': 'Prov',
    'екклесиаст': 'Eccl', 'екклесиаста': 'Eccl',
    'песнь песней': 'Song',
    'исаия': 'Isa', 'исаии': 'Isa',
    'иеремия': 'Jer', 'иеремии': 'Jer',
    'плач': 'Lam', 'плач иеремии': 'Lam',
    'иезекииль': 'Ezek', 'иезекииля': 'Ezek',
    'даниил': 'Dan', 'даниила': 'Dan',
    'осия': 'Hos', 'осии': 'Hos',
    'иоиль': 'Joel', 'иоиля': 'Joel',
    'амос': 'Amos', 'амоса': 'Amos',
    'авдий': 'Obad', 'авдия': 'Obad',
    'иона': 'Jonah', 'ионы': 'Jonah',
    'михей': 'Mic', 'михея': 'Mic',
    'наум': 'Nah', 'наума': 'Nah',
    'аввакум': 'Hab', 'аввакума': 'Hab',
    'софония': 'Zeph', 'софонии': 'Zeph',
    'аггей': 'Hag', 'аггея': 'Hag',
    'захария': 'Zech', 'захарии': 'Zech',
    'малахия': 'Mal', 'малахии': 'Mal',
    
    # Новый Завет
    'матфей': 'Matt', 'матфея': 'Matt', 'от матфея': 'Matt',
    'марк': 'Mark', 'марка': 'Mark', 'от марка': 'Mark',
    'лука': 'Luke', 'луки': 'Luke', 'от луки': 'Luke',
    'иоанн': 'John', 'иоанна': 'John', 'от иоанна': 'John',
    'деяния': 'Acts', 'деяний': 'Acts',
    'римлянам': 'Rom', 'к римлянам': 'Rom',
    '1 коринфянам': '1Cor', 'к 1 коринфянам': '1Cor',
    '2 коринфянам': '2Cor', 'к 2 коринфянам': '2Cor',
    'галатам': 'Gal', 'к галатам': 'Gal',
    'ефесянам': 'Eph', 'к ефесянам': 'Eph',
    'филиппийцам': 'Phil', 'к филиппийцам': 'Phil',
    'колоссянам': 'Col', 'к колоссянам': 'Col',
    '1 фессалоникийцам': '1Thess', 'к 1 фессалоникийцам': '1Thess',
    '2 фессалоникийцам': '2Thess', 'к 2 фессалоникийцам': '2Thess',
    '1 тимофею': '1Tim', 'к 1 тимофею': '1Tim',
    '2 тимофею': '2Tim', 'к 2 тимофею': '2Tim',
    'титу': 'Titus', 'к титу': 'Titus',
    'филимону': 'Phlm', 'к филимону': 'Phlm',
    'евреям': 'Heb', 'к евреям': 'Heb',
    'иакова': 'Jas', 'послание иакова': 'Jas',
    '1 петра': '1Pet', 'первое петра': '1Pet',
    '2 петра': '2Pet', 'второе петра': '2Pet',
    '1 иоанна': '1John', 'первое иоанна': '1John',
    '2 иоанна': '2John', 'второе иоанна': '2John',
    '3 иоанна': '3John', 'третье иоанна': '3John',
    'иуды': 'Jude', 'послание иуды': 'Jude',
    'откровение': 'Rev', 'откровения': 'Rev', 'апокалипсис': 'Rev',
}

# =============================================================================
# ПРОМПТ-ШАБЛОН ДЛЯ ФОРМАТИРОВАНИЯ СООБЩЕНИЯ
# =============================================================================

MESSAGE_TEMPLATE = """🧸 Детям от 0 до 3 лет

{date_formatted}

**{ref}**

❤️ {verse_text}
({note})

**Основная мысль урока** (можно подчеркнуть при рассуждении над текстом Библии):

✅ {main_point}

**Прочитать текст урока:**
{lesson_url}"""

# =============================================================================
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# =============================================================================

async def health_check(request):
    """Простой endpoint для проверки работы сервиса"""
    return web.Response(text="Bible Bot is running! ✅")

async def start_web_server():
    """Запуск веб-сервера для Render"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"🌐 Веб-сервер запущен на порту {PORT}", flush=True)
    return runner

# =============================================================================
# ФУНКЦИИ РАБОТЫ С БИБЛИЕЙ
# =============================================================================

def parse_bible_ref(ref):
    """
    Парсинг ссылки на стих Библии
    Примеры: "Исход 3:4", "1 Коринфянам 13:4-7", "Псалом 118:30"
    Возвращает: (book_abbr, chapter, verse_start, verse_end)
    """
    try:
        # Убираем лишние пробелы
        ref = ref.strip()
        
        # Паттерн: "Книга глава:стих" или "Книга глава:стих-стих"
        match = re.match(r'^(.+?)\s+(\d+):(\d+)(?:-(\d+))?$', ref)
        
        if not match:
            print(f"⚠️ Не удалось распарсить ссылку: {ref}", flush=True)
            return None
        
        book_name = match.group(1).strip().lower()
        chapter = int(match.group(2))
        verse_start = int(match.group(3))
        verse_end = int(match.group(4)) if match.group(4) else verse_start
        
        # Ищем книгу в маппинге
        book_abbr = BOOK_NAMES.get(book_name)
        
        if not book_abbr:
            print(f"⚠️ Неизвестная книга: {book_name}", flush=True)
            # Пробуем найти частичное совпадение
            for key, value in BOOK_NAMES.items():
                if book_name in key or key in book_name:
                    book_abbr = value
                    print(f"✅ Найдено частичное совпадение: {book_name} → {book_abbr}", flush=True)
                    break
        
        if not book_abbr:
            return None
        
        return (book_abbr, chapter, verse_start, verse_end)
        
    except Exception as e:
        print(f"❌ Ошибка парсинга ссылки '{ref}': {e}", flush=True)
        return None


def get_verse_from_db(ref):
    """
    Получение текста стиха из SQLite базы данных
    """
    try:
        parsed = parse_bible_ref(ref)
        if not parsed:
            return None
        
        book_abbr, chapter, verse_start, verse_end = parsed
        
        # Проверяем наличие БД
        if not os.path.exists(DB_PATH):
            print(f"❌ База данных не найдена: {DB_PATH}", flush=True)
            return None
        
        # Подключаемся к БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # ОТЛАДКА: Смотрим структуру БД
        print(f"🔍 Отладка БД для {ref}:", flush=True)
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"   Таблицы в БД: {[t[0] for t in tables]}", flush=True)
        
        # Смотрим структуру первой таблицы
        if tables:
            table_name = tables[0][0]
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            print(f"   Колонки в таблице '{table_name}': {[c[1] for c in columns]}", flush=True)
            
            # Пробуем найти стих разными способами
            print(f"   Ищем: book={book_abbr}, chapter={chapter}, verse={verse_start}", flush=True)
            
            # Вариант 1: стандартная структура
            try:
                query1 = f"SELECT * FROM {table_name} WHERE book = ? AND chapter = ? AND verse = ? LIMIT 1"
                cursor.execute(query1, (book_abbr, chapter, verse_start))
                result1 = cursor.fetchone()
                if result1:
                    print(f"   ✅ Найдено через вариант 1: {result1}", flush=True)
            except Exception as e:
                print(f"   ⚠️ Вариант 1 не сработал: {e}", flush=True)
            
            # Вариант 2: может быть другие названия колонок
            try:
                query2 = f"SELECT * FROM {table_name} LIMIT 5"
                cursor.execute(query2)
                sample = cursor.fetchall()
                print(f"   📊 Первые 5 записей таблицы:", flush=True)
                for row in sample[:3]:
                    print(f"      {row}", flush=True)
            except Exception as e:
                print(f"   ⚠️ Не удалось получить примеры: {e}", flush=True)
        
        conn.close()
        return None  # Временно возвращаем None для отладки
        
    except Exception as e:
        print(f"❌ Ошибка чтения из БД для '{ref}': {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


# =============================================================================
# ФУНКЦИИ РАБОТЫ С GOOGLE SHEETS
# =============================================================================

async def load_google_sheet_data():
    """Загрузка данных из публичной Google Sheets"""
    try:
        csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GOOGLE_SHEET_GID}"
        
        print(f"📊 Загрузка данных из Google Sheets...", flush=True)
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(csv_url)
            response.raise_for_status()
            
            print(f"✅ Данные загружены ({len(response.text)} символов)", flush=True)
            
            csv_reader = csv.DictReader(StringIO(response.text))
            fieldnames = csv_reader.fieldnames
            print(f"📋 Найдены колонки: {fieldnames}", flush=True)
            
            for line_num, row in enumerate(csv_reader, start=2):
                status = row.get('status', '').strip()
                print(f"🔍 Строка {line_num}: status = '{status}'", flush=True)
                
                if status == 'active':
                    print(f"✅ Найдена активная неделя!", flush=True)
                    return parse_week_data(row)
            
            print("⚠️ Не найдена активная неделя в таблице", flush=True)
            return None
        
    except Exception as e:
        print(f"❌ Ошибка при чтении Google Sheets: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


def parse_week_data(row):
    """Парсинг данных недели из Google Sheets"""
    try:
        days_json_str = row.get('days_json', '').strip()
        
        if not days_json_str:
            print("❌ Колонка days_json пустая", flush=True)
            return None
        
        print(f"🔍 Парсинг days_json...", flush=True)
        
        try:
            days_data = json.loads(days_json_str)
        except json.JSONDecodeError as e:
            print(f"❌ Невалидный JSON в days_json: {e}", flush=True)
            return None
        
        if not isinstance(days_data, list) or len(days_data) != 7:
            print(f"❌ days_json должен содержать 7 элементов, получено: {len(days_data)}", flush=True)
            return None
        
        week_data = {
            'start_date': row.get('start_date', ''),
            'lesson_url': row.get('lesson_url', ''),
            'main_point': row.get('main_point', ''),
            'days': days_data
        }
        
        print(f"✅ Данные недели успешно распарсены", flush=True)
        return week_data
        
    except Exception as e:
        print(f"❌ Ошибка парсинга данных недели: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


def generate_messages_from_data(week_data):
    """Генерация 7 сообщений из данных недели"""
    try:
        messages = []
        
        # Парсим стартовую дату
        start_date_str = week_data['start_date']
        start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
        
        # Названия месяцев в родительном падеже
        months_genitive = {
            1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
            5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
            9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
        }
        
        # Дни недели
        weekdays = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
        
        for i, day_data in enumerate(week_data['days']):
            current_date = start_date + timedelta(days=i)
            day_num = current_date.day
            month_name = months_genitive[current_date.month]
            weekday = weekdays[current_date.weekday()]
            
            date_formatted = f"{day_num} {month_name} - {weekday}"
            
            # Получаем текст стиха из БД
            ref = day_data.get('ref', '')
            verse_text = get_verse_from_db(ref)
            
            if not verse_text:
                verse_text = "[ТЕКСТ НЕ НАЙДЕН В БД]"
                print(f"⚠️ Текст для {ref} не найден, используется заглушка", flush=True)
            
            # Формируем сообщение по шаблону
            message = MESSAGE_TEMPLATE.format(
                date_formatted=date_formatted,
                ref=ref,
                verse_text=verse_text,
                note=day_data.get('note', ''),
                main_point=week_data['main_point'],
                lesson_url=week_data['lesson_url']
            )
            
            messages.append(message)
        
        print(f"✅ Сгенерировано {len(messages)} сообщений", flush=True)
        return messages
        
    except Exception as e:
        print(f"❌ Ошибка генерации сообщений: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


# =============================================================================
# ФУНКЦИИ ОТПРАВКИ В TELEGRAM
# =============================================================================

async def send_telegram_message(message_text):
    """Отправка сообщения в Telegram"""
    try:
        print(f"📱 Попытка отправки в Telegram...", flush=True)
        print(f"   Chat ID: '{TELEGRAM_CHAT_ID}' (тип: {type(TELEGRAM_CHAT_ID)})", flush=True)
        
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message_text,
            parse_mode='Markdown'
        )
        print(f"✅ Сообщение отправлено в {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        
    except TelegramError as e:
        print(f"❌ Ошибка отправки в Telegram: {e}", flush=True)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}", flush=True)


async def daily_job():
    """Ежедневная задача: отправка одного сообщения"""
    print(f"\n🔄 Запуск ежедневной задачи: {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    
    # Загружаем данные
    week_data = await load_google_sheet_data()
    
    if not week_data:
        print("❌ Нет данных для отправки", flush=True)
        return
    
    # Генерируем сообщения
    messages = generate_messages_from_data(week_data)
    
    if not messages or len(messages) < 7:
        print("❌ Не удалось сгенерировать сообщения", flush=True)
        return
    
    # Определяем день недели (0=Пн, 6=Вс)
    current_weekday = datetime.now(TIMEZONE).weekday()
    
    if current_weekday < len(messages):
        message_to_send = messages[current_weekday]
        await send_telegram_message(message_to_send)
    else:
        print(f"⚠️ Нет сообщения для дня недели: {current_weekday}", flush=True)


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """Главная функция - запуск планировщика и веб-сервера"""
    print("="*50, flush=True)
    print("🚀 ЗАПУСК BIBLE TELEGRAM BOT (версия с SQLite БД)", flush=True)
    print("="*50, flush=True)
    
    # Проверка переменных окружения
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_ID]):
        print("❌ Не все переменные окружения установлены!", flush=True)
        return
    
    # Проверка наличия БД
    if not os.path.exists(DB_PATH):
        print(f"⚠️ ВНИМАНИЕ: База данных {DB_PATH} не найдена!", flush=True)
        print(f"⚠️ Скачайте synodal.sqlite и поместите в корень проекта", flush=True)
    
    # Запускаем веб-сервер
    print(f"\n🌐 Запуск веб-сервера на порту {PORT}...", flush=True)
    runner = await start_web_server()
    print(f"✅ Веб-сервер запущен", flush=True)
    
    await asyncio.sleep(3)
    
    print(f"\n⏰ Настройка отправки сообщений каждый день в 04:10 UTC+3", flush=True)
    
    # Создаём планировщик
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    
    # Добавляем задачу
    scheduler.add_job(
        daily_job,
        'cron',
        hour=4,
        minute=10,
        id='daily_bible_message'
    )
    
    scheduler.start()
    print("✅ Планировщик запущен", flush=True)
    
    # Тестовая отправка (раскомментируйте для теста)
    # print("\n🧪 Запуск тестовой отправки...", flush=True)
    # await daily_job()
    
    print("\n🎉 Бот полностью запущен и работает!", flush=True)
    print("="*50, flush=True)
    
    # Держим программу запущенной
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        print("\n👋 Остановка бота...", flush=True)
        scheduler.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    asyncio.run(main())
