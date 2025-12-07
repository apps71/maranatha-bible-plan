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

# Русские названия книг → номера книг в БД
BOOK_NUMBERS = {
    # Ветхий Завет
    'бытие': 1, 'бытия': 1,
    'исход': 2, 'исхода': 2,
    'левит': 3, 'левита': 3,
    'числа': 4, 'чисел': 4,
    'второзаконие': 5, 'второзакония': 5,
    'иисус навин': 6, 'иисуса навина': 6, 'навин': 6,
    'судьи': 7, 'судей': 7,
    'руфь': 8, 'руфи': 8,
    '1 царств': 9, '1-я царств': 9, '1царств': 9,
    '2 царств': 10, '2-я царств': 10, '2царств': 10,
    '3 царств': 11, '3-я царств': 11, '3царств': 11,
    '4 царств': 12, '4-я царств': 12, '4царств': 12,
    '1 паралипоменон': 13, '1-я паралипоменон': 13,
    '2 паралипоменон': 14, '2-я паралипоменон': 14,
    'ездра': 15, 'ездры': 15,
    'неемия': 16, 'неемии': 16,
    'есфирь': 17, 'есфири': 17,
    'иов': 18, 'иова': 18,
    'псалом': 19, 'псалтирь': 19, 'псалмы': 19, 'псалтырь': 19,
    'притчи': 20, 'притч': 20,
    'екклесиаст': 21, 'екклесиаста': 21,
    'песнь песней': 22, 'песня песней': 22,
    'исаия': 23, 'исаии': 23,
    'иеремия': 24, 'иеремии': 24,
    'плач': 25, 'плач иеремии': 25,
    'иезекииль': 26, 'иезекииля': 26,
    'даниил': 27, 'даниила': 27,
    'осия': 28, 'осии': 28,
    'иоиль': 29, 'иоиля': 29,
    'амос': 30, 'амоса': 30,
    'авдий': 31, 'авдия': 31,
    'иона': 32, 'ионы': 32,
    'михей': 33, 'михея': 33,
    'наум': 34, 'наума': 34,
    'аввакум': 35, 'аввакума': 35,
    'софония': 36, 'софонии': 36,
    'аггей': 37, 'аггея': 37,
    'захария': 38, 'захарии': 38,
    'малахия': 39, 'малахии': 39,
    
    # Новый Завет
    'матфей': 40, 'матфея': 40, 'от матфея': 40,
    'марк': 41, 'марка': 41, 'от марка': 41,
    'лука': 42, 'луки': 42, 'от луки': 42,
    'иоанн': 43, 'иоанна': 43, 'от иоанна': 43,
    'деяния': 44, 'деяний': 44,
    'римлянам': 45, 'к римлянам': 45,
    '1 коринфянам': 46, 'к 1 коринфянам': 46,
    '2 коринфянам': 47, 'к 2 коринфянам': 47,
    'галатам': 48, 'к галатам': 48,
    'ефесянам': 49, 'к ефесянам': 49,
    'филиппийцам': 50, 'к филиппийцам': 50,
    'колоссянам': 51, 'к колоссянам': 51,
    '1 фессалоникийцам': 52, 'к 1 фессалоникийцам': 52,
    '2 фессалоникийцам': 53, 'к 2 фессалоникийцам': 53,
    '1 тимофею': 54, 'к 1 тимофею': 54,
    '2 тимофею': 55, 'к 2 тимофею': 55,
    'титу': 56, 'к титу': 56,
    'филимону': 57, 'к филимону': 57,
    'евреям': 58, 'к евреям': 58,
    'иакова': 59, 'послание иакова': 59,
    '1 петра': 60, 'первое петра': 60,
    '2 петра': 61, 'второе петра': 61,
    '1 иоанна': 62, 'первое иоанна': 62,
    '2 иоанна': 63, 'второе иоанна': 63,
    '3 иоанна': 64, 'третье иоанна': 64,
    'иуды': 65, 'послание иуды': 65,
    'откровение': 66, 'откровения': 66, 'апокалипсис': 66,
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
    """Парсинг ссылки на стих Библии"""
    ref = ref.strip()
    match = re.match(r'^(.+?)\s+(\d+):(\d+)(?:-(\d+))?$', ref)
    if not match:
        print(f"⚠️ Не удалось распарсить ссылку: {ref}", flush=True)
        return None
    book_name = match.group(1).strip().lower()
    chapter = int(match.group(2))
    verse_start = int(match.group(3))
    verse_end = int(match.group(4)) if match.group(4) else verse_start
    book_number = BOOK_NUMBERS.get(book_name)
    if not book_number:
        print(f"⚠️ Неизвестная книга: {book_name}", flush=True)
        for key, value in BOOK_NUMBERS.items():
            if book_name in key or key in book_name:
                book_number = value
                print(f"✅ Найдено частичное совпадение: {book_name} → {book_number}", flush=True)
                break
    if not book_number:
        return None
    return (book_number, chapter, verse_start, verse_end)

def get_verse_from_db(ref):
    """
    Получение текста стиха из SQLite базы данных
    """
    try:
        parsed = parse_bible_ref(ref)
        if not parsed:
            return None
        
        book_number, chapter, verse_start, verse_end = parsed
        
        # Проверяем наличие БД
        if not os.path.exists(DB_PATH):
            print(f"❌ База данных не найдена: {DB_PATH}", flush=True)
            return None
        
        # Подключаемся к БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Запрос к таблице verses
        query = """
            SELECT text FROM verses 
            WHERE book = ? AND chapter = ? AND verse BETWEEN ? AND ?
            ORDER BY verse
        """
        
        cursor.execute(query, (book_number, chapter, verse_start, verse_end))
        results = cursor.fetchall()
        
        conn.close()
        
        if not results:
            print(f"⚠️ Стих не найден в БД: {ref} (book={book_number}, ch={chapter}, v={verse_start})", flush=True)
            return None
        
        # Объединяем стихи (если диапазон)
        verse_text = ' '.join([row[0] for row in results])
        
        print(f"✅ Найден стих: {ref} ({len(verse_text)} символов)", flush=True)
        return verse_text
        
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
        
        if not match:
            print(f"⚠️ Не удалось распарсить ссылку: {ref}", flush=True)
            return None
        
        book_name = match.group(1).strip().lower()
        chapter = int(match.group(2))
        verse_start = int(match.group(3))
        verse_end = int(match.group(4)) if match.group(4) else verse_start
        
        # Ищем номер книги в маппинге
        book_number = BOOK_NUMBERS.get(book_name)
        
        if not book_number:
            print(f"⚠️ Неизвестная книга: {book_name}", flush=True)
            # Пробуем найти частичное совпадение
            for key, value in BOOK_NUMBERS.items():
                if book_name in key or key in book_name:
                    book_number = value
                    print(f"✅ Найдено частичное совпадение: {book_name} → {book_number}", flush=True)
                    break
        
        if not book_number:
            return None
        
        return (book_number, chapter, verse_start, verse_end)
        
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
