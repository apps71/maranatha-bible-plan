import os
import json
import asyncio
import csv
from datetime import datetime, timedelta, time
import pytz
from telegram import Bot
from telegram.error import TelegramError
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web
import sqlite3

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
GOOGLE_SHEET_GID = os.getenv('GOOGLE_SHEET_GID', '0')
PORT = int(os.getenv('PORT', 8080))

# Timezone для планировщика
TIMEZONE = pytz.timezone('Europe/Moscow')  # UTC+3

# Путь к базе данных
DB_PATH = 'synodal.sqlite'

# =============================================================================
# МАППИНГ КНИГ БИБЛИИ
# =============================================================================
BOOK_NUMBERS = {
    # Ветхий Завет
    'Бытие': 1, 'Исход': 2, 'Левит': 3, 'Числа': 4, 'Второзаконие': 5,
    'Иисус Навин': 6, 'Судьи': 7, 'Руфь': 8, '1 Царств': 9, '2 Царств': 10,
    '3 Царств': 11, '4 Царств': 12, '1 Паралипоменон': 13, '2 Паралипоменон': 14,
    'Ездра': 15, 'Неемия': 16, 'Есфирь': 17, 'Иов': 18, 'Псалтирь': 19,
    'Притчи': 20, 'Екклесиаст': 21, 'Песни Песней': 22, 'Исаия': 23,
    'Иеремия': 24, 'Плач Иеремии': 25, 'Иезекииль': 26, 'Даниил': 27,
    'Осия': 28, 'Иоиль': 29, 'Амос': 30, 'Авдий': 31, 'Иона': 32,
    'Михей': 33, 'Наум': 34, 'Аввакум': 35, 'Софония': 36, 'Аггей': 37,
    'Захария': 38, 'Малахия': 39,
    # Новый Завет
    'Матфей': 40, 'Марк': 41, 'Лука': 42, 'Иоанн': 43, 'Деяния': 44,
    'Иакова': 45, '1 Петра': 46, '2 Петра': 47, '1 Иоанна': 48, '2 Иоанна': 49,
    '3 Иоанна': 50, 'Иуда': 51, 'Римлянам': 52, '1 Коринфянам': 53,
    '2 Коринфянам': 54, 'Галатам': 55, 'Ефесянам': 56, 'Филиппийцам': 57,
    'Колоссянам': 58, '1 Фессалоникийцам': 59, '2 Фессалоникийцам': 60,
    '1 Тимофею': 61, '2 Тимофею': 62, 'Титу': 63, 'Филимону': 64,
    'Евреям': 65, 'Откровение': 66
}

# =============================================================================
# ШАБЛОН СООБЩЕНИЯ
# =============================================================================
MESSAGE_TEMPLATE_COMBINED = """<i>{date_formatted}</i>

🚀 <b>ДЕТЯМ И ПОДРОСТКАМ 3-15 ЛЕТ</b>

{ref_3_15}

❤️ {verse_text_3_15}

<b>Основная мысль урока</b> (можно подчеркнуть при рассуждении над текстом Библии):

✅ {main_point_3_15}

<b>Прочитать текст урока, дети 3-15 лет:</b>
{lesson_url_3_15}


🧸 <b>ДЕТЯМ ОТ 0 ДО 3 ЛЕТ</b>

{ref_0_3}

❤️ {verse_text_0_3}
({note_0_3})

<b>Основная мысль урока</b> (можно подчеркнуть при рассуждении над текстом Библии):

✅ {main_point_0_3}

<b>Прочитать текст урока, дети 0-3 лет:</b>
{lesson_url_0_3}"""

# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С БИБЛИЕЙ
# =============================================================================

def parse_bible_ref(ref):
    """
    Парсит библейскую ссылку типа 'Исход 3:4' или 'Псалтирь 22:1-3'
    Возвращает: (book_number, chapter, verse_start, verse_end)
    """
    try:
        # Разделяем книгу и главу:стих
        parts = ref.split()
        
        # Обрабатываем случаи с номером книги (например, "1 Царств")
        if parts[0].isdigit():
            book_name = f"{parts[0]} {parts[1]}"
            chapter_verse = parts[2]
        else:
            book_name = parts[0]
            chapter_verse = parts[1]
        
        # Получаем номер книги
        book_number = BOOK_NUMBERS.get(book_name)
        if not book_number:
            print(f"⚠️ Книга '{book_name}' не найдена в маппинге", flush=True)
            return None
        
        # Парсим главу и стихи
        chapter_verse_parts = chapter_verse.split(':')
        chapter = int(chapter_verse_parts[0])
        
        # Обрабатываем диапазон стихов (например, "1-3")
        if '-' in chapter_verse_parts[1]:
            verse_parts = chapter_verse_parts[1].split('-')
            verse_start = int(verse_parts[0])
            verse_end = int(verse_parts[1])
        else:
            verse_start = verse_end = int(chapter_verse_parts[1])
        
        return (book_number, chapter, verse_start, verse_end)
    
    except Exception as e:
        print(f"❌ Ошибка парсинга ссылки '{ref}': {e}", flush=True)
        return None


def get_verse_from_db(ref):
    """
    Получает текст стиха из SQLite базы данных по ссылке
    """
    parsed = parse_bible_ref(ref)
    if not parsed:
        return f"[Не удалось найти текст для {ref}]"
    
    book_number, chapter, verse_start, verse_end = parsed
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем стихи
        if verse_start == verse_end:
            # Один стих
            cursor.execute("""
                SELECT text FROM verses 
                WHERE book = ? AND chapter = ? AND verse = ?
            """, (book_number, chapter, verse_start))
            result = cursor.fetchone()
            verse_text = result[0] if result else f"[Стих не найден: {ref}]"
        else:
            # Диапазон стихов
            cursor.execute("""
                SELECT text FROM verses 
                WHERE book = ? AND chapter = ? AND verse BETWEEN ? AND ?
                ORDER BY verse
            """, (book_number, chapter, verse_start, verse_end))
            results = cursor.fetchall()
            verse_text = ' '.join([row[0] for row in results]) if results else f"[Стихи не найдены: {ref}]"
        
        conn.close()
        return verse_text
    
    except Exception as e:
        print(f"❌ Ошибка чтения из БД для '{ref}': {e}", flush=True)
        return f"[Ошибка получения текста для {ref}]"


# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С GOOGLE SHEETS
# =============================================================================

async def load_google_sheet_data():
    """
    Загружает данные из Google Sheets (публичная таблица через CSV export)
    """
    csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GOOGLE_SHEET_GID}"
    
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(csv_url, timeout=60.0)
            response.raise_for_status()
            
            # Парсим CSV
            lines = response.text.splitlines()
            reader = csv.DictReader(lines)
            rows = list(reader)
            
            if not rows:
                print("⚠️ Google Sheets пустая или недоступна", flush=True)
                return None
            
            # Приоритет 1: Ищем неделю по дате
            today = datetime.now(TIMEZONE).date()
            for row in rows:
                start_date_str = row.get('start_date', '').strip()
                if not start_date_str:
                    continue
                
                try:
                    # Парсим дату формата DD.MM.YYYY
                    start_date = datetime.strptime(start_date_str, '%d.%m.%Y').date()
                    end_date = start_date + timedelta(days=6)
                    
                    if start_date <= today <= end_date:
                        print(f"✅ Нашли неделю по дате: {start_date_str} (сегодня {today})", flush=True)
                        return row
                except ValueError:
                    print(f"⚠️ Неверный формат даты: {start_date_str}", flush=True)
                    continue
            
            # Приоритет 2: Если по дате не нашли, ищем status=active
            print(f"⚠️ Неделя по дате не найдена (сегодня {today}), ищем status=active", flush=True)
            for row in rows:
                if row.get('status', '').strip().lower() == 'active':
                    print(f"✅ Нашли неделю по status=active", flush=True)
                    return row
            
            print("❌ Не найдено ни одной недели (ни по дате, ни по status=active)", flush=True)
            return None
    
    except Exception as e:
        print(f"❌ Ошибка загрузки Google Sheets: {e}", flush=True)
        return None


def generate_messages_from_data(week_data):
    """
    Генерирует 7 сообщений на основе данных недели.
    Каждое сообщение содержит данные для двух возрастных групп: 0-3 и 3-15 лет.
    """
    try:
        # Парсим start_date
        start_date_str = week_data.get('start_date', '').strip()
        start_date = datetime.strptime(start_date_str, '%d.%m.%Y').date()
        
        # Получаем данные для 0-3 лет
        lesson_url_0_3 = week_data.get('lesson_url_0_3', '').strip()
        main_point_0_3 = week_data.get('main_point_0_3', '').strip()
        days_json_0_3_str = week_data.get('days_json_0_3', '').strip()
        
        # Получаем данные для 3-15 лет
        lesson_url_3_15 = week_data.get('lesson_url_3_15', '').strip()
        main_point_3_15 = week_data.get('main_point_3_15', '').strip()
        days_json_3_15_str = week_data.get('days_json_3_15', '').strip()
        
        # Парсим JSON с днями для обеих групп
        days_data_0_3 = json.loads(days_json_0_3_str)
        days_data_3_15 = json.loads(days_json_3_15_str)
        
        if len(days_data_0_3) != 7:
            print(f"⚠️ В days_json_0_3 должно быть 7 элементов, найдено {len(days_data_0_3)}", flush=True)
            return []
        
        if len(days_data_3_15) != 7:
            print(f"⚠️ В days_json_3_15 должно быть 7 элементов, найдено {len(days_data_3_15)}", flush=True)
            return []
        
        messages = []
        weekdays_ru = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
        
        for day_index in range(7):
            # Вычисляем дату
            current_date = start_date + timedelta(days=day_index)
            date_formatted = f"{current_date.strftime('%d.%m.%Y')} - {weekdays_ru[day_index]}"
            
            # Получаем данные дня для 0-3 лет
            day_data_0_3 = days_data_0_3[day_index]
            ref_0_3 = day_data_0_3.get('ref', '').strip()
            note_0_3 = day_data_0_3.get('note', '').strip()
            verse_text_0_3 = get_verse_from_db(ref_0_3)
            
            # Получаем данные дня для 3-15 лет
            day_data_3_15 = days_data_3_15[day_index]
            ref_3_15 = day_data_3_15.get('ref', '').strip()
            verse_text_3_15 = get_verse_from_db(ref_3_15)
            
            # Форматируем объединённое сообщение
            message = MESSAGE_TEMPLATE_COMBINED.format(
                date_formatted=date_formatted,
                # Данные для 3-15 лет
                ref_3_15=ref_3_15,
                verse_text_3_15=verse_text_3_15,
                main_point_3_15=main_point_3_15,
                lesson_url_3_15=lesson_url_3_15,
                # Данные для 0-3 лет
                ref_0_3=ref_0_3,
                verse_text_0_3=verse_text_0_3,
                note_0_3=note_0_3,
                main_point_0_3=main_point_0_3,
                lesson_url_0_3=lesson_url_0_3
            )
            
            messages.append(message)
        
        return messages
    
    except Exception as e:
        print(f"❌ Ошибка генерации сообщений: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []


# =============================================================================
# TELEGRAM ФУНКЦИИ
# =============================================================================

async def send_telegram_message(bot, chat_id, message):
    """
    Отправляет сообщение в Telegram с HTML форматированием
    """
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML'
        )
        print(f"✅ Сообщение отправлено в Telegram", flush=True)
        return True
    except TelegramError as e:
        print(f"❌ Ошибка отправки в Telegram: {e}", flush=True)
        return False


# =============================================================================
# ОСНОВНАЯ ЗАДАЧА
# =============================================================================

async def daily_job():
    """
    Основная ежедневная задача: загрузить данные, сгенерировать сообщения, отправить
    """
    print("\n" + "="*50, flush=True)
    print(f"🕐 Запуск ежедневной задачи: {datetime.now(TIMEZONE)}", flush=True)
    print("="*50, flush=True)
    
    # Загружаем данные из Google Sheets
    week_data = await load_google_sheet_data()
    if not week_data:
        print("❌ Не удалось загрузить данные недели. Пропускаем отправку.", flush=True)
        return
    
    # Генерируем сообщения
    messages = generate_messages_from_data(week_data)
    if not messages:
        print("❌ Не удалось сгенерировать сообщения. Пропускаем отправку.", flush=True)
        return
    
    # Определяем какой день недели сегодня (0=понедельник, 6=воскресенье)
    today_weekday = datetime.now(TIMEZONE).weekday()
    today_message = messages[today_weekday]
    
    print(f"\n📤 Отправляем сообщение для дня {today_weekday + 1} ({['пн','вт','ср','чт','пт','сб','вс'][today_weekday]}):", flush=True)
    print("-" * 50, flush=True)
    print(today_message[:200] + "...", flush=True)
    print("-" * 50, flush=True)
    
    # Отправляем в Telegram
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    success = await send_telegram_message(bot, TELEGRAM_CHAT_ID, today_message)
    
    if success:
        print("✅ Задача выполнена успешно!", flush=True)
    else:
        print("❌ Задача завершилась с ошибкой", flush=True)


# =============================================================================
# ВЕБ-СЕРВЕР (для Render)
# =============================================================================

async def handle_health(request):
    """Health check endpoint"""
    return web.Response(text="OK")

async def handle_root(request):
    """Root endpoint"""
    return web.Response(text="Telegram Bible Bot is running!")


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """
    Главная функция: запускает веб-сервер, планировщик задач, и keep-alive
    """
    print("\n" + "="*50, flush=True)
    print("🚀 Запуск Telegram Bible Bot", flush=True)
    print("="*50, flush=True)
    
    # Проверка переменных окружения
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not GOOGLE_SHEET_ID:
        print("❌ Отсутствуют обязательные переменные окружения!", flush=True)
        return
    
    # Проверка базы данных
    if not os.path.exists(DB_PATH):
        print(f"❌ База данных не найдена: {DB_PATH}", flush=True)
        return
    
    print(f"✅ База данных найдена: {DB_PATH}", flush=True)
    
    # Создаём веб-приложение
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get('/health', handle_health)
    
    # Запускаем веб-сервер
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"✅ Веб-сервер запущен на порту {PORT}", flush=True)
    
    # Создаём планировщик
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    
    # Добавляем ежедневную задачу в 04:10
    scheduler.add_job(
        daily_job,
        trigger='cron',
        hour=4,
        minute=10,
        id='daily_bible_message'
    )
    
    scheduler.start()
    print(f"✅ Планировщик запущен. Задача будет выполняться каждый день в 04:10 UTC+3", flush=True)
    
    # Опционально: запустить задачу сразу для теста
    await daily_job()
    
    print("\n" + "="*50, flush=True)
    print("✅ Бот полностью запущен и работает!", flush=True)
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
