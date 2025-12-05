import os
import json
import asyncio
import csv
from io import StringIO
from datetime import datetime, time
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
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
OPENROUTER_MODEL = os.getenv('OPENROUTER_MODEL', 'anthropic/claude-3.5-sonnet')  # По умолчанию Claude 3.5 Sonnet
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')  # ID публичной Google таблицы
GOOGLE_SHEET_GID = os.getenv('GOOGLE_SHEET_GID', '0')  # GID листа (по умолчанию 0)
PORT = int(os.getenv('PORT', 10000))  # Порт для Render

# Часовой пояс
TIMEZONE = pytz.timezone('Europe/Moscow')  # UTC+3

# =============================================================================
# ПРОМПТ ДЛЯ CLAUDE
# =============================================================================
PROMPT_TEMPLATE = """Вы — помощник редактора детской библейской рассылки (0–3). Ваша задача — сформировать 7 ежедневных сообщений для Telegram строго по шаблону, в Markdown, с пустыми строками между абзацами, без отклонений и без лишнего текста.

КРИТИЧЕСКИЕ ПРАВИЛА (ОБЯЗАТЕЛЬНЫ К ВЫПОЛНЕНИЮ)

- Строгий формат каждого сообщения (отступы не менять, используй HTML разметку для жирного):
  🧸 Детям от 0 до 3 лет

  {{ДД месяц(в родительном падеже, строчными) - {{день недели (строчными)}}}}

  <b>{{Книга}} {{глава}}:{{стихи}}</b>

  ❤️ {{Текст стиха}}
  ({{Пояснение из входных данных}})

  <b>Основная мысль урока</b> (можно подчеркнуть при рассуждении над текстом Библии):

  ✅ {{Основная мысль (одна и та же для всех 7 дней)}}

  <b>Прочитать текст урока:</b>
  {{Ссылка}}

  - Между каждой из указанных логических частей должна быть ровно одна пустая строка, как в шаблоне выше.
  - Не добавляйте/не убирайте ни одной строки, символа форматирования и эмодзи.

- Вывод:
  - Ровно 7 отдельных код-блоков (```), по одному на каждый день, без какого-либо текста вне блоков.
  - Внутри каждого блока используйте Markdown (не HTML).
  - Не склеивайте строки; сохраняйте пустые строки из шаблона.

- Даты:
  - Вход даёт стартовую дату ДД.MM.ГГГГ. Сгенерируйте 7 последовательных дат.
  - Дни недели: понедельник, вторник, среда, четверг, пятница, суббота, воскресенье (строчными).
  - Названия месяцев в родительном падеже, строчными: января, февраля, марта, апреля, мая, июня, июля, августа, сентября, октября, ноября, декабря.

- Цитата из Библии (Синодальный перевод, БЕЗ ПЕРЕФРАЗИРОВАНИЯ):
  - Источник текста: Синодальный русский перевод.
  - Вставляйте ПОЛНЫЙ точный текст стиха (без номеров стихов, без HTML/лишней разметки).
  - Нельзя перефразировать, сокращать, «улучшать» или цитировать лишь часть, если стих — одно предложение в переводе, вставляйте его целиком.
  - Строгое требование: если вы НЕ уверены на 100% в точной формулировке Синодального перевода по данной ссылке ref И у вас нет поля verse_text в INPUT — НЕ подставляйте приблизительный текст. Вместо этого:
    • Вставьте строку-заглушку: «[ТРЕБУЕТСЯ ТОЧНЫЙ ТЕКСТ СИН.]»
    • И выведите стих полностью корректно только если уверены.
  - Если в INPUT есть поле verse_text — используйте его как единственный допустимый текст стиха (не изменяйте, не редактируйте).

- Пояснение (note):
  - Строка в круглых скобках сразу после стиха. Использовать как дано, без изменений.

- Основная мысль (main_point):
  - Одна и та же на все 7 дней. Вставлять дословно.

- Ссылка на урок:
  - Используйте lesson_url. Убедитесь, что «deti03» — латиницей.

- Язык вывода — русский. Никаких комментариев, предупреждений или пояснений вне 7 код-блоков.

INPUT:
{input_data}
"""

# =============================================================================
# ПРОСТОЙ ВЕБ-СЕРВЕР ДЛЯ RENDER
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
    print(f"🌐 Веб-сервер запущен на порту {PORT}")
    return runner

# =============================================================================
# ФУНКЦИИ
# =============================================================================

async def load_google_sheet_data():
    """Загрузка данных из публичной Google Sheets через CSV экспорт"""
    try:
        # URL для экспорта Google Sheets в CSV формате
        csv_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={GOOGLE_SHEET_GID}"
        
        print(f"📊 Загрузка данных из Google Sheets...", flush=True)
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(csv_url)
            response.raise_for_status()
            
            print(f"✅ Данные загружены ({len(response.text)} символов)", flush=True)
            
            # Используем стандартный CSV парсер Python (правильно обрабатывает запятые внутри полей)
            csv_reader = csv.DictReader(StringIO(response.text))
            
            # Получаем заголовки
            fieldnames = csv_reader.fieldnames
            print(f"📋 Найдены колонки: {fieldnames}", flush=True)
            
            # Ищем активную неделю
            for line_num, row in enumerate(csv_reader, start=2):
                status = row.get('status', '').strip()
                print(f"🔍 Строка {line_num}: status = '{status}'", flush=True)
                
                # Проверяем статус
                if status == 'active':
                    print(f"✅ Найдена активная неделя!", flush=True)
                    
                    # Выводим все поля для отладки
                    print(f"📝 Данные строки:", flush=True)
                    for key, value in row.items():
                        preview = value[:50] + "..." if len(value) > 50 else value
                        print(f"   {key}: {preview}", flush=True)
                    
                    return format_week_data(row)
            
            print("⚠️ Не найдена активная неделя в таблице (нет строки со status='active')", flush=True)
            return None
        
    except Exception as e:
        print(f"❌ Ошибка при чтении Google Sheets: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


def format_week_data(row):
    """Форматирование данных недели из строки таблицы"""
    try:
        # Парсим JSON с данными дней
        days_json_str = row.get('days_json', '').strip()
        
        if not days_json_str:
            print("❌ Колонка days_json пустая", flush=True)
            return None
        
        print(f"🔍 Парсинг days_json ({len(days_json_str)} символов)...", flush=True)
        
        try:
            days_data = json.loads(days_json_str)
        except json.JSONDecodeError as e:
            print(f"❌ Невалидный JSON в days_json: {e}", flush=True)
            print(f"📄 Первые 200 символов: {days_json_str[:200]}", flush=True)
            return None
        
        if not isinstance(days_data, list):
            print(f"❌ days_json должен быть массивом, получен: {type(days_data)}", flush=True)
            return None
        
        if len(days_data) != 7:
            print(f"⚠️ В days_json должно быть 7 элементов, получено: {len(days_data)}", flush=True)
        
        # Формируем INPUT для промпта
        input_data = f"""start_date: {row.get('start_date', '')}
lesson_url: {row.get('lesson_url', '')}
main_point: {row.get('main_point', '')}
days:
"""
        
        for day in days_data:
            input_data += f"""
- ref: "{day.get('ref', '')}"
  note: "{day.get('note', '')}"
  verse_text: "{day.get('verse_text', '')}"
"""
        
        print(f"✅ Данные недели успешно сформированы", flush=True)
        return input_data
        
    except Exception as e:
        print(f"❌ Ошибка форматирования данных: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


async def generate_messages_with_claude(input_data):
    """Генерация 7 сообщений через OpenRouter API"""
    try:
        prompt = PROMPT_TEMPLATE.format(input_data=input_data)
        
        # OpenRouter API endpoint
        url = "https://openrouter.ai/api/v1/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": "https://github.com/your-repo",  # Опционально
            "X-Title": "Bible Telegram Bot",  # Опционально
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": OPENROUTER_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 4000,
            "temperature": 0.3  # Низкая температура для точности
        }
        
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            result = response.json()
            response_text = result['choices'][0]['message']['content']
            
            # Парсим 7 сообщений из кодовых блоков
            messages = extract_messages_from_response(response_text)
            
            return messages
        
    except Exception as e:
        print(f"❌ Ошибка генерации через OpenRouter: {e}")
        return None


def extract_messages_from_response(response_text):
    """Извлечение 7 сообщений из ответа Claude (из код-блоков)"""
    import re
    
    # Ищем все блоки кода ```...```
    code_blocks = re.findall(r'```(.*?)```', response_text, re.DOTALL)
    
    if len(code_blocks) != 7:
        print(f"⚠️ Ожидалось 7 сообщений, получено: {len(code_blocks)}")
    
    # Убираем возможные языковые маркеры типа ```markdown
    messages = []
    for block in code_blocks:
        # Если первая строка - это язык (markdown, text и т.д.), убираем её
        lines = block.strip().split('\n')
        if lines[0].strip() in ['markdown', 'text', 'md']:
            block = '\n'.join(lines[1:])
        messages.append(block.strip())
    
    return messages


async def send_telegram_message(message_text):
    """Отправка сообщения в Telegram группу"""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message_text,
            parse_mode='HTML'
        )
        print(f"✅ Сообщение отправлено в {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
        
    except TelegramError as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")


async def daily_job():
    """Ежедневная задача: отправка одного сообщения"""
    print(f"\n🔄 Запуск ежедневной задачи: {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Загружаем данные из Google Sheets
    input_data = await load_google_sheet_data()
    
    if not input_data:
        print("❌ Нет данных для отправки")
        return
    
    # Генерируем все 7 сообщений
    messages = await generate_messages_with_claude(input_data)
    
    if not messages or len(messages) < 7:
        print("❌ Не удалось сгенерировать сообщения")
        return
    
    # Определяем, какое сообщение отправить (по дню недели)
    # Предполагается, что неделя начинается с понедельника (0-индекс)
    current_weekday = datetime.now(TIMEZONE).weekday()  # 0=Пн, 6=Вс
    
    if current_weekday < len(messages):
        message_to_send = messages[current_weekday]
        await send_telegram_message(message_to_send)
    else:
        print(f"⚠️ Нет сообщения для дня недели: {current_weekday}")


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

async def main():
    """Главная функция - запуск планировщика и веб-сервера"""
    print("🚀 Запуск Bible Telegram Bot")
    
    # Проверка переменных окружения
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, OPENROUTER_API_KEY, GOOGLE_SHEET_ID]):
        print("❌ Не все переменные окружения установлены!")
        print("Требуются: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, OPENROUTER_API_KEY, GOOGLE_SHEET_ID")
        return
    
    # ВАЖНО: Сначала запускаем веб-сервер для Render
    print(f"🌐 Запуск веб-сервера на порту {PORT}...")
    runner = await start_web_server()
    print(f"✅ Веб-сервер запущен на порту {PORT}")
    
    # Даём время Render обнаружить порт
    await asyncio.sleep(3)
    
    print(f"⏰ Настройка отправки сообщений каждый день в 04:10 UTC+3")
    
    # Создаём планировщик
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    
    # Добавляем задачу: каждый день в 04:10
    scheduler.add_job(
        daily_job,
        'cron',
        hour=4,
        minute=10,
        id='daily_bible_message'
    )
    
    # Запускаем планировщик
    scheduler.start()
    print("✅ Планировщик запущен")
    
    # Опционально: запустить задачу сразу для теста
    await daily_job()
    
    print("🎉 Бот полностью запущен и работает!")
    
    # Держим программу запущенной
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        print("\n👋 Остановка бота...")
        scheduler.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    # Отключаем буферизацию вывода для Render
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    asyncio.run(main())
