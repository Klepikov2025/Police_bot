import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta

import nest_asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ChatJoinRequest, ChatMemberUpdated

nest_asyncio.apply()  # Синхронизация event loops

##############################################
# Логирование
##############################################
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

##############################################
# Получение токена бота
##############################################
API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    logger.error("Не удалось получить токен бота! Проверьте переменную окружения BOT_TOKEN.")
    exit(1)

##############################################
# Подключение к базе данных SQLite
##############################################
# Файл базы будет создан в рабочей директории контейнера (bot.db)
conn = sqlite3.connect('bot.db', check_same_thread=False)
cursor = conn.cursor()

# Создание таблиц (если их нет)
cursor.execute('''
CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    city TEXT,
    region_code INTEGER,
    is_old_group BOOLEAN DEFAULT 0,
    network TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS requests (
    user_id INTEGER,
    group_id INTEGER,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, group_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS member_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    group_id INTEGER,
    event_type TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_interactions (
    user_id INTEGER PRIMARY KEY,
    first_interaction DATETIME
)
''')
conn.commit()

# Если таблица groups пуста, заполняем её данными
cursor.execute("SELECT COUNT(*) FROM groups")
if cursor.fetchone()[0] == 0:
    ns_groups = [
        (-1001465465654, 'Курган', 45, 1, 'NS'),
        (-1001824149334, 'Новосибирск', 54, 1, 'NS'),
        (-1002233108474, 'Челябинск', 74, 1, 'NS'),
        (-1001753881279, 'Пермь', 59, 1, 'NS'),
        (-1001823390636, 'Уфа', 2, 1, 'NS'),
        (-1002145851794, 'Ямал', 89, 1, 'NS'),
        (-1001938448310, 'Москва', 77, 1, 'NS'),
        (-1001442597049, 'ХМАО', 86, 1, 'NS'),
        (-1001475923869, 'Секс Чат 72', 72, 1, 'NS'),
        (-1002114646372, 'Чат НС 86', 86, 1, 'NS'),
        (-1001709856174, 'Чат НС 59', 59, 1, 'NS'),
        (-1001550223843, 'Чат НС 74', 74, 1, 'NS'),
        (-1002154524569, 'Чат НС 66', 66, 1, 'NS'),
        (-1001265993934, 'Чат НС 72', 72, 1, 'NS'),
        (-1001841617225, 'Чат НС 16', 16, 1, 'NS'),
        (-1002137199355, 'Чат НС 89', 89, 1, 'NS'),
        (-1002072150944, 'Чат НС 54', 54, 1, 'NS'),
        (-1002200049020, 'Чат НС 78', 78, 1, 'NS'),
        (-1001746554744, 'Чат НС 55', 55, 1, 'NS'),
        (-1002193127380, 'Челябинск', 74, 1, 'NS'),
        (-1002169473861, 'Екатеринбург', 66, 1, 'NS')
    ]
    parni_groups = [
        (-1002413948841, 'Екатеринбург', 66, 0, 'PARNI'),
        (-1002255622479, 'Тюмень', 72, 0, 'PARNI'),
        (-1002274367832, 'Омск', 55, 0, 'PARNI'),
        (-1002406302365, 'Челябинск', 74, 0, 'PARNI'),
        (-1002280860973, 'Пермь', 59, 0, 'PARNI'),
        (-1002469285352, 'Курган', 45, 0, 'PARNI'),
        (-1002287709568, 'ХМАО', 86, 0, 'PARNI'),
        (-1002448909000, 'Уфа', 2, 0, 'PARNI'),
        (-1002261777025, 'Новосибирск', 54, 0, 'PARNI'),
        (-1002371438340, 'ЯМАО', 89, 0, 'PARNI')
    ]
    mk_groups = [
        (-1001219669239, '🥷 МК | Без предрассудков 18+', 0, 0, 'MK'),
        (-1001814693664, '🥷 МК | Юг', 0, 0, 'MK'),
        (-1002210043742, '🥷 МК | Екатеринбург', 0, 0, 'MK'),
        (-1002205127231, '🥷 МК | Пермь', 0, 0, 'MK'),
        (-1002228881675, '🥷 МК | Казань', 0, 0, 'MK'),
        (-1002208434096, '🥷 МК | Москва', 0, 0, 'MK'),
        (-1001604781452, '🥷 МК | Ижевск', 0, 0, 'MK'),
        (-1001852671383, '🥷 МК | Самара', 0, 0, 'MK'),
        (-1001631628911, '🥷 МК | Нижний Новосибирск', 0, 0, 'MK'),
        (-1002151258573, '🥷 МК | Омск', 0, 0, 'MK'),
        (-1002248474008, '🥷 МК | Красноярск', 0, 0, 'MK'),
        (-1002207503508, '🥷 МК | Воронеж', 0, 0, 'MK'),
        (-1002167762598, '🥷 МК | Волгоград', 0, 0, 'MK'),
        (-1002161346845, '🥷 МК | Дальний Восток', 0, 0, 'MK'),
        (-1002235645677, '🥷 МК | Новосибирск', 0, 0, 'MK'),
        (-1002210623988, '🥷 МК | Тюмень, ХМАО и ЯНАО', 0, 0, 'MK'),
        (-1002217056197, '🥷 МК | Калининград', 0, 0, 'MK'),
        (-1002210419274, '🥷 МК | Иркутск', 0, 0, 'MK'),
        (-1002147522863, '🥷 МК | Кемерово', 0, 0, 'MK'),
        (-1002169723426, '🥷 МК | Мужской чат', 0, 0, 'MK'),
        (-1002196469365, '🥷 МК | Уфа', 0, 0, 'MK'),
        (-1002236337328, '🥷 МК | Секс-туризм', 0, 0, 'MK'),
        (-1002234471215, '🥷 МК | Барнаул', 0, 0, 'MK'),
        (-1002238514762, '🥷 МК | Челябинск', 0, 0, 'MK'),
        (-1002217967528, '🥷 МК | Галерея', 0, 0, 'MK'),
        (-1002485776859, '🥷 МК | Санкт-Петербург', 0, 0, 'MK'),
        (-1002426762134, '🥷 МК | Саратов', 0, 0, 'MK'),
        (-1002255568202, '🥷 МК | Оренбург', 0, 0, 'MK'),
        (-1002197215824, '🥷 МК | Фетиши', 0, 0, 'MK'),
        (-1002427366211, 'МК | Казахстан', 0, 0, 'MK'),
        (-1001727060632, 'Группа FAQ', 0, 0, 'MK'),
        (-1001415498051, 'RAINBOW MAN', 0, 0, 'MK'),
    ]
    all_groups = ns_groups + parni_groups + mk_groups
    cursor.executemany(
        "INSERT OR REPLACE INTO groups (group_id, city, region_code, is_old_group, network) VALUES (?, ?, ?, ?, ?)",
        all_groups
    )
    conn.commit()

##############################################
# Создаем бота и диспетчер
##############################################
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot=bot)

##############################################
# Вспомогательные функции для проверки пользователей
##############################################
async def is_user_in_parni(user_id: int) -> bool:
    cursor.execute("SELECT group_id FROM groups WHERE network = 'PARNI'")
    parni_groups = cursor.fetchall()
    for group in parni_groups:
        try:
            member = await bot.get_chat_member(group[0], user_id)
            if member.status == 'member':
                return True
        except Exception as e:
            logger.error(f"Ошибка при проверке участия пользователя {user_id} в группе {group[0]}: {e}")
    return False

async def is_user_verified_in_ns(user_id: int) -> bool:
    cursor.execute("SELECT group_id FROM groups WHERE network = 'NS'")
    ns_groups = cursor.fetchall()
    for group in ns_groups:
        try:
            member = await bot.get_chat_member(group[0], user_id)
            join_date = getattr(member, 'joined_at', None)
            if member.status == 'member':
                if join_date is None or (datetime.now() - join_date) >= timedelta(days=7):
                    return True
        except Exception as e:
            logger.error(f"Ошибка при проверке участия пользователя {user_id} в группе {group[0]}: {e}")
    return False

async def is_user_verified_in_mk(user_id: int) -> bool:
    cursor.execute("SELECT group_id FROM groups WHERE network = 'MK'")
    mk_groups = cursor.fetchall()
    for group in mk_groups:
        try:
            member = await bot.get_chat_member(group[0], user_id)
            join_date = getattr(member, 'joined_at', None)
            if member.status == 'member':
                if join_date is None or (datetime.now() - join_date) >= timedelta(days=7):
                    return True
        except Exception as e:
            logger.error(f"Ошибка при проверке участия пользователя {user_id} в группе {group[0]}: {e}")
    return False

async def is_user_verified_for_parni(user_id: int) -> bool:
    return (await is_user_verified_in_ns(user_id)) or (await is_user_verified_in_mk(user_id))

##############################################
# Обработка заявок на вступление
##############################################
async def process_request(user_id: int, req_group_id: int):
    logger.info(f"Обработка заявки от пользователя {user_id} для группы {req_group_id}")
    cursor.execute("SELECT city, network FROM groups WHERE group_id = ?", (req_group_id,))
    group_info = cursor.fetchone()

    if not group_info:
        logger.info(f"Группа {req_group_id} не найдена в базе данных. Отклонение заявки для пользователя {user_id}.")
        try:
            await bot.decline_chat_join_request(req_group_id, user_id)
        except Exception as e:
            logger.error(f"Ошибка при отклонении заявки для пользователя {user_id} в группе {req_group_id}: {e}")
        return

    group_city, network = group_info
    logger.info(f"Данные группы {req_group_id}: город={group_city}, сеть={network}")

    if network in ['NS', 'MK']:
        logger.info(f"Игнорирование заявки для пользователя {user_id} в группе {network} {req_group_id} (группа для верификации).")
        return
    elif network == 'PARNI':
        if await is_user_in_parni(user_id):
            logger.info(f"Пользователь {user_id} уже в группе PARNI. Отклонение заявки для группы {req_group_id}.")
            try:
                await bot.decline_chat_join_request(req_group_id, user_id)
            except Exception as e:
                logger.error(f"Ошибка при отклонении заявки PARNI для пользователя {user_id} в группе {req_group_id}: {e}")
            return
        if await is_user_verified_for_parni(user_id):
            try:
                await bot.approve_chat_join_request(req_group_id, user_id)
                cursor.execute("INSERT OR REPLACE INTO requests (user_id, group_id) VALUES (?, ?)", (user_id, req_group_id))
                conn.commit()
                logger.info(f"Заявка пользователя {user_id} в группу PARNI {req_group_id} одобрена.")
            except Exception as e:
                logger.error(f"Ошибка при одобрении заявки PARNI для пользователя {user_id} в группе {req_group_id}: {e}")
        else:
            logger.info(f"Пользователь {user_id} не подтвержден в сети NS/MK (не состоит ни в одной группе NS/MK >=7 дней). Отклонение заявки для группы {req_group_id}.")
            try:
                await bot.decline_chat_join_request(req_group_id, user_id)
            except Exception as e:
                logger.error(f"Ошибка при отклонении заявки PARNI для пользователя {user_id} в группе {req_group_id}: {e}")
        return
    else:
        logger.error(f"Неизвестный тип сети для группы {req_group_id}. Отклонение заявки для пользователя {user_id}.")
        try:
            await bot.decline_chat_join_request(req_group_id, user_id)
        except Exception as e:
            logger.error(f"Ошибка при отклонении заявки для пользователя {user_id} в группе {req_group_id}: {e}")
        return

##############################################
# Просмотр висящих заявок
##############################################
async def process_pending_requests():
    logger.info("Запуск обработки висящих заявок...")
    cursor.execute("SELECT group_id FROM groups")
    groups_list = cursor.fetchall()
    for group in groups_list:
        group_id = group[0]
        try:
            join_requests = await bot.get_chat_join_requests(chat_id=group_id, limit=100)
            if not join_requests:
                logger.info(f"Нет висящих заявок для группы {group_id}.")
                continue
            logger.info(f"Найдено {len(join_requests)} висящих заявок для группы {group_id}.")
            for req in join_requests:
                user_id = req.from_user.id
                logger.info(f"Обработка висящей заявки: пользователь {user_id} в группе {group_id}")
                await process_request(user_id, group_id)
        except Exception as e:
            logger.error(f"Ошибка при получении заявки для группы {group_id}: {e}")
    logger.info("Обработка висящих заявок завершена.")

##############################################
# Команда для ручной обработки висящих заявок
##############################################
@dp.message(Command("process_pending"))
async def process_pending_cmd(message: types.Message):
    await process_pending_requests()
    await message.answer("Обработка висящих заявок завершена.")

##############################################
# Логирование входа/выхода участников
##############################################
@dp.chat_member()
async def chat_member_update_handler(update: ChatMemberUpdated):
    user_id = update.from_user.id
    group_id = update.chat.id
    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    event = None
    if old_status != "member" and new_status == "member":
        event = "join"
    elif old_status == "member" and new_status != "member":
        event = "leave"
    if event:
        cursor.execute("INSERT INTO member_logs (user_id, group_id, event_type) VALUES (?, ?, ?)",
                       (user_id, group_id, event))
        conn.commit()
        logger.info(f"Лог: пользователь {user_id} {event} группу {group_id}")

##############################################
# Обработчик заявок на вступление
##############################################
@dp.chat_join_request()
async def handle_join_request(request: ChatJoinRequest):
    user_id = request.from_user.id
    req_group_id = request.chat.id
    logger.info(f"Получена заявка от пользователя {user_id} для группы {req_group_id}.")
    await process_request(user_id, req_group_id)

##############################################
# Основной polling-цикл бота
##############################################
async def polling_loop():
    while True:
        try:
            logger.info("Запуск polling-цикла бота...")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            logger.error(f"Ошибка в работе бота: {e}. Перезапуск через 10 секунд...")
            try:
                await dp.stop_polling()
            except Exception as inner_e:
                logger.error(f"Ошибка при остановке polling: {inner_e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(polling_loop())
