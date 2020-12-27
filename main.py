import os
import io
import json
import time
import zipfile
import asyncio
import argparse
import threading

import aiohttp
import asyncpg
import aiofiles

from datetime import datetime

from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.utils.exceptions import BadRequest as AioGramBadRequest


# Config DB
DATABASE = ""
USER = ""
PASSWORD = ""
HOST = "localhost"
PORT = "5432"

# ID юзеров с которыми будет связь
admin_ids = [123, 456]

# Токен бота
bot_token = ""

# Содержит временные файлы работы скрипта
CACHE_PATH = os.path.abspath('./.cache/')
# Архивы с текстовыми документами доменов
ARCHIVES_PATH = os.path.join(CACHE_PATH, 'archives/')
# Текстовые документы с доменами
DOMAINS_PATH = os.path.join(CACHE_PATH, 'domains/')

BYPASS = [
    'Telenet',
    'Shopify',
    'Verizon Media',
    'Cisco Meraki',
    'Alibaba',
    'stanford',
    'Microsoft Online Services',
    'AT&T',
    'MTN Group',
    'WP Engine'
]

# Создаем папки, если их нет
if not os.path.exists(CACHE_PATH):
    os.mkdir(CACHE_PATH)
    os.mkdir(ARCHIVES_PATH)
    os.mkdir(DOMAINS_PATH)
else:
    if not os.path.exists(ARCHIVES_PATH):
        os.mkdir(ARCHIVES_PATH)
    if not os.path.exists(DOMAINS_PATH):
        os.mkdir(DOMAINS_PATH)

logger.add(
    "fileLog.log",
    rotation="0.1 GB",
    compression="tar.gz",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"
)

@logger.catch
def createArgumentParser():
    ''' Парсер аргументов пользователя из коммандной строки '''

    parser = argparse.ArgumentParser()

    # create - создание и заполнение бд
    parser.add_argument('-m', '--mode', default='scrap')

    return parser


async def downloadPrograms(data):
    ''' Скачивает архивы программ с их доменами '''

    archive_name = data['URL'].split('/')[-1]
    path_to_archive = os.path.join(ARCHIVES_PATH, archive_name)

    # Конвертируем из строки в объект datetime, обрезается с конца
    # 4 символа: нулевая таймзона (символ Z) и из-за того что библиотека
    # datetime поддерживает только миллисекунды (в них 6 символов) -
    # с конца обрезаются три символа наносекунд (они содержат 9 символов)
    data['last_updated'] = datetime.strptime(
        data['last_updated'][:-4],
        '%Y-%m-%dT%H:%M:%S.%f'
    )
    data['name'] = data['name'].lower()

    record_program = await pool.fetchrow('SELECT * FROM bg_programs WHERE name = ($1);', data['name'])
    if record_program is None:
        # Если записи программы нет - добавляем и достаем запись
        await pool.execute(
            'INSERT INTO bg_programs(name, program_url, last_updated) VALUES ($1, $2, $3);',
            data['name'], data['program_url'], data['last_updated']
        )
        record_program = await pool.fetchrow('SELECT * FROM bg_programs WHERE name = ($1);', data['name'])
    else:
        # В случае отсутствия обновлений - выходим
        if data['change'] == 0 or record_program.get('last_updated') >= data['last_updated']:
            return
        else:
            # Изменяем время обновления
            await pool.execute(
                'UPDATE bg_programs SET last_updated = ($1) WHERE name = ($2);',
                data['last_updated'], data['name']
            )

    # Создаем файл, в который будет происходить запись скачиваемого содержимого
    async with aiofiles.open(path_to_archive, 'wb') as archive_file:
        async with aiohttp.ClientSession() as session:
            async with session.get(data['URL']) as response:
                file_response = await response.read()
                await archive_file.write(file_response)

    # Разархивируем все текстовики с доменами в одну папку
    with zipfile.ZipFile(path_to_archive, mode="r") as archive:
        program_domains_path = os.path.join(DOMAINS_PATH, data['name'])
        os.mkdir(program_domains_path)
        archive.extractall(program_domains_path)

    # Начинаем парсить файлы в директории
    if namespace.mode == "create":
        domains = list()
    else:
        domains = set()
    names_files_domains = os.listdir(program_domains_path)
    for name_file_domain in names_files_domains:
        # Добавляем домен верхнего уровня (в названии файла)
        domain = ".".join(name_file_domain.split('.')[0:-1])
        if namespace.mode == "create":
            domains.append("\t".join((
                domain,
                str(data['last_updated']),
                str(record_program.get('id'))
            )))
        else:
            domains.add(domain)

        # Добавляем сабдомены из файла
        file_subdomain_path = os.path.join(program_domains_path, name_file_domain)
        async with aiofiles.open(file_subdomain_path, 'r', encoding='utf-8') as file_subdomain:
            async for subdomain in file_subdomain:
                subdomain = subdomain[:-1]  # Убираем символ \n
                if namespace.mode == "create":
                    domains.append("\t".join((
                        subdomain,
                        str(data['last_updated']),
                        str(record_program.get('id'))
                    )))
                else:
                    domains.add(subdomain)
    for file_domains in names_files_domains:
        os.remove(os.path.join(program_domains_path, file_domains))
    os.rmdir(program_domains_path)
    os.remove(path_to_archive)

    @logger.catch
    async def import_data(connection, new_domains_add):
        # Создаем в памяти файл, в который будем лить домены
        f = io.BytesIO()
        f.write('\n'.join(new_domains_add).encode('utf-8'))
        f.seek(0)

        # Импортируем в бд созданный ранее файл
        res = await connection.copy_to_table(
            "bg_domains", source=f,
            columns=['domain', 'date_add', 'bg_programs_id']
        )

    if namespace.mode != "create":
        async with pool.acquire() as con:
            bd_domains = await con.fetch('SELECT domain FROM bg_domains WHERE bg_programs_id=$1;', record_program.get('id'))
            bd_domains = {bd_domain.get('domain') for bd_domain in bd_domains}
            new_domains = ["\t".join((
                new_domain,
                str(data['last_updated']),
                str(record_program.get('id'))
            )) for new_domain in domains.difference(bd_domains)]
            await import_data(con, new_domains)
    else:
        async with pool.acquire() as con:
            await import_data(con, domains)

    # Возвращаем добавленные домены
    return await pool.fetch(
        'SELECT domain FROM bg_domains WHERE date_add = ($1) AND bg_programs_id = ($2);',
        data['last_updated'], record_program.get('id')
    )


@logger.catch
async def createDb():
    async with await asyncpg.create_pool(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT) as pool:
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute('''
                    CREATE TABLE bg_programs
                    (id SERIAL PRIMARY KEY NOT NULL,
                    name VARCHAR(40) NOT NULL,
                    program_url VARCHAR(300) NOT NULL,
                    last_updated timestamp NOT NULL);
                ''')
                logger.info('Создана таблица "bg_programs"')
                await con.execute('''
                    CREATE TABLE bg_domains
                    (id SERIAL PRIMARY KEY NOT NULL,
                    domain VARCHAR(300) NOT NULL,
                    date_add timestamp NOT NULL,
                    bg_programs_id INT NOT NULL,
                    FOREIGN KEY (bg_programs_id) REFERENCES bg_programs (id) ON DELETE CASCADE);
                ''')
                logger.info('Создана таблица "bg_domains"')


@logger.catch
async def getProgramsList():
    ''' Вовзвращает список программ '''
    async with aiohttp.ClientSession() as session:
        async with session.get('https://chaos-data.projectdiscovery.io/index.json') as response:
            content = await response.text()
    logger.info('Взят список программ')
    return json.loads(content)


@logger.catch
async def main():
    global pool, main_bot

    main_bot = Bot(token=bot_token, loop=loop)

    if namespace.mode == "create":
        await createDb()

    while True:
        programs_list = await getProgramsList()
        pool = await asyncpg.create_pool(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
        domains_records = [downloadPrograms(program) for program in programs_list if program['name'] not in BYPASS]
        records = await asyncio.gather(*domains_records)
        await pool.close()


        async with aiofiles.open('new_domains.txt', 'w', encoding='utf-8') as file:
            domains = list()
            for record in records:
                if record is None:
                    continue
                domains.extend([result_domains.get('domain') for result_domains in record if result_domains is not None])
            await file.write("\n".join(domains))

        try:
            for admin_id in admin_ids:
                await main_bot.send_document(str(admin_id), types.InputFile('new_domains.txt'))
                logger.info('Новые домены отправлены пользователю {admin_id}', admin_id=admin_id)
        except AioGramBadRequest:
            logger.info('Новых доменов - нет')

        os.remove('new_domains.txt')

        if namespace.mode == "create":
            logger.info('Все основные данные добавлены в базу данных')
            exit()

        time.sleep(1800)  # 5 часов


async def programInfo(event: types.Message):
    ''' Отправляет сведения о программе '''
    msg = event.text
    user_id = event.from_user['id']
    if user_id not in admin_ids:
        return
    command_parts = msg.split(' ')
    if len(command_parts) <=1:
        logger.info('Сведения программы не были отправлены пользователю {user_id}. Не правильный ввод комманды. Вводмая комманда: {text}',
                    user_id=user_id, text=event.text)
        await event.answer("Не правильная комманда.")
        return
    program_name = " ".join(command_parts[1:]).lower()


    pool_bot = await asyncpg.create_pool(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
    program = await pool_bot.fetchrow('SELECT * FROM bg_programs WHERE name=$1;', program_name)
    if program is None:
        logger.info('Сведения программы не были отправлены пользователю {user_id}. Не найдена программа ({name}).',
                    user_id=user_id, name=program_name)
        await event.answer("Программа не найдена.")
        return

    await event.answer(
        "Название: %s\nСсылка: %s\nДата последнего обновления: %s" % \
        (program.get('name'), program.get('program_url'), str(program.get('last_updated')))
    )
    await pool_bot.close()


async def programDomains(event: types.Message):
    ''' Отправляет список доменов программы '''
    msg = event.text
    user_id = event.from_user['id']
    if user_id not in admin_ids:
        return
    command_parts = msg.split(' ')
    if len(command_parts) <= 1:
        logger.info('Сведения программы не были отправлены пользователю {user_id}. \
                        Не правильный ввод комманды. Вводмая комманда: {text}',
                    user_id=user_id, text=event.text)
        await event.answer("Не правильная комманда.")
        return
    program_name = " ".join(command_parts[1:]).lower()

    pool_bot = await asyncpg.create_pool(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
    program = await pool_bot.fetchrow('SELECT * FROM bg_programs WHERE name=$1;', program_name)
    if program is None:
        logger.info('Сведения программы не были отправлены пользователю {user_id}. \
                           Не найдена программа ({name}).',
                    user_id=user_id, name=program_name)
        await event.answer("Программа не найдена.")
        return
    domains = await pool_bot.fetch('SELECT domain FROM bg_domains WHERE bg_programs_id=$1', program.get('id'))

    file_name = '%s.txt' % program_name
    async with aiofiles.open(file_name, 'w', encoding='utf-8') as file:
        await file.write('\n'.join([domain.get('domain') for domain in domains]))

    await event.reply_document(document=types.InputFile(file_name))

    os.remove(file_name)
    logger.info('Список доменов программы {program} отправлен пользователю {user_id}',
                user_id=user_id,
                program=program_name)

    await pool_bot.close()


async def tgBot():
    global pool_bot
    global bot

    bot = Bot(token=bot_token, loop=loop_bot)

    try:
        disp = Dispatcher(bot=bot)

        disp.register_message_handler(programInfo, commands={'programinfo', })
        disp.register_message_handler(programDomains, commands={'programdomains', })

        await disp.start_polling()
    finally:
        await bot.close()


if __name__ == "__main__":
    parser = createArgumentParser()
    namespace = parser.parse_args()

    logger.info('Режим выполнения установлен в позиции "{mode}"', mode=namespace.mode)

    loop = asyncio.new_event_loop()
    loop_bot = asyncio.new_event_loop()

    threading.Thread(target=loop.run_forever).start()
    threading.Thread(target=loop_bot.run_forever).start()

    asyncio.run_coroutine_threadsafe(main(), loop)
    asyncio.run_coroutine_threadsafe(tgBot(), loop_bot)

