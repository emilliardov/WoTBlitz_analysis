import pandas as pd
import numpy as np
import pandahouse as ph
import asyncio
import aiohttp
import random
import conf_secret

random.seed(80)

connection_default = conf_secret.connection_default

# создаем таблицу в БД
q = """
CREATE TABLE IF NOT EXISTS default.players_ru(
    account_id UInt64,
    created_at DateTime,
    updated_at DateTime,
    last_battle_time DateTime,
    nickname String,
    spotted UInt64,
    max_frags_tank_id UInt64,
    hits UInt64,
    frags UInt64,
    max_xp UInt64,
    max_xp_tank_id UInt64,
    wins UInt64,
    losses UInt64,
    capture_points UInt64,
    battles UInt64,
    damage_dealt UInt64,
    damage_received UInt64,
    max_frags UInt8,
    shots UInt64,
    frags8p UInt64,
    xp UInt64,
    win_and_survived UInt64,
    survived_battles UInt64,
    dropped_capture_points UInt64 
    ) 
ENGINE = MergeTree() 
ORDER BY (account_id)
"""
#ph.execute(query=q, connection=connection_default)

players_info_url_ser = 'https://api.wotblitz.ru/wotb/account/info/?application_id=fe3da7e52fdf14aeb35a6872e0773bfe&fields=-private%2C+-statistics.clan&account_id='
# это может не работать на другой машине, видимо варгейминг разрешает делать запросы только с ip-адреса, указанного при создании сервера
# можно подставить значение fe3da7e52fdf14aeb35a6872e0773bfe в url вместо d96b94196649d62e93e7d1f3b977d20d в части application_id=
# это снизит кол-во запросов в сек с 20 до 10, но должно работать. В таком случае нужно заменить части кода

#player_ids_sim = np.arange(1*10**8,1.5*10**8)
player_ids_sim = list(np.arange(100000000,200000000))
player_ids_sim = random.sample(player_ids_sim, int(len(player_ids_sim)/4))
player_ids_sim_chuncked = np.array_split(player_ids_sim, len(player_ids_sim)/100)
player_ids_sim_chuncked = np.array_split(player_ids_sim_chuncked, len(player_ids_sim_chuncked)/10) # заменить /10 
player_ids_sim_chuncked = np.array_split(player_ids_sim_chuncked, len(player_ids_sim_chuncked)/50)

# группируем по 100 для составления ссылок
# массивы по 100 групп по 20 так как 20 запросов/сек
# масс по 100 по 20 групп по 50 так как 100*20*50=100к id
# то есть записи в базу данных делаем максимум по 100к id


async def get(
        session: aiohttp.ClientSession,
        ids_list: list,
        **kwargs
) -> dict:
    # создаем часть ссылки, в которой перечисляются id игроков
    url_part = '%2C+'.join(map(str, ids_list))

    resp = await session.request('GET', url=(players_info_url_ser + url_part), **kwargs)
    data = await resp.json()
    data = data['data']

    # примерно треть id не привязана к игрокам и возвращает None, эти id можно удалить
    no_nulls_data = {k: v for k, v in data.items() if v is not None} 

    # делаем список плоским (одноразмерным)
    if len(no_nulls_data) != 0:
        for key, value in no_nulls_data.items():
            value.update(value.pop('statistics').pop('all')) 

    return no_nulls_data

# так как сервер ограничен 20 запросами/сек, то асинхронно отправляем 20 ссылок
async def main(players_ids, **kwargs):
    sem = asyncio.Semaphore(10) # заменить на 10
    async with sem:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for ids in players_ids:
                tasks.append(get(session=session, ids_list=ids, **kwargs))
            pl_stats = await asyncio.gather(*tasks, return_exceptions=True)
            return pl_stats


j=1
# big содержит 50 списков, в каждом из которых 20 списков
for big in player_ids_sim_chuncked:

    final_arr = []

    # medium содержит 20 списков, в каждом из которых 100 id
    for medium in big:
        itog = asyncio.run(main(medium))
        final_arr.extend(itog)

    try:
        final_arr = [list(i.values()) for i in final_arr]
        final_arr = [item for sublist in final_arr for item in sublist]

        df_to_db = pd.DataFrame.from_records(final_arr)
        df_to_db = df_to_db.dropna()
        df_to_db = df_to_db.astype({'created_at': 'datetime64[s]',
                                'updated_at': 'datetime64[s]',
                                'last_battle_time': 'datetime64[s]'})

        ph.to_clickhouse(df_to_db, 'players_ru', index=False, connection=connection_default)

        print(str(j) + ' из 172. ' + str(df_to_db.shape[0]) + ' записей добавлено') # заменить на Из 2000.

    # если повезло и ни один id не содержал информации, то может выдать ошибку
    except:
        print(str(j) + ' из 172. 0 записей добавлено')

    j+=1