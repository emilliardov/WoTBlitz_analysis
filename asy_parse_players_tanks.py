import pandas as pd
import numpy as np
import pandahouse as ph
import asyncio
import aiohttp
import pickle
import time
import requests
import random
import conf_secret

random.seed(80)

connection_default = conf_secret.connection_default

players_info_url_ser = 'https://api.wotblitz.ru/wotb/tanks/stats/?application_id=fe3da7e52fdf14aeb35a6872e0773bfe&account_id='

players_upd_tanks_creation_q = """
CREATE TABLE default.players_ru_tanks (
    account_id UInt64,
    last_battle_time DateTime,
    max_xp UInt64,
    in_garage_updated UInt64,
    max_frags UInt64,
    frags UInt64,
    mark_of_mastery UInt64,
    battle_life_time UInt64,
    tank_id UInt64,
    spotted UInt64,
    hits UInt64,
    wins UInt64,
    losses UInt64,
    capture_points UInt64,
    battles UInt64,
    damage_dealt UInt64,
    damage_received UInt64,
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

#ph.execute(query=players_upd_tanks_creation_q, connection=connection_default)

qu = """
select account_id
from players_ru
where last_battle_time > toDateTime('2022-08-30 00:00:00') and battles > 500 and account_id not in (select account_id from players_ru_tanks)
"""

account_ids = ph.read_clickhouse(qu, connection=connection_default)

account_ids = account_ids['account_id'].tolist()

account_ids = random.sample(account_ids, 100000)

async def get(
    session: aiohttp.ClientSession,
    account_id: int
) -> dict:

    resp = await session.request('GET', url=(players_info_url_ser+str(account_id)))
    data = await resp.json()
    data = data['data'][str(account_id)]
    
    if data is None:
        return
    
    for i in data:
        i.update(i.pop('all'))

    return data


async def main(players_ids):
    sem = asyncio.Semaphore(10)
    async with sem:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for account_id in players_ids:
                tasks.append(get(session=session, account_id=account_id))
        # asyncio.gather() will wait on the entire task set to be
        # completed.  If you want to process results greedily as they come in,
        # loop over asyncio.as_completed()
            pl_stats = await asyncio.gather(*tasks, return_exceptions=True)
            return pl_stats

#asyncio.run(main(players_ids))

async def parsy(ls):
    final_arr = []
    for med_tw in ls:
        
        itog = await main(med_tw)
        final_arr.extend(itog)
        #time.sleep(0.5)
        
    final_arr = [i for i in final_arr if i is not None]
    final_arr = [item for sublist in final_arr for item in sublist]

    df_to_db = pd.DataFrame(final_arr)
    df_to_db = df_to_db.astype({'last_battle_time': 'datetime64[s]'})
    df_to_db.drop('in_garage', axis=1, inplace=True)
    first_column = df_to_db.pop('account_id')
    df_to_db.insert(0, 'account_id', first_column)

    ph.to_clickhouse(df_to_db, 'players_ru_tanks', index=False, connection=connection_default)
    return df_to_db.shape[0]
    #return final_arr

account_ids_chunk = np.array_split(account_ids, len(account_ids)/10)
account_ids_chunk = np.array_split(account_ids_chunk, len(account_ids_chunk)/50)

i=1
for big in account_ids_chunk:
    try:
        r = await parsy(big)
        print(str(i)+' из 100. '+str(r)+' записей добавлено')
    except:
        print(str(i)+' из 100. 0 записей добавлено')
    i+=1