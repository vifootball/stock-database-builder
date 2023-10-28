import os
import time
import ray
import pandas as pd
import random
from constants import *
from history import *

# load symbols
etf_master = pd.read_csv(os.path.join('downloads', 'masters_etf.csv'))
etf_symbols = list(enumerate(etf_master['symbol'].to_list()))

currency_master = pd.read_csv(os.path.join('downloads', 'masters_currency.csv'))
currency_symbols = list(enumerate(currency_master['symbol'].to_list()))

indices_master_yahoo = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_yahoo.csv'))
indices_symbols_yahoo = list(enumerate(indices_master_yahoo['symbol'].to_list()))

indices_master_investpy = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_investpy.csv'))
indices_symbols_investpy = list(enumerate(indices_master_investpy['symbol'].to_list()))

indices_master_fred = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_fred.csv'))
indices_symbols_fred = list(enumerate(indices_master_fred['symbol'].to_list()))

# time
# time.sleep(0.3)
# time.sleep(round(random.uniform(0.2, 3), 5))

# define ray functions
@ray.remote
def collect_etf_history_from_yf(order, symbol):
    time.sleep(0.3)
    time.sleep((order % 8) * 0.15) #앞 2자리
    history = transform_history(get_history_from_yf(symbol))
    if history is not None:
        os.makedirs('downloads/history/etf', exist_ok=True)
        history.to_csv(f'downloads/history/etf/{symbol}_history.csv', index=False)

@ray.remote
def collect_currency_history_from_yf(order, symbol):
    time.sleep(0.2)
    time.sleep((order % 8) * 0.1)
    history = transform_history(get_history_from_yf(symbol))
    if history is not None:
        os.makedirs('downloads/history/currency', exist_ok=True)
        history.to_csv(f'downloads/history/currency/{symbol}_history.csv', index=False)

@ray.remote
def collect_indices_history_from_yf(order, symbol):
    time.sleep(0.2)
    time.sleep((order % 8) * 0.1)
    history = transform_history(get_history_from_yf(symbol))
    if history is not None:
        os.makedirs('downloads/history/indices', exist_ok=True)
        history.to_csv(f'downloads/history/indices/{symbol}_history.csv', index=False)

@ray.remote
def collect_indices_history_from_fred(order, symbol):
    time.sleep(0.3)
    time.sleep((order % 8) * 0.25)
    history = transform_history(get_history_from_fred(symbol))
    if history is not None:
        os.makedirs('downloads/history/indices', exist_ok=True)
        history.to_csv(f'downloads/history/indices/{symbol}_history.csv', index=False)


if __name__ == '__main__':
    # print(etf_symbols[:10])
    # print(indices_symbols_fred)

    ray.init(ignore_reinit_error=True,  num_cpus=8)
    
    # tasks_collect_etf_history_from_yf = [collect_etf_history_from_yf.remote(order, symbol) for order, symbol in etf_symbols[:]]
    # ray.get(tasks_collect_etf_history_from_yf)
    
    tasks_collect_currency_history_from_yf = [collect_currency_history_from_yf.remote(order, symbol) for order, symbol in currency_symbols[:]]
    ray.get(tasks_collect_currency_history_from_yf)

    tasks_collect_indices_investpy_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_investpy[:]]
    ray.get(tasks_collect_indices_investpy_history_from_yf)
    
    tasks_collect_indices_yahoo_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_yahoo[:]]
    ray.get(tasks_collect_indices_yahoo_history_from_yf)
    
    tasks_collect_indices_history_from_fred = [collect_indices_history_from_fred.remote(order,symbol) for order, symbol in indices_symbols_fred[:]]
    ray.get(tasks_collect_indices_history_from_fred)








# if __name__ == '__main__':
#     print(os.getcwd())

#     # path setting for window scheduler
#     os.chdir(r"C:\Users\Dongwook Jung\Home\dongwook-src\stock-database-builder")
    
#     print('hi')
#     # print(os.getcwd())
#     # print(os.listdir())

#     # # Run Only Once a Year
#     # DataConstructor.construct_etf_metas()
#     # DataConstructor.construct_etf_profiles()
#     # DataConstructor.construct_etf_infos()
    
#     # Masters
#     # DataConstructor.construct_etf_masters()
#     # DataConstructor.construct_currency_masters()
#     # DataConstructor.construct_index_yahoo_main_masters()
#     # DataConstructor.construct_index_fd_masters()
#     # DataConstructor.construct_index_investpy_masters()
#     # DataConstructor.construct_index_yf_masters()
#     # DataConstructor.construct_index_fred_masters()

#     # Histories
#     DataConstructor.construct_etf_histories()
#     DataConstructor.construct_currency_histories()
#     DataConstructor.construct_index_yf_histories()
#     DataConstructor.construct_index_fred_histories()

#     # DataConstructor.construct_recents(
#     #     get_dir_histories=os.path.join("download", "etf_history"),
#     #     put_fpath_recents=os.path.join("download", "recent", "etf_recents.csv")
#     # )
#     # DataConstructor.construct_summaries(
#     #     master=pd.read_csv(os.path.join("download", "master", "etf_masters.csv")),
#     #     recent=pd.read_csv(os.path.join("download", "recent", "etf_recents.csv")),
#     #     fpath_summary=os.path.join("download", "summary", "etf_summaries.csv")
#     # )

#     # DataConstructor.construct_recents(
#     #     get_dir_histories=os.path.join("download", "currency_history"),
#     #     put_fpath_recents=os.path.join("download", "recent", "currency_recents.csv")
#     # )
#     # DataConstructor.construct_summaries(
#     #     master=pd.read_csv(os.path.join("download", "master", "currency_masters.csv")),
#     #     recent=pd.read_csv(os.path.join("download", "recent", "currency_recents.csv")),
#     #     fpath_summary=os.path.join("download", "summary", "currency_summaries.csv")
#     # )

#     # DataConstructor.construct_recents(
#     #     get_dir_histories=os.path.join("download", "index_yf_history"),
#     #     put_fpath_recents=os.path.join("download", "recent", "index_yf_recents.csv")
#     # )
#     # DataConstructor.construct_summaries(
#     #     master=pd.read_csv(os.path.join("download", "master", "index_yf_masters.csv")),
#     #     recent=pd.read_csv(os.path.join("download", "recent", "index_yf_recents.csv")),
#     #     fpath_summary=os.path.join("download", "summary", "index_yf_summaries.csv")
#     # )

#     # DataConstructor.construct_recents(
#     #     get_dir_histories=os.path.join("download", "index_fred_history"),
#     #     put_fpath_recents=os.path.join("download", "recent", "index_fred_recents.csv")
#     # )
#     # DataConstructor.construct_summaries(
#     #     master=pd.read_csv(os.path.join("download", "master", "index_fred_masters.csv")),
#     #     recent=pd.read_csv(os.path.join("download", "recent", "index_fred_recents.csv")),
#     #     fpath_summary=os.path.join("download", "summary", "index_fred_summaries.csv")
#     # )


#     bq_uploader = BqUploader()
#     # bq_uploader.upload_summaries_to_bq()
#     bq_uploader.upload_history_to_bq()

#     print("Done")