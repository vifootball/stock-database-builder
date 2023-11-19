import os
import time
import ray
import pandas as pd
import random
from constants import *
from history import *
from utils import *
from summary_grade import *
from bigquery_schema import Schema
from bigquery_helper import *

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

    ### History
    # print(etf_symbols[:10])
    # print(indices_symbols_fred)

    # ray.init(ignore_reinit_error=True,  num_cpus=8)
    
    # tasks_collect_etf_history_from_yf = [collect_etf_history_from_yf.remote(order, symbol) for order, symbol in etf_symbols[:]]
    # ray.get(tasks_collect_etf_history_from_yf)
    
    # tasks_collect_currency_history_from_yf = [collect_currency_history_from_yf.remote(order, symbol) for order, symbol in currency_symbols[:]]
    # ray.get(tasks_collect_currency_history_from_yf)

    # tasks_collect_indices_investpy_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_investpy[:]]
    # ray.get(tasks_collect_indices_investpy_history_from_yf)
    
    # tasks_collect_indices_yahoo_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_yahoo[:]]
    # ray.get(tasks_collect_indices_yahoo_history_from_yf)
    
    # tasks_collect_indices_history_from_fred = [collect_indices_history_from_fred.remote(order,symbol) for order, symbol in indices_symbols_fred[:]]
    # ray.get(tasks_collect_indices_history_from_fred)

    ### Make History Chunk
    save_dfs_by_chunk(
        get_dirpath="./downloads/history/currency/",
        put_dirpath="./downloads/history/chunks/",
        prefix_chunk="currency_chunk"
    )

    ### Summary Grade
    run_summary_main()

    ### Bigquery Upload
    bq_project_id = "between-buy-and-sell"
    bq_dataset_id ="stock"
    schema = Schema()
    
    print()
    copy_local_csv_files_to_bq_table(
        local_dirpath='./downloads/bigquery_tables',
        bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name="master_date",
        schema=schema.MASTER_DATE
    )






#     bq_uploader = BqUploader()
#     # bq_uploader.upload_summaries_to_bq()
#     bq_uploader.upload_history_to_bq()

#     print("Done")