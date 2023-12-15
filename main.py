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
from datetime import datetime

# path setting for Cron: 다음 명령어로 체크 print(os.path.abspath('.'))
os.chdir('/Users/chungdongwook/dongwook-src/stock-database-builder')

# load symbols
# Ray에서 동시 실행 시 에러가 나는 것을 방지하기 위해 enumerate로 감싸주고 순차적으로 데이터를 수집하도록 함

etf_master = pd.read_csv(os.path.join('downloads', 'master_symbols', 'masters_etf.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE]
etf_symbols = list(enumerate(etf_symbols))

currency_master = pd.read_csv(os.path.join('downloads', 'master_symbols', 'masters_currency.csv'))
currency_symbols = list(enumerate(currency_master['symbol'].to_list()))

indices_master_yahoo = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_yahoo.csv'))
indices_symbols_yahoo = list(enumerate(indices_master_yahoo['symbol'].to_list()))

indices_master_investpy = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_investpy.csv'))
indices_symbols_investpy = list(enumerate(indices_master_investpy['symbol'].to_list()))

indices_master_fd = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_fd.csv'))
indices_symbols_fd = list(enumerate(indices_master_investpy['symbol'].to_list()))

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

    ### Collect History
    ray.init(ignore_reinit_error=True,  num_cpus=8)
    
    tasks_collect_etf_history_from_yf = [collect_etf_history_from_yf.remote(order, symbol) for order, symbol in etf_symbols[:]]
    ray.get(tasks_collect_etf_history_from_yf)
    
    tasks_collect_currency_history_from_yf = [collect_currency_history_from_yf.remote(order, symbol) for order, symbol in currency_symbols[:]]
    ray.get(tasks_collect_currency_history_from_yf)

    tasks_collect_indices_investpy_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_investpy[:]]
    ray.get(tasks_collect_indices_investpy_history_from_yf)
    
    tasks_collect_indices_yahoo_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_yahoo[:]]
    ray.get(tasks_collect_indices_yahoo_history_from_yf)
    
    tasks_collect_indices_history_from_fred = [collect_indices_history_from_fred.remote(order,symbol) for order, symbol in indices_symbols_fred[:]]
    ray.get(tasks_collect_indices_history_from_fred)

    ### Make History Chunk
    save_dfs_by_chunk(
        get_dirpath="./downloads/history/currency/",
        put_dirpath="./downloads/bigquery_tables/history_chunks/",
        prefix_chunk="currency_chunk"
    )
    save_dfs_by_chunk(
        get_dirpath="./downloads/history/etf/",
        put_dirpath="./downloads/bigquery_tables/history_chunks/",
        prefix_chunk="etf_chunk"
    )
    save_dfs_by_chunk(
        get_dirpath="./downloads/history/indices/",
        put_dirpath="./downloads/bigquery_tables/history_chunks/",
        prefix_chunk="indices_chunk"
    )

    ### Calculate Summary Grade
    run_summary_grade_main()


    ### Bigquery Upload
    bq_project_id = "between-buy-and-sell"
    bq_dataset_id ="stock"
    schema = Schema()

    # HISTORY    
    copy_local_csv_files_to_bq_table(
        local_dirpath='./downloads/bigquery_tables/history_chunks/',
        bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='history',
        schema=schema.HISTORY
    )
    
    # DM_GRADE
    copy_local_csv_file_to_bq_table(
        local_fpath='./downloads/bigquery_tables/summary_grades.csv',
        bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='dm_grade',
        schema=schema.DM_GRADE
    )

    # DM_GRADE_PIVOT
    copy_local_csv_file_to_bq_table(
        local_fpath='./downloads/bigquery_tables/summary_grades_piv.csv',
        bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='dm_grade_pivot',
        schema=schema.DM_GRADE_PIVOT
    )

    copy_local_csv_file_to_bq_table(
        local_fpath='./downloads/bigquery_tables/master_date.csv',
        bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name="master_date",
        schema=schema.MASTER_DATE
    )

end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
print()
print(f"Done: {end_time}")