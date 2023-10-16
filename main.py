import os
import time
import ray
import pandas as pd
from constants import *
from history import *

if __name__ == '__main__':
    etf_master = pd.read_csv(os.path.join('downloads', 'masters_etf.csv'))
    etf_symbols = etf_master['symbol'].to_list()
    # print(etf_symbols)

    currency_master = pd.read_csv(os.path.join('downloads', 'masters_currency.csv'))
    currency_sumbols = currency_master['symbol'].to_list()
    print(currency_master)


    # 위에 ray 함수 몰아서 정의
    # 밑에 ray task 몰아서 정으;ㅣ

    @ray.remote
    def collect_etf_history_from_yf(symbol):
        time.sleep(2)
        history = get_history_from_yf(symbol)
        if history is not None:
            os.makedirs('downloads/history/etf', exist_ok=True)
            history.to_csv(f'downloads/history/etf/{symbol}_history.csv', index=False)
    symbols = etf_symbols[:50]
    print(symbols)
    tasks = [collect_etf_history_from_yf.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)

    @ray.remote
    def collect_currency_history_from_yf(symbol):
        time.sleep(0.5)
        history = get_history_from_yf(symbol)
        if history is not None:
            os.makedirs('downloads/history/currency', exist_ok=True)
            history.to_csv(f'downloads/history/currency/{symbol}_history.csv', index=False)
    symbols = currency_sumbols[:30]
    print(symbols)
    tasks = [collect_currency_history_from_yf.remote(symbol) for symbol in symbols]
    ray.init(ignore_reinit_error=True)
    ray.get(tasks)


#    @ray.remote
#     def collect_etf_master_yf(symbol):
#         time.sleep(2)
#         master_yf = get_etf_master_yf(symbol)
#         if master_yf is not None:
#             os.makedirs('downloads/masters_etf_yf', exist_ok=True)
#             master_yf.to_csv(f'downloads/masters_etf_yf/{symbol}_master_yf.csv', index=False)
#     # symbols = get_symbols()[:10]
#     symbols = get_symbols()[2190:]
#     tasks = [collect_etf_master_yf.remote(symbol) for symbol in symbols]
#     ray.init(ignore_reinit_error=True)
#     ray.get(tasks)
#     concat_csv_files_in_dir('downloads/masters_etf_yf').to_csv('downloads/masters_etf_yf.csv', index=False)






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