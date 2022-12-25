import os
import pandas as pd
from constants import *
from data_constructor import DataConstructor
from bq_uploader import BqUploader


if __name__ == '__main__':
    print(os.getcwd())

    # path setting for window scheduler
    os.chdir(r"C:\Users\Dongwook Jung\Home\dongwook-src\stock-database-builder")
    
    print('hi')
    # print(os.getcwd())
    # print(os.listdir())

    # # Run Only Once a Year
    # DataConstructor.construct_etf_metas()
    # DataConstructor.construct_etf_profiles()
    # DataConstructor.construct_etf_infos()
    
    # Masters
    # DataConstructor.construct_etf_masters()
    # DataConstructor.construct_currency_masters()
    # DataConstructor.construct_index_yahoo_main_masters()
    # DataConstructor.construct_index_fd_masters()
    # DataConstructor.construct_index_investpy_masters()
    # DataConstructor.construct_index_yf_masters()
    # DataConstructor.construct_index_fred_masters()

    # Histories
    DataConstructor.construct_etf_histories()
    DataConstructor.construct_currency_histories()
    DataConstructor.construct_index_yf_histories()
    DataConstructor.construct_index_fred_histories()

    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join("download", "etf_history"),
    #     put_fpath_recents=os.path.join("download", "recent", "etf_recents.csv")
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join("download", "master", "etf_masters.csv")),
    #     recent=pd.read_csv(os.path.join("download", "recent", "etf_recents.csv")),
    #     fpath_summary=os.path.join("download", "summary", "etf_summaries.csv")
    # )

    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join("download", "currency_history"),
    #     put_fpath_recents=os.path.join("download", "recent", "currency_recents.csv")
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join("download", "master", "currency_masters.csv")),
    #     recent=pd.read_csv(os.path.join("download", "recent", "currency_recents.csv")),
    #     fpath_summary=os.path.join("download", "summary", "currency_summaries.csv")
    # )

    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join("download", "index_yf_history"),
    #     put_fpath_recents=os.path.join("download", "recent", "index_yf_recents.csv")
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join("download", "master", "index_yf_masters.csv")),
    #     recent=pd.read_csv(os.path.join("download", "recent", "index_yf_recents.csv")),
    #     fpath_summary=os.path.join("download", "summary", "index_yf_summaries.csv")
    # )

    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join("download", "index_fred_history"),
    #     put_fpath_recents=os.path.join("download", "recent", "index_fred_recents.csv")
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join("download", "master", "index_fred_masters.csv")),
    #     recent=pd.read_csv(os.path.join("download", "recent", "index_fred_recents.csv")),
    #     fpath_summary=os.path.join("download", "summary", "index_fred_summaries.csv")
    # )


    bq_uploader = BqUploader()
    # bq_uploader.upload_summaries_to_bq()
    bq_uploader.upload_history_to_bq()

    print("Done")