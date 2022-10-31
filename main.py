import os
import pandas as pd
from constants import *
from raw_data_collector import RawDataCollector
from data_constructor import DataConstructor
from preprocessor import Preprocessor
from bq_uploader import BqUploader


if __name__ == '__main__':
    print('hi')

    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

        DataConstructor.construct_index_fred_histories()
        DataConstructor.construct_index_yahoo_histories()
        DataConstructor.construct_index_investpy_histories()
        DataConstructor.construct_currency_histories()
        DataConstructor.construct_etf_histories()

        DataConstructor.construct_recents(
            get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY),
            put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_FRED_RECENTS)
        )
        DataConstructor.construct_recents(
            get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_INVESTPY_HISTORY),
            put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_INVESTPY_RECENTS)
        )
        DataConstructor.construct_recents(
            get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_YAHOO_HISTORY),
            put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_YAHOO_RECENTS)
        )
        DataConstructor.construct_recents(
            get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY),
            put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_CURRENCY_RECENTS)
        )
        DataConstructor.construct_recents(
            get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_HISTORY),
            put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_ETF_RECENTS)
        )

        DataConstructor.construct_summaries(
            master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_ETF_MASTERS)),
            recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_ETF_RECENTS)),
            fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_ETF_SUMMARIES)
        )
        DataConstructor.construct_summaries(
            master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_CURRENCY_MASTERS)),
            recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_CURRENCY_RECENTS)),
            fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_CURRENCY_SUMMARIES)
        )
        DataConstructor.construct_summaries(
            master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_INVESTPY_MASTERS)),
            recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_INVESTPY_RECENTS)),
            fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_INVESTPY_SUMMARIES)
        )
        DataConstructor.construct_summaries(
            master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_YAHOO_MASTERS)),
            recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_YAHOO_RECENTS)),
            fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_YAHOO_SUMMARIES)
        )
        DataConstructor.construct_summaries(
            master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_FRED_MASTERS)),
            recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_FRED_RECENTS)),
            fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_FRED_SUMMARIES)
        )

        bq_uploader = BqUploader()
        bq_uploader.upload_summaries_to_bq()
        bq_uploader.upload_history_to_bq()