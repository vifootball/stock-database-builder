import os
from utils import *
from columns import *
from meta_data_collector  import MetaDataCollector
from history_collector import HistoryCollector

class DataConstructor(): # 함수를 조합해서 데이터를 구성하고 저장하는 녀석
    
    @staticmethod
    def construct_etf_metas():
        mdc = MetaDataCollector()
        raw_etf_metas = mdc.get_raw_etf_metas()
        pp_etf_metas = mdc.preprocess_raw_etf_metas(raw_etf_metas)

        fpath_pp_etf_metas = os.path.join("download", "etf_meta", "etf_metas.csv")
        os.makedirs(os.path.dirname(fpath_pp_etf_metas), exist_ok=True)
        pp_etf_metas.to_csv(fpath_pp_etf_metas, index=False)

    @staticmethod
    def construct_etf_infos():
        mdc = MetaDataCollector()
        etf_symbols = mdc.get_etf_symbols()[:]
        mdc.collect_raw_etf_infos(etf_symbols=etf_symbols)
        
        raw_etf_infos = concat_csv_files_in_dir(os.path.join("download", "etf_info", "raw_etf_info"))
        pp_etf_infos = mdc.preprocess_raw_etf_infos(raw_etf_infos)
        fpath_pp_etf_infos = os.path.join("download", "etf_info", "etf_infos.csv")
        os.makedirs(os.path.dirname(fpath_pp_etf_infos), exist_ok=True)
        pp_etf_infos.to_csv(fpath_pp_etf_infos, index=False)
    
    @staticmethod
    def construct_etf_profiles():
        mdc = MetaDataCollector()
        etf_symbols = mdc.get_etf_symbols()[:]
        mdc.collect_raw_etf_profiles(etf_symbols=etf_symbols)
        
        raw_etf_profiles = concat_csv_files_in_dir(os.path.join("download", "etf_profile", "raw_etf_profile"))
        pp_etf_profiles = mdc.preprocess_raw_etf_profiles(raw_etf_profiles)
        fpath_pp_etf_profiles = os.path.join("download", "etf_profile", "etf_profiles.csv")
        os.makedirs(os.path.dirname(fpath_pp_etf_profiles), exist_ok=True)
        pp_etf_profiles.to_csv(fpath_pp_etf_profiles, index=False)

    @staticmethod
    def construct_etf_masters():
        pp_etf_metas = pd.read_csv(os.path.join("download", "etf_meta", "etf_metas.csv"))[COL_ETF_META]
        pp_etf_infos = pd.read_csv(os.path.join("download", "etf_info", "etf_infos.csv"))[COL_ETF_INFO]
        pp_etf_profiles = pd.read_csv(os.path.join("download", "etf_profile", "etf_profiles.csv"))[COL_ETF_PROFILE]

        etf_masters = pp_etf_metas.merge(pp_etf_infos, how='left', on='symbol')
        etf_masters = etf_masters.merge(pp_etf_profiles, how='left', on='symbol')
        etf_masters = etf_masters[COL_MASTER]

        fpath_etf_masters = os.path.join("download", "master", "etf_masters.csv")
        os.makedirs(os.path.dirname(fpath_etf_masters), exist_ok=True)
        etf_masters.to_csv(fpath_etf_masters, index=False)

    @staticmethod
    def construct_index_yahoo_masters():
        mdc = MetaDataCollector()
        index_yahoo_masters = mdc.get_index_masters_from_yahoo()

        fpath = os.path.join("download", "master", "index_yahoo_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True) 
        index_yahoo_masters.to_csv(fpath, index=False)
    
    @staticmethod
    def construct_index_fd_masters():
        mdc = MetaDataCollector()
        index_fd_masters = mdc.get_index_masters_from_fd()

        fpath = os.path.join("download", "master", "index_fd_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        index_fd_masters.to_csv(fpath, index=False)

    @staticmethod
    def construct_index_investpy_masters():
        mdc = MetaDataCollector()
        index_investpy_masters = mdc.get_index_masters_from_investpy()

        fpath = os.path.join("download", "master", "index_investpy_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        index_investpy_masters.to_csv(fpath, index=False) 

    @staticmethod
    def construct_index_fred_masters():
        mdc = MetaDataCollector()
        index_fred_masters = mdc.get_index_masters_from_fred()

        fpath = os.path.join("download", "master", "index_fred_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        index_fred_masters.to_csv(fpath, index=False)

    @staticmethod
    def construct_currency_masters():
        mdc = MetaDataCollector()
        currency_masters = mdc.get_currency_masters()

        fpath = os.path.join("download", "master", "currency_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        currency_masters.to_csv(fpath, index=False)
    
    @staticmethod
    def construct_etf_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        etf_symbols = mdc.get_etf_symbols()[:10]
        hc.collect_histories_from_yf(
            symbols=etf_symbols, 
            save_dirpath=os.path.join("download", "etf_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "etf_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_etf_histories"
        )

    @staticmethod
    def construct_index_yahoo_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        index_yahoo_masters = pd.read_csv(os.path.join("download", "master", "index_yahoo_masters.csv"))
        index_yahoo_symbols = index_yahoo_masters["symbol"].to_list()[:]

        hc.collect_histories_from_yf(
            symbols=index_yahoo_symbols, 
            save_dirpath=os.path.join("download", "index_yahoo_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "index_yahoo_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_index_yahoo_histories"
        )

    @staticmethod
    def construct_index_investpy_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        index_investpy_masters = pd.read_csv(os.path.join("download", "master", "index_investpy_masters.csv"))
        index_investpy_symbols = index_investpy_masters["symbol"].to_list()[:]

        hc.collect_histories_from_yf(
            symbols=index_investpy_symbols, 
            save_dirpath=os.path.join("download", "index_investpy_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "index_investpy_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_index_investpy_histories"
        )

    @staticmethod
    def construct_index_fd_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        index_fd_masters = pd.read_csv(os.path.join("download", "master", "index_fd_masters.csv"))
        index_fd_symbols = index_fd_masters["symbol"].to_list()[:10]

        hc.collect_histories_from_yf(
            symbols=index_fd_symbols, 
            save_dirpath=os.path.join("download", "index_fd_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "index_fd_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_index_fd_histories"
        )

    @staticmethod
    def construct_currency_histories():
        currency_symbols = RawDataCollector.get_currency_symbols()
        for currency_symbol in currency_symbols:
            print(f"{currency_symbol} processing...")
            raw_currency_history = RawDataCollector.get_raw_history_from_yf(currency_symbol)
            if raw_currency_history is not None:
                pp_currency_history = Preprocessor.preprocess_raw_history(raw_currency_history)
                export_df_to_csv(
                    df=pp_currency_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY, f"history_{currency_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_currency_histories"
        )

    @staticmethod
    def construct_index_fred_histories():
        index_symbols_fred = RawDataCollector.get_index_fred_symbols()
        for index_symbol in index_symbols_fred:
            print(f"{index_symbol} processing...")
            raw_index_history = RawDataCollector.get_raw_history_from_fred(index_symbol) #
            if raw_index_history is not None:
                pp_index_history = Preprocessor.preprocess_raw_history(raw_index_history)
                export_df_to_csv(
                    df=pp_index_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY, f"history_{index_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_fred_index_histories"
        )
        


    @staticmethod
    def construct_recents(get_dir_histories, put_fpath_recents):
        history_generator = (pd.read_csv(os.path.join(get_dir_histories, f)) for f in os.listdir(get_dir_histories) if f.endswith('csv'))        
        recents = []
        for history in tqdm(history_generator, mininterval=0.5, total=len(os.listdir(get_dir_histories))):
            history = history.loc[history['close'].notnull()] # 휴장일을 제외한 최신 데이터
            recent = history.iloc[-1]
            recents.append(recent)
        recents = pd.DataFrame(recents).reset_index(drop=True)
        export_df_to_csv(
            df=recents,
            fpath=os.path.join(put_fpath_recents)
        )

    @staticmethod
    def construct_summaries(master, recent, fpath_summary):
        summary = pd.merge(master, recent, how='inner', on='symbol')
        export_df_to_csv(
            df=summary,
            fpath=fpath_summary
        )


if __name__ == '__main__':
    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')
    
    # DataConstructor.construct_etf_metas() 
    # DataConstructor.construct_etf_infos()  # 현재 안됨
    # DataConstructor.construct_etf_profiles()

    # DataConstructor.construct_etf_masters()
    # DataConstructor.construct_index_yahoo_masters()
    # DataConstructor.construct_index_investpy_masters()
    # DataConstructor.construct_index_fred_masters()
    # DataConstructor.construct_currency_masters()

    # DataConstructor.construct_index_fred_histories()
    # DataConstructor.construct_index_yahoo_histories()
    # DataConstructor.construct_index_investpy_histories()
    # DataConstructor.construct_currency_histories()
    # DataConstructor.construct_etf_histories()

    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY),
    #     put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_FRED_RECENTS)
    # )
    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_INVESTPY_HISTORY),
    #     put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_INVESTPY_RECENTS)
    # )
    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_YAHOO_HISTORY),
    #     put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_YAHOO_RECENTS)
    # )
    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY),
    #     put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_CURRENCY_RECENTS)
    # )
    # DataConstructor.construct_recents(
    #     get_dir_histories=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_HISTORY),
    #     put_fpath_recents=os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_ETF_RECENTS)
    # )

    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_ETF_MASTERS)),
    #     recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_ETF_RECENTS)),
    #     fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_ETF_SUMMARIES)
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_CURRENCY_MASTERS)),
    #     recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_CURRENCY_RECENTS)),
    #     fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_CURRENCY_SUMMARIES)
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_INVESTPY_MASTERS)),
    #     recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_INVESTPY_RECENTS)),
    #     fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_INVESTPY_SUMMARIES)
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_YAHOO_MASTERS)),
    #     recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_YAHOO_RECENTS)),
    #     fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_YAHOO_SUMMARIES)
    # )
    # DataConstructor.construct_summaries(
    #     master=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_FRED_MASTERS)),
    #     recent=pd.read_csv(os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_INDEX_FRED_RECENTS)),
    #     fpath_summary=os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_INDEX_FRED_SUMMARIES)
    # )