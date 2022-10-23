from importlib.resources import path
from operator import index
from raw_data_collector import *
from preprocessor import *

class DataConstructor(): # 함수를 조합해서 데이터를 구성하고 저장하는 녀석

    @staticmethod 
    def construct_etf_metas():
        raw_etf_metas = RawDataCollector.get_raw_etf_metas()
        pp_etf_metas = Preprocessor.preprocess_raw_etf_metas(raw_etf_metas)
        export_df_to_csv(
            df=pp_etf_metas,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_META, FNAME_ETF_METAS)
        )

    @staticmethod 
    def construct_etf_infos():
        etf_names = RawDataCollector.get_etf_names()
        for etf_name in tqdm(etf_names, mininterval=0.5):
            raw_etf_info = RawDataCollector.get_raw_etf_info(etf_name)
            if raw_etf_info is not None:
                export_df_to_csv(
                    df=raw_etf_info,
                    fpath=os.path.join(
                        DIR_DOWNLOAD, 
                        SUBDIR_ETF_INFO, 
                        SUBDIR_RAW_ETF_INFO,
                        f'raw_etf_info_{etf_name}.csv'
                    )
                )
        
        raw_etf_infos = concat_csv_files_in_dir(
            get_dirpath = os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_INFO,SUBDIR_RAW_ETF_INFO)
        )

        pp_etf_infos = Preprocessor.preprocess_raw_etf_infos(raw_etf_infos)
        export_df_to_csv(
            df=pp_etf_infos,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_INFO, FNAME_ETF_INFOS)
        )
    
    @staticmethod 
    def construct_etf_profiles():
        etf_symbols = RawDataCollector.get_etf_symbols()
        for etf_symbol in tqdm(etf_symbols, mininterval=0.5):
            raw_etf_profile = RawDataCollector.get_raw_etf_profile(symbol=etf_symbol)
            if raw_etf_profile is not None:
                export_df_to_csv(
                    df=raw_etf_profile,
                    fpath=os.path.join(
                        DIR_DOWNLOAD,
                        SUBDIR_ETF_PROFILE,
                        SUBDIR_RAW_ETF_PROFILE,
                        f'raw_etf_profile_{etf_symbol}.csv'
                    )
                )
        
        raw_etf_profiles = concat_csv_files_in_dir(
            get_dirpath=os.path.join(
                DIR_DOWNLOAD,
                SUBDIR_ETF_PROFILE,
                SUBDIR_RAW_ETF_PROFILE
            )
        )

        pp_etf_profiles = Preprocessor.preprocess_raw_etf_profiles(raw_etf_profiles)
        export_df_to_csv(
            df=pp_etf_profiles,
            fpath=os.path.join(
                DIR_DOWNLOAD,
                SUBDIR_ETF_PROFILE,
                FNAME_ETF_PROFILES
            )
        )

    @staticmethod 
    def construct_etf_masters():
        pp_etf_metas = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_META, FNAME_ETF_METAS)
            )[COLS_PP_ETF_META]
        pp_etf_infos = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_INFO, FNAME_ETF_INFOS)
            )[COLS_PP_ETF_INFO]
        pp_etf_profiles = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_PROFILE, FNAME_ETF_PROFILES)
            )[COLS_PP_ETF_PROFILE]

        etf_masters = pp_etf_metas.merge(pp_etf_infos, how='left', on='name')
        etf_masters = etf_masters.merge(pp_etf_profiles, how='left', on='symbol')
        etf_masters = etf_masters[COLS_MASTER_ENTIRE]
        export_df_to_csv(
            df=etf_masters,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_ETF_MASTERS)
        )

    @staticmethod 
    def construct_index_yahoo_masters():
        index_yahoo_masters = RawDataCollector.get_index_masters_from_yahoo()
        export_df_to_csv(
            df=index_yahoo_masters,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_YAHOO_MASTERS)
        )

    @staticmethod 
    def construct_index_investpy_masters():
        index_investpy_masters = RawDataCollector.get_index_masters_from_investpy()
        export_df_to_csv(
            df=index_investpy_masters,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_INVESTPY_MASTERS)
        )

    @staticmethod 
    def construct_index_fred_masters():
        index_fred_masters = RawDataCollector.get_index_masters_from_fred()
        export_df_to_csv(
            df=index_fred_masters,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_INDEX_FRED_MASTERS)
        )

    @staticmethod 
    def construct_currency_masters():
        currency_masters = RawDataCollector.get_currency_masters()
        export_df_to_csv(
            df=currency_masters,
            fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_CURRENCY_MASTERS)
        )
    
    @staticmethod
    def construct_etf_histories():
        etf_symbols = RawDataCollector.get_etf_symbols()
        for etf_symbol in etf_symbols:
            raw_etf_history = RawDataCollector.get_raw_history_from_yf(etf_symbol)
            if raw_etf_history is not None:
                pp_etf_history = Preprocessor.preprocess_history(raw_etf_history)
                export_df_to_csv(
                    df=pp_etf_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_HISTORY, "history_{etf_symbol}.csv"),
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_etf_histories"
        )

    @staticmethod
    def construct_currency_histories():
        currency_symbols = RawDataCollector.get_currency_symbols()
        for currency_symbol in currency_symbols:
            raw_currency_history = RawDataCollector.get_raw_history_from_yf(currency_symbol)
            if raw_currency_history is not None:
                pp_currency_history = Preprocessor.preprocess_history(raw_currency_history)
                export_df_to_csv(
                    df=pp_currency_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY, "history_{currency_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_CURRENCY_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_currency_histories"
        )
    
    @staticmethod
    def construct_index_yahoo_histories():
        index_symbols = RawDataCollector.get_index_yahoo_symbols()
        for index_symbol in index_symbols:
            raw_index_history = RawDataCollector.get_raw_history_from_yf(index_symbol) #
            if raw_index_history is not None:
                pp_index_history = Preprocessor.preprocess_history(raw_index_history)
                export_df_to_csv(
                    df=pp_index_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_YAHOO_HISTORY, "history_{index_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_YAHOO_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_index_yahoo_histories"
        )  

    @staticmethod
    def construct_index_investpy_histories():
        index_symbols = RawDataCollector.get_index_investpy_symbols()
        for index_symbol in index_symbols:
            raw_index_history = RawDataCollector.get_raw_history_from_yf(index_symbol) #
            if raw_index_history is not None:
                pp_index_history = Preprocessor.preprocess_history(raw_index_history)
                export_df_to_csv(
                    df=pp_index_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_INVESTPY_HISTORY, "history_{index_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_INVESTPY_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_index_investpy_histories"
        )  

    @staticmethod
    def construct_index_fred_histories():
        index_symbols_fred = RawDataCollector.get_index_fred_symbols()
        for index_symbol in index_symbols_fred:
            raw_index_history = RawDataCollector.get_raw_history_from_fred(index_symbol) #
            if raw_index_history is not None:
                pp_index_history = Preprocessor.preprocess_history(raw_index_history)
                export_df_to_csv(
                    df=pp_index_history,
                    fpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY, "history_{index_symbol}.csv")
                )
        Preprocessor.save_dfs_by_chunk(
            get_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_INDEX_FRED_HISTORY),
            put_dirpath=os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK),
            prefix_chunk="concatenated_fred_index_histories"
        )
        
    @staticmethod
    def construct_recents(get_dir_histories, put_fpath_recents):
        pass

    @staticmethod
    def construct_summaries(master, recent, fpath_summary):
        summary = pd.merge(master, recent, how='inner', on=['symbol'])
        export_df_to_csv(
            df=summary,
            fpath=fpath_summary
        )

  