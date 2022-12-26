import os
from utils import *
from columns import *
from meta_data_collector  import MetaDataCollector
from history_collector import HistoryCollector

class DataConstructor(): # 단위함수를 이용하여 테이블을 만드는 녀석
    
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
    def construct_etf_aums():
        mdc = MetaDataCollector()
        etf_symbols = mdc.get_etf_symbols()[:]
        mdc.collect_raw_etf_aums(etf_symbols=etf_symbols)

        raw_etf_aums = concat_csv_files_in_dir(os.path.join("download", "etf_aum", "raw_etf_aum"))
        raw_etf_aums = raw_etf_aums.loc[raw_etf_aums['aum'].notna()]
        pp_etf_aums = mdc.preprocess_raw_etf_aums(raw_etf_aums)
        fpath_pp_etf_aums = os.path.join("download", "etf_aum", "etf_aums.csv")
        os.makedirs(os.path.dirname(fpath_pp_etf_aums), exist_ok=True)
        pp_etf_aums.to_csv(fpath_pp_etf_aums, index=False)


    @staticmethod
    def construct_etf_masters():
        pp_etf_metas = pd.read_csv(os.path.join("download", "etf_meta", "etf_metas.csv"))[COL_ETF_META]
        pp_etf_infos = pd.read_csv(os.path.join("download", "etf_info", "etf_infos.csv"))[COL_ETF_INFO]
        pp_etf_profiles = pd.read_csv(os.path.join("download", "etf_profile", "etf_profiles.csv"))[COL_ETF_PROFILE]
        pp_etf_aums = pd.read_csv(os.path.join("download", "etf_aum", "etf_aums.csv"))[COL_ETF_AUM]

        etf_masters = pp_etf_metas.merge(pp_etf_infos, how='left', on='symbol')
        etf_masters = etf_masters.merge(pp_etf_profiles, how='left', on='symbol')
        etf_masters = etf_masters.merge(pp_etf_aums, how='left', on='symbol')
        etf_masters = etf_masters[COL_MASTER]

        fpath_etf_masters = os.path.join("download", "master", "etf_masters.csv")
        os.makedirs(os.path.dirname(fpath_etf_masters), exist_ok=True)
        etf_masters.to_csv(fpath_etf_masters, index=False)

    @staticmethod
    def construct_index_yf_masters():
        index_yahoo_main_masters = pd.read_csv(os.path.join("download", "master", "index_yahoo_main_masters.csv"))
        index_fd_masters = pd.read_csv(os.path.join("download", "master", "index_fd_masters.csv"))
        index_investpy_masters = pd.read_csv(os.path.join("download", "master", "index_investpy_masters.csv"))

        index_yf_masters = pd.concat([
            index_yahoo_main_masters,
            index_fd_masters,
            index_investpy_masters
        ]).drop_duplicates(subset=['symbol'])

        fpath = os.path.join("download", "master", "index_yf_masters.csv")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        index_yf_masters.to_csv(fpath, index=False)

    @staticmethod
    def construct_index_yahoo_main_masters():
        mdc = MetaDataCollector()
        index_yahoo_masters = mdc.get_index_masters_from_yahoo_main()

        fpath = os.path.join("download", "master", "index_yahoo_main_masters.csv")
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
        etf_symbols = mdc.get_etf_symbols()[:]
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
    def construct_index_yf_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        index_yf_masters = pd.read_csv(os.path.join("download", "master", "index_yf_masters.csv"))
        index_yf_symbols = index_yf_masters["symbol"].to_list()[:]

        hc.collect_histories_from_yf(
            symbols=index_yf_symbols,
            save_dirpath=os.path.join("download", "index_yf_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "index_yf_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_yf_histories"
        )

    @staticmethod
    def construct_currency_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        currency_masters = pd.read_csv(os.path.join("download", "master", "currency_masters.csv"))
        currency_symbols = currency_masters["symbol"].to_list()[:]

        hc.collect_histories_from_yf(
            symbols=currency_symbols,
            save_dirpath=os.path.join("download", "currency_history")
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "currency_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_currency_histories"
        )

    @staticmethod
    def construct_index_fred_histories():
        mdc = MetaDataCollector()
        hc = HistoryCollector()
        index_fred_masters = pd.read_csv(os.path.join("download", "master", "index_fred_masters.csv"))
        index_fred_symbols = index_fred_masters["symbol"].to_list()

        hc.collect_histories_from_fred(
            symbols=index_fred_symbols,
            save_dirpath=os.path.join("download", "index_fred_history"),
        )
        save_dfs_by_chunk(
            get_dirpath=os.path.join("download", "index_fred_history"),
            put_dirpath=os.path.join("download", "history_chunk"),
            prefix_chunk="concatenated_fred_histories"
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

        os.makedirs(os.path.dirname(put_fpath_recents), exist_ok=True)
        recents.to_csv(put_fpath_recents, index=False)

    @staticmethod
    def construct_summaries(master, recent, fpath_summary):
        summaries = pd.merge(master, recent, how='inner', on='symbol')

        os.makedirs(os.path.dirname(fpath_summary), exist_ok=True)
        summaries.to_csv(fpath_summary, index=False)


if __name__ == '__main__':
    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')