from raw_data_collector import *
from preprocessor import *

class DataConstructor():

    @staticmethod
    def construct_etf_metas():
        raw_etf_metas = RawDataCollector.get_raw_etf_metas()
        pp_etf_metas = Preprocessor.preprocess_raw_etf_metas(raw_etf_metas)
        export_df_to_csv(
            df=pp_etf_metas,
            fpath=os.path.join(
                DIR_DOWNLOAD,
                SUBDIR_ETF_META,
                FNAME_PP_ETF_METAS
            )
        )

    @staticmethod
    def construct_etf_infos():
        etf_names = RawDataCollector.get_etf_names()
        for etf_name in tqdm(etf_names, mininterval=0.5):
            raw_etf_info = RawDataCollector.get_raw_etf_info(etf_name)
        
        raw_etf_infos = concat_csv_files_in_dir(
            get_dirpath = os.path.join(
                DIR_DOWNLOAD, 
                SUBDIR_ETF_INFO, 
                SUBDIR_RAW_ETF_INFO
            )
        )

        pp_etf_infos = Preprocessor.preprocess_raw_etf_infos(raw_etf_infos)
        export_df_to_csv(
            df=pp_etf_infos,
            fpath=os.path.join(
                DIR_DOWNLOAD,
                SUBDIR_ETF_INFO,
                FNAME_PP_ETF_INFOS
            )
        )
    
    @staticmethod
    def construct_etf_profiles():
        etf_symbols = RawDataCollector.get_etf_symbols()
        for etf_symbol in tqdm(etf_symbols, mininterval=0.5):
            raw_etf_profile = RawDataCollector.get_raw_etf_profile(symbol=etf_symbol)
        
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
                FNAME_PP_ETF_PROFILES
            )
        )

    @staticmethod
    def construct_etf_masters():
        pp_etf_metas = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_META, FNAME_PP_ETF_METAS)
            )[COLS_PP_ETF_META]
        pp_etf_infos = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_INFO, FNAME_PP_ETF_INFOS)
            )[COLS_PP_ETF_INFO]
        pp_etf_profiles = pd.read_csv(
            os.path.join(DIR_DOWNLOAD, SUBDIR_ETF_PROFILE, FNAME_PP_ETF_PROFILES)
            )[COLS_PP_ETF_PROFILE]

        etf_masters = pp_etf_metas.merge(pp_etf_infos, how='left', on='name')
        etf_masters = etf_masters.merge(pp_etf_profiles, how='left', on='symbol')
        etf_masters = etf_masters[COLS_MASTER_ENTIRE]

    @staticmethod
    def construct_index_masters():
        pass



    # @measure_time
    # def concat_master_indices(self):
    #     concat_csv_files_in_dir(
    #         get_dirpath=self.dirpath_master_indices,
    #         put_fpath=self.fpath_master_indices
    #     )