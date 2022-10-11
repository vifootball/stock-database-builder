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
                SUBDIR_ETF_PROFILES
            )



