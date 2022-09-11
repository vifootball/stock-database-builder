from utils import *
from directory_helper import DirectoryHelper
from raw_data_collector import RawDataCollector
from preprocessor import Preprocessor

class EtlProcesssor():
    
    @staticmethod
    def etl_meta_etf():
        raw_meta_etf = RawDataCollector.get_meta_etf()
        meta_etf = Preprocessor.preprocess_meta_etf(raw_meta_etf)
        export_df_to_csv(
            df=meta_etf,
            fpath=DirectoryHelper.get_path_dict(category='etf').get('fpath_meta_etf') 
        )

    @staticmethod
    @measure_time
    def etl_info_etf():
        meta_etf = pd.read_csv(DirectoryHelper.get_path_dict(category='etf').get('fpath_meta_etf'))
        for row in tqdm(meta_etf.itertuples(), total=len(meta_etf), mininterval=0.5):
            # etl이 모두 성공해야 하니까 try가 여기 있어야 하나 아니면 추적할 수 있도록 냅둬야하나
            raw_info_etf = RawDataCollector.get_info_etf(etf_name=getattr(row, 'name'))
            info_etf = Preprocessor.preprocess_info_etf(raw_info_etf)
            export_df_to_csv(
                df=info_etf,
                fpath=os.path.join(
                    DirectoryHelper.get_path_dict(category='etf').get('dirpath_info_etf'),
                    f"info_{(symbol := getattr(row, 'symbol'))}.csv"
                )
            )
        Preprocessor.concat_info_etf()


    @staticmethod
    def etl_profile_etf():
        meta_etf = pd.read_csv(DirectoryHelper.get_path_dict(category='etf').get('fpath_meta_etf'))
        for row in tqdm(meta_etf.itertuples(), total=len(meta_etf), mininterval=0.5):
            raw_profile_etf = RawDataCollector.get_profile_etf(symbol= getattr(row, 'symbol'))
            profile_etf = Preprocessor.preprocess_profile_etf(raw_profile_etf)
            export_df_to_csv(
                df=profile_etf,
                fpath=os.path.join(
                    DirectoryHelper.get_path_dict(category='etf').get('dirpath_profile_etf'),
                    f"profile_{(symbol := getattr(row, 'symbol'))}.csv"
                )
            )
        Preprocessor.concat_profile_etf()

if __name__ == '__main__':

    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    # EtlProcesssor.etl_meta_etf()
    # EtlProcesssor.etl_info_etf()
    EtlProcesssor.etl_profile_etf()
    