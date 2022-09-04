import os
import numpy as np
import pandas as pd
from tqdm import tqdm

from utils import *
from constants import *
from metric_calculator import *
from directory_builder import DirectoryBuilder

 
class Preprocessor(DirectoryBuilder):
    def __init__(self):
        ###
        super().__init__()

    @measure_time
    def concat_info_etf(self):
        concat_csv_files_in_dir(
            get_dirpath=self.dirpath_info_etf,
            put_fpath=self.fpath_info_etf
        )   

    @measure_time
    def concat_profile_etf(self):
        concat_csv_files_in_dir(
            get_dirpath=self.dirpath_profile_etf,
            put_fpath=self.fpath_profile_etf
        )

    @measure_time
    def construct_master_etf(self):
        meta_etf = pd.read_csv(self.fpath_meta_etf)
        info_etf = pd.read_csv(self.fpath_info_etf)[COLS_INFO_ETF]
        profile_etf = pd.read_csv(self.fpath_profile_etf)[COLS_PROFILE_ETF]
                
        master_etf = meta_etf.merge(info_etf, how='left', on='name')
        master_etf = master_etf.merge(profile_etf, how='left', on='symbol')
        master_etf.to_csv(self.fpath_master_etf, index=False)
        return master_etf

    def preprocess_history(self, category):
        path_dict = self.get_path_dict_by_category(category)
        dirpath_history_raw = path_dict.get('dirpath_history_raw')
        dirpath_history_pp = path_dict.get('dirpath_history_pp')

        history_raw_generator = (pd.read_csv(os.path.join(dirpath_history_raw, fname)) for fname in os.listdir(dirpath_history_raw) if fname.endswith('csv'))
        for history_raw in tqdm(history_raw_generator, mininterval=0.5, total=len(os.listdir(dirpath_history_raw))):    
            history_pp = calculate_metrics(history_raw)
            #history = self._join_benchmark
            symbol = history_pp['symbol'].iat[0]
            history_pp.to_csv(os.path.join(dirpath_history_pp, f'history_pp_{symbol}.csv'), index=False)
        print(f"Finished Preprocessing History: {category}")

    def get_recent_from_history(self, category):
        path_dict = self.get_path_dict_by_category(category)
        dirpath_history_pp = path_dict.get('dirpath_history_pp')
        fpath_recent = path_dict.get('fpath_recent')

        history_pp_generator = (pd.read_csv(os.path.join(dirpath_history_pp, fname)) for fname in os.listdir(dirpath_history_pp) if fname.endswith('csv'))        
        recents = []
        for history_pp in tqdm(history_pp_generator, mininterval=0.5, total=len(os.listdir(dirpath_history_pp))):
            history_pp = history_pp.loc[history_pp['close'].notnull()] # 휴장일을 제외한 최신 데이터
            recent = history_pp.iloc[-1]
            recents.append(recent)
        recents = pd.DataFrame(recents).reset_index(drop=True)
        recents.to_csv(fpath_recent, index=False)
        print(f"Finished Extracting Recent Data of Histories: {category}")
        return recents
    
    def construct_summary(self, category):
        path_dict = self.get_path_dict_by_category(category)
        master = pd.read_csv(path_dict.get('fpath_master'))
        recent = pd.read_csv(path_dict.get('fpath_recent'))
        fpath_summary = path_dict.get('fpath_summary')

        summary = pd.merge(master, recent, how='inner', on=['symbol', 'full_name'])
        summary.to_csv(fpath_summary, index=False)
        return summary



