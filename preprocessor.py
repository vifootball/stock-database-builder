import os
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

from utils import *
from constants import *
from metric_calculator import *
from directory_helper import DirectoryHelper

class Preprocessor():

    @staticmethod
    def preprocess_raw_etf_metas(raw_etf_metas):
        pp_etf_metas = raw_etf_metas.copy()
        pp_etf_metas['category'] = 'etf'
        return pp_etf_metas

    @staticmethod
    def preprocess_raw_etf_infos(raw_etf_infos): # None 처리
        if raw_etf_infos is not None:
            raw_etf_infos.rename(columns=DICT_COLS_RAW_INFO_ETF, inplace=True)
        return raw_etf_infos

    @staticmethod
    def preprocess_raw_etf_profiles(raw_etf_profiles):
        if raw_etf_profiles is not None:
            raw_etf_profiles.rename(columns=DICT_COLS_RAW_PROFILE_ETF, inplace=True)
            raw_etf_profiles['elapsed_year'] = round((dt.datetime.today() - pd.to_datetime(raw_etf_profiles['inception_date'])).dt.days/365, 1)
            raw_etf_profiles['expense_ratio'] = raw_etf_profiles['expense_ratio'].str.replace('%','').astype('float')/100
            raw_etf_profiles['net_assets_abbv'] = raw_etf_profiles['net_assets_abbv'].fillna("0")
            raw_etf_profiles['net_assets_sig_figs'] = raw_etf_profiles['net_assets_abbv'].str.extract('([0-9.]*)').astype('float')
            raw_etf_profiles['multiplier_mil'] = (raw_etf_profiles["net_assets_abbv"].str.endswith('M').astype('int') * (1000_000-1)) + 1
            raw_etf_profiles['multiplier_bil'] = (raw_etf_profiles["net_assets_abbv"].str.endswith('B').astype('int') * (1000_000_000-1)) + 1
            raw_etf_profiles['multiplier_tril'] = (raw_etf_profiles["net_assets_abbv"].str.endswith('T').astype('int') * (1000_000_000_000-1)) + 1
            raw_etf_profiles['net_assets'] = raw_etf_profiles['net_assets_sig_figs'] \
                                        * raw_etf_profiles['multiplier_mil'] \
                                        * raw_etf_profiles['multiplier_bil'] \
                                        * raw_etf_profiles['multiplier_tril']
        return raw_etf_profiles


    @measure_time
    def construct_master_etf(self):
        meta_etf = pd.read_csv(self.fpath_meta_etf)
        info_etf = pd.read_csv(self.fpath_info_etf)[COLS_INFO_ETF]
        profile_etf = pd.read_csv(self.fpath_profile_etf)[COLS_PROFILE_ETF]
                
        master_etf = meta_etf.merge(info_etf, how='left', on='name')
        master_etf = master_etf.merge(profile_etf, how='left', on='symbol')
        master_etf = master_etf[COLS_MASTER_ENTIRE]
        master_etf.to_csv(self.fpath_master_etf, index=False)
        return master_etf

    @measure_time
    def concat_master_indices(self):
        concat_csv_files_in_dir(
            get_dirpath=self.dirpath_master_indices,
            put_fpath=self.fpath_master_indices
        )

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
    
    def concat_history_pp(self,category): # n행씩 분할저장.. # 마지막거는 어케하지..?
        history_list = []
        total_len = 0
        chunk_num = 1
        
        path_dict = self.get_path_dict_by_category(category)
        dirpath_history_pp = path_dict.get('dirpath_history_pp')
        
        
        history_pp_generator =  (pd.read_csv(os.path.join(dirpath_history_pp, fname)) for fname in os.listdir(dirpath_history_pp) if fname.endswith('csv'))
        for history_pp in history_pp_generator:
            history_list.append(history_pp)
            total_len += len(history_pp)
            
            print(f'Concatenateing History of {category.capitalize()} | Chunk No.{chunk_num} | Total Length: {total_len}')
            if total_len > 1000_000:
                fpath_history_pp_concatenated = os.path.join(self.dirpath_history_pp_concatenated, f'history_pp_{category}_{chunk_num}.csv')
                df = pd.concat(history_list)
                df.to_csv(fpath_history_pp_concatenated, index=False)

                history_list = []
                total_len = 0
                chunk_num += 1
        
        if total_len > 0: # 나눠 떨어지지 않은 마지막 사이클 저장
            fpath_history_pp_concatenated = os.path.join(self.dirpath_history_pp_concatenated, f'history_pp_{category}_{chunk_num}.csv')
            df = pd.concat(history_list)
            df.to_csv(fpath_history_pp_concatenated, index=False)


    def construct_summary(self, category):
        path_dict = self.get_path_dict_by_category(category)
        master = pd.read_csv(path_dict.get('fpath_master'))
        recent = pd.read_csv(path_dict.get('fpath_recent'))
        fpath_summary = path_dict.get('fpath_summary')

        summary = pd.merge(master, recent, how='inner', on=['symbol', 'full_name'])
        summary.to_csv(fpath_summary, index=False)
        return summary



if __name__ == '__main__':
    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    # preprocessor = Preprocessor()
    # preprocessor.construct_master_etf()

    # preprocessor.concat_master_indices()

    # preprocessor.preprocess_history(category='etf')
    # preprocessor.preprocess_history(category='index')
    # preprocessor.preprocess_history(category='currency')

    # preprocessor.get_recent_from_history(category='etf')
    # preprocessor.get_recent_from_history(category='index')
    # preprocessor.get_recent_from_history(category='currency')

    # preprocessor.construct_summary(category='etf')
    # preprocessor.construct_summary(category='index')
    # preprocessor.construct_summary(category='currency')

    # preprocessor.concat_history_pp(category='index')
    # preprocessor.concat_history_pp(category='currency')
    # preprocessor.concat_history_pp(category='etf')



