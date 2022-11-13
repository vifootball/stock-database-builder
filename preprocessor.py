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
        pp_etf_metas.rename(columns={"family": "fund_family"}, inplace=True)
        pp_etf_metas.rename(columns={"category": "asset_subcategory"}, inplace=True)
        def _asset_subcat_to_asset_cat(subcat):
            if subcat in ASSET_CAT_EQT:
                return "Equity"
            elif subcat in ASSET_CAT_BND:
                return "Bond"
            elif subcat in ASSET_CAT_COM:
                return "Commodity"
            elif subcat in ASSET_CAT_OTH:
                return "Other"
            else:
                return None
        pp_etf_metas["asset_category"] = pp_etf_metas['asset_subcategory'].apply(_asset_subcat_to_asset_cat)
        pp_etf_metas['category'] = 'etf'
        comm = ['pdbc', 'gld', 'gldm', 'iau'] # 누락된 애들 중 눈에 띄는 것
        pp_etf_metas.loc[pp_etf_metas['symbol'].str.lower().isin(comm), "asset_category"] = "Commodity"
        return pp_etf_metas

    @staticmethod
    def preprocess_raw_etf_infos(raw_etf_infos): # None 처리
        if raw_etf_infos is not None:
            pass
            #raw_etf_infos.rename(columns=COL_MAPPER_RAW_ETF_INFO, inplace=True)
        return raw_etf_infos

    @staticmethod
    def preprocess_raw_etf_profiles(raw_etf_profiles):
        if raw_etf_profiles is not None:
            raw_etf_profiles.rename(columns=COL_MAPPER_RAW_ETF_PROFILE, inplace=True)
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

    @staticmethod
    def preprocess_raw_history(raw_history):
        history = calculate_metrics(raw_history)
        return history

    @staticmethod
    def save_dfs_by_chunk(get_dirpath, put_dirpath, prefix_chunk): # n행씩 분할저장.. # 마지막거는 어케하지..?
        df_list = []
        total_len = 0
        chunk_num = 1

        df_generator =  (pd.read_csv(os.path.join(get_dirpath, fname)) for fname in os.listdir(get_dirpath) if fname.endswith('csv'))
        for df in df_generator:
            df_list.append(df)
            total_len += len(df)
            print(f'Concatenateing Dfs in {get_dirpath} | Chunk No.{chunk_num} | Total Length: {total_len}')
            
            if total_len > 1000_000:
                df_concatenated = pd.concat(df_list)
                export_df_to_csv(
                    df=df_concatenated,
                    fpath=os.path.join(put_dirpath, f'{prefix_chunk}_{chunk_num}.csv')
                )

                df_list = []
                total_len = 0
                chunk_num += 1
        
        if total_len > 0: # 나눠 떨어지지 않은 마지막 사이클 저장
            df_concatenated = pd.concat(df_list)
            export_df_to_csv(
                df = df_concatenated,
                fpath=os.path.join(put_dirpath, f'{prefix_chunk}_{chunk_num}.csv')
            )




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



