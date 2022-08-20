import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
from constants import *
import investpy
import yfinance as yf
import pandas_datareader.data as web
from utils import *


class RawDataCollector:
    def __init__(self):
        ###
        self.dirpath_download = DIR_DOWNLOAD
        self.dirpath_profile_etf = os.path.join(self.dirpath_download, SUBDIR_PROFILE_ETF)
        self.dirpath_info_etf = os.path.join(self.dirpath_download, SUBDIR_INFO_ETF)
        self.dirpath_master = os.path.join(self.dirpath_download, SUBDIR_MASTER)
        self.dirpath_master_indices = os.path.join(self.dirpath_download, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES)
        self.dirpath_history_raw_etf = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_ETF)
        self.dirpath_history_raw_indices = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_ETF)
        self.dirpath_history_raw_currencies = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_CURRENCIES)
        self.dirpath_history_pp_etf = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_ETF)
        self.dirpath_history_pp_indices = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_INDICES)
        self.dirpath_history_pp_currencies = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_CURRENCIES)
        self.dirpath_history_pp_concatenated = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_CONCATENATED)
        self.dirpath_recent = os.path.join(self.dirpath_download, SUBDIR_RECENT)
        self.dirpath_summary = os.path.join(self.dirpath_download, SUBDIR_SUMMARY)

        os.makedirs(self.dirpath_download, exist_ok=True)
        os.makedirs(self.dirpath_profile_etf, exist_ok=True)
        os.makedirs(self.dirpath_info_etf, exist_ok=True)
        os.makedirs(self.dirpath_master, exist_ok=True)
        os.makedirs(self.dirpath_master_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_etf, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_currencies, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_etf, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_currencies, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_concatenated, exist_ok=True)
        os.makedirs(self.dirpath_recent, exist_ok=True)
        os.makedirs(self.dirpath_summary, exist_ok=True)

        self.fpath_meta_etf = os.path.join(self.dirpath_download, FNAME_META_ETF)
        self.fpath_info_etf = os.path.join(self.dirpath_download, FNAME_INFO_ETF)
        self.fpath_profile_etf = os.path.join(self.dirpath_download, FNAME_PROFILE_ETF)
        self.fpath_master_etf = os.path.join(self.dirpath_master, FNAME_MASTER_ETF)
        self.fpath_master_currencies = os.path.join(self.dirpath_master_indices, FNAME_MASTER_CURRENCIES)
        self.fpath_master_indices_yahoo = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_YAHOO)
        self.fpath_master_indices_investpy = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_INVESTPY)
        self.fpath_master_indices_fred = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_FRED)
        self.fpath_master_indices = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES)
        self.fpath_recent_etf = os.path.join(self.dirpath_recent, FNAME_RECENT_ETF)
        self.fpath_recent_indices = os.path.join(self.dirpath_recent, FNAME_RECENT_INDICES)
        self.fpath_recent_currencies = os.path.join(self.dirpath_recent, FNAME_RECENT_CURRENCIES)
        self.fpath_history_pp_etf = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_ETF)
        self.fpath_history_pp_indices = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_INDICES)
        self.fpath_history_pp_currencies = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_CURRENCIES)
        self.fpath_summary_etf = os.path.join(self.dirpath_summary, FNAME_SUMMARY_ETF)
        self.fpath_summary_indices = os.path.join(self.dirpath_summary, FNAME_SUMMARY_INDICES)
        self.fpath_summary_currencies = os.path.join(self.dirpath_summary, FNAME_SUMMARY_CURRENCIES)

        self.cols_etf_info_to_master = COLS_ETF_INFO_TO_MASTER
        self.cols_etf_profile_to_master = COLS_ETF_PROFILE_TO_MASTER
        self.cols_etf_entire = COLS_MASTER_ENTIRE
        self.dict_cols_etf_info = DICT_COLS_ETF_INFO
        self.dict_cols_etf_profile = DICT_COLS_ETF_PROFILE
        self.dict_cols_history_raw = DICT_COLS_HISTORY_RAW
        self.dict_cols_recession = DICT_COLS_RECESSION
        self.list_dict_symbols_fred = LIST_DICT_SYMBOLS_FRED
    
    @measure_time
    def get_meta_etf(self):
        meta_etf = investpy.etfs.get_etfs(country='united states')
        meta_etf['category'] = 'etf'
        meta_etf.to_csv(self.fpath_meta_etf, index=False)
        return meta_etf
    
    @measure_time
    def get_info_etf(self): # takes about an hour # recommended to run monthly rather than daily
        etf_meta = pd.read_csv(self.fpath_meta_etf)#[:5]
        for row in tqdm(etf_meta.itertuples(), total=len(etf_meta), mininterval=0.5):
            i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            etf_name = getattr(row, 'name')

            try:
                etf_info = investpy.etfs.get_etf_information(etf_name, country='united states')
                time.sleep(0.5)
            except:
                print(f'Loop #{i} | Error Ocurred While Getting Information of: {symbol}')
                continue    

            etf_info.rename(columns=self.dict_cols_etf_info, inplace=True)
            etf_info.to_csv(os.path.join(self.dirpath_info_etf, f'info_{symbol}.csv'), index=False)

    @measure_time
    def get_profile_etf(self):
        etf_meta = pd.read_csv(self.fpath_meta_etf)#[:5]
        for row in tqdm(etf_meta.itertuples(), total=len(etf_meta), mininterval=0.5):
            time.sleep(1)
            i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            
            etf = yf.Ticker(symbol)
            try: 
                profile = etf.get_institutional_holders().T
            except: # return이 Noneype일때
                print(f'Loop #{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue

            profile.columns = profile.iloc[0]
            profile = profile[1:]

            if 'Expense Ratio (net)' not in profile.columns: # ETF가 아닐때
                print(f'Loop #{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue

            profile['symbol'] = symbol
            profile['fund_family'] = etf.info.get('fundFamily')

            profile.rename(columns=self.dict_cols_etf_profile, inplace=True)
            profile['expense_ratio'] = profile['expense_ratio'].str.replace('%','').astype('float')/100
            profile.rename(columns={'net_assets': 'net_assets_original'}, inplace=True)
            profile['net_assets_original'] = profile["net_assets_original"].fillna("0")
            profile['multiplier_mil'] = (profile["net_assets_original"].str.endswith('M').astype('int') * (1000_000-1)) + 1
            profile['multiplier_bil'] = (profile["net_assets_original"].str.endswith('B').astype('int') * (1000_000_000-1)) + 1
            profile['multiplier_tril'] = (profile["net_assets_original"].str.endswith('T').astype('int') * (1000_000_000_000-1)) + 1
            profile['net_assets'] = profile['net_assets_original'].str.extract('([0-9.]*)').astype('float')
            profile['net_assets'] *= profile['multiplier_mil'] * profile['multiplier_bil'] * profile['multiplier_tril']

            profile.rename(columns=self.dict_cols_etf_profile)
            fpath = os.path.join(self.dirpath_profile_etf, f'profile_{symbol}.csv')    
            profile.to_csv(fpath, index=False)



if __name__ == '__main__':
    print('hi')

    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    collector = RawDataCollector()
    # collector.get_meta_etf()
    # collector.get_info_etf()
    collector.get_profile_etf()