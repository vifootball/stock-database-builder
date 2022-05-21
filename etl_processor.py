import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
from tqdm.notebook import tqdm
from bs4 import BeautifulSoup as bs
import yfinance as yf
import pandas_datareader.data as web
from constants import *

# 티커가 바뀔 수 있으니, 마스터를 add 방식으로 수집하고 drop duplicates?

class EtlProcessor:
    def __init__(self):
        self.dir_download = DIRNAME_DOWNLOAD
        self.subdir_history_etf = SUBDIRNAME_HISTORY_ETF
        self.subdir_history_indices = SUBDIRNAME_HISTORY_INDICES
        self.subdir_history_currencies = SUBDIRNAME_HISTORY_CURRENCIES
        self.fname_meta_etf = FNAME_META_ETF
        self.fname_info_etf = FNAME_INFO_ETF
        self.fname_profile_etf = FNAME_PROFILE_ETF
        self.fname_master_etf = FNAME_MASTER_ETF
        self.fname_master_indices_yahoo = FNAME_MASTER_INDICES_YAHOO
        self.fname_master_indices_investpy = FNAME_MASTER_INDICES_INVESTPY
        self.fname_master_currencies = FNAME_MASTER_CURRENCIES
        self.fname_master_indices_fred = FNAME_MASTER_INDICES_FRED
        self.fname_recession = FNAME_RECESSION
        self.cols_etf_profile = COLS_PROFILE_ETF
        self.cols_etf_master = COLS_MASTER_ETF
        self.dict_cols_etf_info = DICT_COLS_ETF_INFO
        self.dict_cols_etf_profile = DICT_COLS_ETF_PROFILE
        self.list_dict_symbols_fred = LIST_DICT_SYMBOLS_FRED

        os.makedirs(self.dir_download, exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_indices), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_currencies), exist_ok=True)

    def _get_meta_etf(self):
        etf_meta = investpy.etfs.get_etfs(country='united states')
        etf_meta.to_csv(os.path.join(self.dir_download, self.fname_meta_etf), index=False)
        return etf_meta

    def _get_info_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))
        header = investpy.etfs.get_etf_information(etf_names[0], country='united states').columns.to_list()
        rows = []
        etf_names = etf_meta['name']

        for i, name in enumerate(tqdm((etf_names[:]), mininterval=0.5)):
            try:
                time.sleep(0.5)
                row = investpy.etfs.get_etf_information(name, country='united states').iloc[0].to_list()
                rows.append(row)
            except:
                print(f'Loop No.{i} | Error Ocurred While Getting Information of: {name}')
        
        etf_info = pd.DataFrame(rows, columns=header) 
        etf_info.to_csv(os.path.join(self.dir_download, self.fname_info_etf), index=False)
        return etf_info
    
    def _get_profile_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))
        header = self.cols_etf_profile
        rows = []
        symbols = etf_meta['symbol']

        for i, symbol in enumerate(tqdm((symbols[:]), mininterval=0.5)):
            try:
                time.sleep(0.5)
                etf = yf.Ticker(symbol)
                temp_df = etf.get_institutional_holders().T
                temp_df.columns = temp_df.iloc[0]
                temp_df = temp_df[1:]

                temp_df['Symbol'] = symbol
                temp_df['Fund Family'] = etf.info.get('fundFamily')

                row = temp_df[COLS_ETF_PROFILE].iloc[0].to_list()
                rows.append(row)
            except:
                print(f'Loop No.{i} | Error Ocurred While Getting Profile of: {symbol}')
            
        etf_profile = pd.DataFrame(rows, columns=header)
        etf_profile.to_csv(os.path.join(self.dir_download, self.fname_profile_etf), index=False)
        return etf_profile

    def construct_master_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))
        etf_info = pd.read_csv(os.path.join(self.dir_download, self.fname_info_etf))
        etf_profile = pd.read_csv(os.path.join(self.dir_download, self.fname_profile_etf))
        
        etf_meta.rename(columns={'asset_class': 'category'}, inplace=True)
        
        etf_info = etf_info[self.dict_cols_etf_info.keys()]
        etf_info.rename(columns=self.dict_cols_etf_info, inplace=True)
        etf_info['dividend_yield_rate'] = etf_info['dividend_yield_rate'].str.replace('%','').astype('float32')/100
        etf_info['1_year_change_rate'] = etf_info['1_year_change_rate'].str.replace('[ %]','', regex=True).astype('float32') /100 # 공백과 %기호 제거

        etf_profile = etf_profile[self.dict_cols_etf_profile.keys()]
        etf_profile.rename(columns=self.dict_cols_etf_profile, inplace=True)
        etf_profile['expense_ratio'] = etf_profile['expense_ratio'].str.replace('%','').astype('float32')

        etf_master = etf_meta.merge(etf_info, how='left', left_on='name', right_on='etf_name')
        etf_master = etf_master.merge(etf_profile, how='left', left_on='symbol', right_on='symbol')
        etf_master = etf_master[self.cols_etf_master]
        etf_master.to_csv(os.path.join(self.dir_download, self.fname_master_etf), index=False)
        return etf_master

    def get_master_indices_yahoo(self):
        dfs = []
        urls = [
            'https://finance.yahoo.com/world-indices',
            'https://finance.yahoo.com/commodities'
        ]

        for url in urls:
            response = requests.get(url)
            html = bs(response.text, "lxml")
            html_table = html.select("table")
            table = pd.read_html(str(html_table))
            df_indices = table[0][['Symbol','Name']]
            df_indices['full_name'] = df_indices['Name']
            df_indices['country'] = None
            df_indices['currency'] = None
            df_indices['category'] = 'index'
            df_indices.columns = df_indices.columns.str.lower().to_list()
            dfs.append(df_indices)

        df_yahoo = pd.concat(dfs).reset_index(drop=True)
        df_yahoo.to_csv(os.path.join(self.dir_download, self.fname_master_indices_yahoo), index=False)

        return df_yahoo

    def get_master_indices_investpy(self):
        countries = ['united states', 'south korea']
        df_indices = investpy.indices.get_indices()
        df_indices = df_indices[df_indices['country'].isin(countries)].reset_index(drop=True)
        df_indices['symbol'] = '^' + df_indices['symbol']
        df_indices['category'] = 'index'
        cols = ['symbol', 'name', 'full_name', 'country', 'currency', 'category']
        df_indices = df_indices[cols]
        
        df_indices.to_csv(os.path.join(self.dir_download, self.fname_master_indices_investpy), index=False)
        return df_indices

    def get_master_currencies(self):
        currencies = investpy.currency_crosses.get_currency_crosses()
        base_cur = ['KRW', 'USD']
        currencies = currencies[currencies['base'].isin(base_cur)].reset_index(drop=True)
        currencies['currency'] = currencies['base']
        currencies['category'] = 'currency'
        currency_to_country = {'USD': 'united states', 'KRW': 'south Korea'}
        currencies['country'] = currencies['currency'].map(currency_to_country)
        def _encode_symbol(name):
            base_cur, second_cur = name.split('/')
            if base_cur == 'USD':
                symbol = f'{second_cur}=X'
            else:
                symbol = f'{base_cur}{second_cur}=X'
            return symbol
        currencies['symbol'] = currencies['name'].apply(_encode_symbol)
        currencies = currencies[['symbol', 'name', 'full_name', 'country', 'currency', 'category']]
        
        currencies.to_csv(os.path.join(self.dir_download, self.fname_master_currencies), index=False)
        return currencies

    def get_master_indices_fred(self):
        df = pd.DataFrame(self.list_dict_symbols_fred)
        df.to_csv(os.path.join(self.dir_download,self.fname_master_indices_fred), index=False)
        return df

    def get_recession(self):
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        recession = web.DataReader('USREC', 'fred', start, end)
        recession = recession.reset_index(drop=False)
        recession['yyyy-mm'] = recession['DATE'].dt.to_period('M').astype('str')
        recession.to_csv(os.path.join(self.dir_download, self.fname_recession), index=False)
        return recession

    def integrate_master():
        pass

    def get_history():
        # for yahoo finance

        # for fred


        pass

    def summarize_history():
        pass