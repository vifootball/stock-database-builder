import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt

import investpy
import yfinance as yf
import pandas_datareader.data as web
from bs4 import BeautifulSoup as bs

from utils import *
from constants import *
from directory_builder import DirectoryBuilder

pd.options.mode.chained_assignment = None


class RawDataCollector(DirectoryBuilder):
    def __init__(self):
        ###
        super().__init__()

        self.dict_cols_info_etf = DICT_COLS_INFO_ETF
        self.dict_cols_profile_etf = DICT_COLS_PROFILE_ETF

        self.cols_info_etf = COLS_INFO_ETF
        self.cols_profile_etf = COLS_PROFILE_ETF
        
        self.cols_etf_entire = COLS_MASTER_ENTIRE

        self.dict_cols_history_raw = DICT_COLS_HISTORY_RAW
        self.list_dict_symbols_fred = LIST_DICT_SYMBOLS_FRED

    @measure_time
    def get_meta_etf(self):
        meta_etf = investpy.etfs.get_etfs(country='united states')
        meta_etf['category'] = 'etf'
        meta_etf.to_csv(self.fpath_meta_etf, index=False)
        return meta_etf
    
    @measure_time
    def get_info_etf(self): # takes about an hour 
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

            etf_info.rename(columns=self.dict_cols_info_etf, inplace=True)
            etf_info.to_csv(os.path.join(self.dirpath_info_etf, f'info_{symbol}.csv'), index=False)

    @measure_time
    def get_profile_etf(self): # takes about 8-10 hours # recommended to run monthly in weekend
        etf_meta = pd.read_csv(self.fpath_meta_etf)#[:5]
        for row in tqdm(etf_meta.itertuples(), total=len(etf_meta), mininterval=0.5):
            time.sleep(1)
            i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            
            etf = yf.Ticker(symbol)

            try: 
                profile = etf.get_institutional_holders().T
                profile.columns = profile.iloc[0]
                profile = profile[1:]
            except: # return이 Noneype일때
                print(f'Loop #{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue            

            if 'Expense Ratio (net)' not in profile.columns: # ETF가 아닐때
                print(f'Loop #{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue

            profile['symbol'] = symbol
            profile['fund_family'] = etf.info.get('fundFamily')

            profile.rename(columns=self.dict_cols_profile_etf, inplace=True)
            profile['elapsed_year'] = round((dt.datetime.today() - pd.to_datetime(profile['inception_date'])).dt.days/365, 1)
            profile['expense_ratio'] = profile['expense_ratio'].str.replace('%','').astype('float')/100
            profile.rename(columns={'net_assets': 'net_assets_original'}, inplace=True)
            profile['net_assets_original'] = profile["net_assets_original"].fillna("0")
            profile['multiplier_mil'] = (profile["net_assets_original"].str.endswith('M').astype('int') * (1000_000-1)) + 1
            profile['multiplier_bil'] = (profile["net_assets_original"].str.endswith('B').astype('int') * (1000_000_000-1)) + 1
            profile['multiplier_tril'] = (profile["net_assets_original"].str.endswith('T').astype('int') * (1000_000_000_000-1)) + 1
            profile['net_assets'] = profile['net_assets_original'].str.extract('([0-9.]*)').astype('float')
            profile['net_assets'] *= profile['multiplier_mil'] * profile['multiplier_bil'] * profile['multiplier_tril']

            profile.rename(columns=self.dict_cols_profile_etf)
            fpath = os.path.join(self.dirpath_profile_etf, f'profile_{symbol}.csv')    
            profile.to_csv(fpath, index=False)

    @measure_time
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

        df_yahoo = pd.concat(dfs).reset_index(drop=True)[COLS_MASTER_BASIC]
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df_yahoo = pd.concat([header, df_yahoo])
        
        df_yahoo.to_csv(self.fpath_master_indices_yahoo, index=False)
        return df_yahoo

    @measure_time
    def get_master_indices_investpy(self):
        countries = ['united states', 'south korea']

        df_indices = investpy.indices.get_indices()
        df_indices = df_indices[df_indices['country'].isin(countries)].reset_index(drop=True)
        df_indices['symbol'] = '^' + df_indices['symbol']
        df_indices['category'] = 'index'
        df_indices = df_indices[COLS_MASTER_BASIC]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df_indices = pd.concat([header, df_indices])
        
        df_indices.to_csv(self.fpath_master_indices_investpy, index=False)
        return df_indices

    @measure_time
    def get_master_indices_fred(self):
        df = pd.DataFrame(self.list_dict_symbols_fred)[COLS_MASTER_BASIC]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df = pd.concat([header, df])
        
        df.to_csv(self.fpath_master_indices_fred, index=False)
        return df

    @measure_time
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
            symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
            return symbol
        currencies['symbol'] = currencies['name'].apply(_encode_symbol)
        currencies = currencies[COLS_MASTER_BASIC]
        
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        currencies = pd.concat([header, currencies]) 

        currencies.to_csv(self.fpath_master_currencies, index=False)
        return currencies

    @measure_time
    def get_history_from_yf(self, category):
        path_dict = self.get_path_dict_by_category(category)
        master = pd.read_csv(path_dict.get('fpath_master'))
        dirpath_history_raw = path_dict.get('dirpath_history_raw')

        for row in tqdm(master.itertuples(), total=len(master), mininterval=0.5):
            symbol = getattr(row, 'symbol')
            fpath = os.path.join(dirpath_history_raw, f'history_raw_{symbol}.csv')
            if True:
            # if not os.path.exists(fpath):
                time.sleep(0.1)
                i = getattr(row, 'Index') # enumerate i 의 용도
                history = yf.Ticker(symbol).history(period='max')
                history = history.reset_index()
                history.rename(columns=self.dict_cols_history_raw, inplace=True)
                if len(history) > 50:
                    days_from_last_traded = dt.datetime.today() - history['date'].max()
                    if days_from_last_traded < pd.Timedelta('100 days'):
                        # history = history.asfreq(freq = "1d").reset_index()
                        history['date'] = history['date'].astype('str')
                        # history['country'] = getattr(row, 'country') # 마스터에 있어서 필요 없음
                        history['symbol'] = getattr(row, 'symbol')
                        history['full_name'] = getattr(row, 'full_name')
                        history.to_csv(fpath, index=False)
                else:
                    print(f'Empty DataFrame at Loop {i}: {symbol}')

    @measure_time
    def get_history_from_fred(self):
        master_df = pd.read_csv(self.fpath_master_indices_fred)
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        for row in tqdm(master_df.itertuples(), total=len(master_df)):
            i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
            history = history.reset_index()
            history.rename(columns={f'{symbol}':'close'}, inplace=True)
            history.rename(columns={'DATE':'date'}, inplace=True)
            history['symbol'] = getattr(row, 'symbol')
            history['full_name'] = getattr(row, 'full_name')
            history['date'] = history['date'].astype('str')

            header = pd.DataFrame(columns=COLS_HISTORY_RAW)
            history = pd.concat([header, history])
            history.to_csv(os.path.join(self.dirpath_history_raw_indices, f'history_raw_{symbol}.csv'), index=False)


if __name__ == '__main__':
    print('hi')

    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    collector = RawDataCollector()
    # ETF
    # collector.get_meta_etf()
    # collector.get_info_etf()
    collector.get_profile_etf()

    # Indices
    # collector.get_master_indices_yahoo()
    # collector.get_master_indices_investpy()
    # collector.get_master_indices_fred()

    # Currencies
    # collector.get_master_currencies()

    # History
    # collector.get_history_from_yf(category='etf')
    # collector.get_history_from_yf(category='index')
    # collector.get_history_from_yf(category='currency')

    # collector.get_history_from_fred()