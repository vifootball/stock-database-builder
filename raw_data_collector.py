from locale import currency
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
from preprocessor import *

pd.options.mode.chained_assignment = None

class RawDataCollector():
    @staticmethod
    def get_raw_etf_metas():
        raw_etf_metas = investpy.etfs.get_etfs(country='united states')
        return raw_etf_metas

    @staticmethod
    def get_etf_names():
        pp_etf_metas = pd.read_csv(os.path.join(
            DIR_DOWNLOAD,
            SUBDIR_ETF_META,
            FNAME_PP_ETF_METAS
        ))
        etf_names = list(pp_etf_metas['name'])
        return etf_names

    @staticmethod
    def get_etf_symbols():
        pp_etf_metas = pd.read_csv(os.path.join(
            DIR_DOWNLOAD,
            SUBDIR_ETF_META,
            FNAME_PP_ETF_METAS
        ))
        etf_symbols = list(pp_etf_metas['symbol'])
        return etf_symbols

    @staticmethod
    def get_raw_etf_info(etf_name):
        try:
            time.sleep(0.5)
            raw_etf_info = investpy.etfs.get_etf_information(etf_name, country='united states')
        except:
            raw_etf_info = None
            print(f'Error Ocurred While Getting Information of: {etf_name}')
        finally:
            return raw_etf_info
    
    @staticmethod
    def get_raw_etf_profile(symbol):
        etf = yf.Ticker(symbol)
        try:
            raw_etf_profile = etf.get_institutional_holders().T
            raw_etf_profile.columns = raw_etf_profile.iloc[0]
            raw_etf_profile = raw_etf_profile[1:]
            raw_etf_profile['symbol'] = symbol
            raw_etf_profile['fund_family'] = etf.info.get('fundFamily')
            if 'Expense Ratio (net)' not in raw_etf_profile.columns: # 구해져도 ETF가 아닌 경우가 있음
                raw_etf_profile= None
                print(f'Not ETF: {symbol}')
        except:
            raw_etf_profile = None
            print(f'Error Ocurred While Getting Profile of: {symbol}')

        finally:
            return raw_etf_profile

    @staticmethod
    def get_index_masters_from_yahoo():
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
            df = table[0][['Symbol','Name']]
            df['full_name'] = df['Name']
            df['country'] = None
            df['currency'] = None
            df['category'] = 'index'
            df.columns = df.columns.str.lower().to_list()
            dfs.append(df)

        index_masters_yahoo = pd.concat(dfs).reset_index(drop=True)[COLS_MASTER_COMMON]
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        index_masters_yahoo = pd.concat([header, index_masters_yahoo])
        return index_masters_yahoo

    @staticmethod
    def get_index_masters_from_investpy():
        countries = ['united states', 'south korea']

        df = investpy.indices.get_indices()
        df = df[df['country'].isin(countries)].reset_index(drop=True)
        df['symbol'] = '^' + df['symbol']
        df['category'] = 'index'
        df = df[COLS_MASTER_COMMON]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        index_masters_invespty = pd.concat([header, df])
        return index_masters_invespty

    @staticmethod
    def get_index_masters_from_fred():
        df = pd.DataFrame(FRED_METAS)[COLS_MASTER_COMMON]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        index_masters_fred = pd.concat([header, df])
        
        return index_masters_fred

    @staticmethod
    def get_currency_masters():
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
        currencies = currencies[COLS_MASTER_COMMON]
        
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        currency_masters = pd.concat([header, currencies]) 
        return currency_masters

    @staticmethod
    def get_index_symbols():
        index_masters = pd.read_csv(os.path.join(
            DIR_DOWNLOAD, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES
        ))
        index_symbols = list(index_masters['symbol'])
        return index_symbols
    
    @staticmethod
    def get_index_symbols_fred():
        index_masters_fred = pd.rea_csv(os.path.join(
            DIR_DOWNLOAD, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES_FRED
        ))
        index_symbols_fred = list(index_masters_fred['symbol'])
        return index_symbols_fred

    @staticmethod
    def get_currency_symbols():
        currency_masters =  pd.read_csv(os.path.join(
            DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_MASTER_CURRENCIES
        ))
        currency_symbols = list(currency_masters['symbol'])
        return currency_symbols

    @staticmethod
    def get_raw_history_from_yf(symbol):
        history = yf.Ticker(symbol).history(period='max')
        history = history.reset_index()
        history.rename(columns=COLS_MAPPER_RAW_HISTORY, inplace=True)

        if (len(history) > 50):
            if  (days_from_last_traded := dt.datetime.today() - history['date'].max()) < pd.Timedelta('50 days'):
                history['date'] = history['date'].astype('str')
                history['symbol'] = symbol
                history = history[COLS_HISTORY_RAW]
        else:
            history = None
         
        return history

    @staticmethod
    def get_raw_history_from_fred(symbol):
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        try: 
            history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
            history = history.reset_index()
            history.rename(columns={f'{symbol}':'close'}, inplace=True)
            history.rename(columns={'DATE':'date'}, inplace=True)
            history['symbol'] = 'symbol'
            history['date'] = history['date'].astype('str')

            header = pd.DataFrame(columns=COLS_HISTORY_RAW)
            history = pd.concat([header, history])
        
        except:
            history = None

        return history

    @staticmethod
    def get_recent_from_history(history):
        pass

# if __name__ == '__main__':
#     print('hi')

#     if 'stock-database-builder' in os.listdir():
#         os.chdir('stock-database-builder')

#     collector = RawDataCollector()
#     ETF
#     collector.get_meta_etf()
#     collector.get_info_etf()
#     collector.get_profile_etf()

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