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
        export_df_to_csv(
            df=raw_etf_metas, 
            fpath=os.path.join(
                DIR_DOWNLOAD, 
                SUBDIR_ETF_META, 
                FNAME_RAW_ETF_METAS
            )
        )
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
            export_df_to_csv(
                df=raw_etf_info,
                fpath=os.path.join(
                    DIR_DOWNLOAD,
                    SUBDIR_ETF_INFO,
                    SUBDIR_RAW_ETF_INFO,
                    f'raw_etf_info_{etf_name}.csv'
                )
            )
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
            else:
                export_df_to_csv(
                    df=raw_etf_profile,
                    fpath=os.path.join(
                        DIR_DOWNLOAD,
                        SUBDIR_ETF_PROFILE,
                        SUBDIR_ETF_PROFILE,
                        f'raw_etf_profile_{symbol}.csv'
                    )
                )
        except:
            raw_etf_profile = None
            print(f'Error Ocurred While Getting Profile of: {symbol}')

        finally:
            return raw_etf_profile



    # @measure_time
    # def get_master_indices_yahoo(self):
    #     dfs = []
    #     urls = [
    #         'https://finance.yahoo.com/world-indices',
    #         'https://finance.yahoo.com/commodities'
    #     ]

    #     for url in urls:
    #         response = requests.get(url)
    #         html = bs(response.text, "lxml")
    #         html_table = html.select("table")
    #         table = pd.read_html(str(html_table))
    #         df_indices = table[0][['Symbol','Name']]
    #         df_indices['full_name'] = df_indices['Name']
    #         df_indices['country'] = None
    #         df_indices['currency'] = None
    #         df_indices['category'] = 'index'
    #         df_indices.columns = df_indices.columns.str.lower().to_list()
    #         dfs.append(df_indices)

    #     df_yahoo = pd.concat(dfs).reset_index(drop=True)[COLS_MASTER_BASIC]
    #     header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
    #     df_yahoo = pd.concat([header, df_yahoo])
        
    #     df_yahoo.to_csv(self.fpath_master_indices_yahoo, index=False)
    #     return df_yahoo

    # @measure_time
    # def get_master_indices_investpy(self):
    #     countries = ['united states', 'south korea']

    #     df_indices = investpy.indices.get_indices()
    #     df_indices = df_indices[df_indices['country'].isin(countries)].reset_index(drop=True)
    #     df_indices['symbol'] = '^' + df_indices['symbol']
    #     df_indices['category'] = 'index'
    #     df_indices = df_indices[COLS_MASTER_BASIC]

    #     header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
    #     df_indices = pd.concat([header, df_indices])
        
    #     df_indices.to_csv(self.fpath_master_indices_investpy, index=False)
    #     return df_indices

    # @measure_time
    # def get_master_indices_fred(self):
    #     df = pd.DataFrame(self.list_dict_symbols_fred)[COLS_MASTER_BASIC]

    #     header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
    #     df = pd.concat([header, df])
        
    #     df.to_csv(self.fpath_master_indices_fred, index=False)
    #     return df

    # @measure_time
    # def get_master_currencies(self):
    #     currencies = investpy.currency_crosses.get_currency_crosses()
    #     base_cur = ['KRW', 'USD']
    #     currencies = currencies[currencies['base'].isin(base_cur)].reset_index(drop=True)
    #     currencies['currency'] = currencies['base']
    #     currencies['category'] = 'currency'
    #     currency_to_country = {'USD': 'united states', 'KRW': 'south Korea'}
    #     currencies['country'] = currencies['currency'].map(currency_to_country)
    #     def _encode_symbol(name):
    #         base_cur, second_cur = name.split('/')
    #         symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
    #         return symbol
    #     currencies['symbol'] = currencies['name'].apply(_encode_symbol)
    #     currencies = currencies[COLS_MASTER_BASIC]
        
    #     header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
    #     currencies = pd.concat([header, currencies]) 

    #     currencies.to_csv(self.fpath_master_currencies, index=False)
    #     return currencies

    # @measure_time
    # def get_history_from_yf(self, category):
    #     path_dict = self.get_path_dict_by_category(category)
    #     master = pd.read_csv(path_dict.get('fpath_master'))
    #     dirpath_history_raw = path_dict.get('dirpath_history_raw')

    #     for row in tqdm(master.itertuples(), total=len(master), mininterval=0.5):
    #         symbol = getattr(row, 'symbol')
    #         fpath = os.path.join(dirpath_history_raw, f'history_raw_{symbol}.csv')
    #         if True:
    #         # if not os.path.exists(fpath):
    #             time.sleep(0.1)
    #             i = getattr(row, 'Index') # enumerate i 의 용도
    #             history = yf.Ticker(symbol).history(period='max')
    #             history = history.reset_index()
    #             history.rename(columns=self.dict_cols_history_raw, inplace=True)
    #             if len(history) > 50:
    #                 days_from_last_traded = dt.datetime.today() - history['date'].max()
    #                 if days_from_last_traded < pd.Timedelta('100 days'):
    #                     # history = history.asfreq(freq = "1d").reset_index()
    #                     history['date'] = history['date'].astype('str')
    #                     # history['country'] = getattr(row, 'country') # 마스터에 있어서 필요 없음
    #                     history['symbol'] = getattr(row, 'symbol')
    #                     history['full_name'] = getattr(row, 'full_name')
    #                     history.to_csv(fpath, index=False)
    #             else:
    #                 print(f'Empty DataFrame at Loop {i}: {symbol}')

    # @measure_time
    # def get_history_from_fred(self):
    #     master_df = pd.read_csv(self.fpath_master_indices_fred)
    #     start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
    #     for row in tqdm(master_df.itertuples(), total=len(master_df)):
    #         i = getattr(row, 'Index')
    #         symbol = getattr(row, 'symbol')
    #         history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
    #         history = history.reset_index()
    #         history.rename(columns={f'{symbol}':'close'}, inplace=True)
    #         history.rename(columns={'DATE':'date'}, inplace=True)
    #         history['symbol'] = getattr(row, 'symbol')
    #         history['full_name'] = getattr(row, 'full_name')
    #         history['date'] = history['date'].astype('str')

    #         header = pd.DataFrame(columns=COLS_HISTORY_RAW)
    #         history = pd.concat([header, history])
    #         history.to_csv(os.path.join(self.dirpath_history_raw_indices, f'history_raw_{symbol}.csv'), index=False)


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