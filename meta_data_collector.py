import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt

import investpy
import yfinance as yf
import financedatabase as fd
import pandas_datareader.data as web
from bs4 import BeautifulSoup as bs

from utils import *
from constants import *
from preprocessor import *

pd.options.mode.chained_assignment = None

class MetaDataCollector():

    @staticmethod
    def get_etf_symbols():
        etf_metas = fd.select_etfs(category=None)
        etf_metas = pd.DataFrame(etf_metas).T.reset_index().rename(columns={"index": "symbol"})
        etf_symbols = list(etf_metas["symbol"])
        return etf_symbols


    @staticmethod
    def get_raw_etf_metas():
        raw_etf_metas = fd.select_etfs(category=None)
        raw_etf_metas = pd.DataFrame(raw_etf_metas).T.reset_index()
        raw_etf_metas = raw_etf_metas.rename(columns={
            "index": "symbol",
            "family": "fund_family",
            "category": "asset_subcategory"
        })
        return raw_etf_metas

    @staticmethod
    def preprocess_raw_etf_metas(raw_etf_metas):
        pp_etf_metas = raw_etf_metas.copy()
        pp_etf_metas['country'] = None
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


    def get_raw_etf_info(self, symbol):
        time.sleep(0.5)
        raw_etf_info = yf.Ticker(symbol).info
        try:
            raw_etf_info = pd.json_normalize(raw_etf_info)[[  # only useful infos
                "symbol", "totalAssets", "sectorWeightings", "holdings", "bondRatings"
            ]].rename(columns={
                "symbol": "symbol",
                "totalAssets": "total_assets",
                "sectorWeightings": "sector_weight",
                "holdings": "holdings", # Top 10 Holdings
                "bondRatings": "bond_rating"
            })
        except:
            raw_etf_info = None
            print(f'Error Ocurred While Getting Information of: {symbol}')
        finally:
            return raw_etf_info
    
    def collect_raw_etf_infos(self, etf_symbols: list): # 임시로 indexing으로 끊어 사용
        for etf_symbol in etf_symbols:
            dirpath = ""
            fname = ""
            # fpath = os.makedirs(os.path.join(dirpath, fpath))
            raw_etf_info = self.get_raw_etf_info(etf_symbol)
            # raw_etf_info.to_csv(fpath, index=False)
            print(raw_etf_info)

    @staticmethod
    def preprocess_raw_etf_infos(raw_etf_infos): # None 처리
        if raw_etf_infos is not None:
            # Todo Later
            ### split sector_weight
            ### split holdings
            ### split bond_rating
            pass
        return raw_etf_infos
    

    def get_raw_etf_profile(self, symbol): # 6-7초 -> 16초, sleep해도 똑같음
        etf = yf.Ticker(symbol)
        try:
            time.sleep(0.5)
            raw_etf_profile = etf.get_institutional_holders().T
            raw_etf_profile.columns = raw_etf_profile.iloc[0]
            raw_etf_profile = raw_etf_profile[1:].rename(columns={
                'Net Assets': 'net_assets_abbv',
                'NAV': 'nav',
                'PE Ratio (TTM)': 'per_ttm',
                'Yield': 'yield',
                'YTD Daily Total Return': 'ytd_daily_total_return',
                'Beta (5Y Monthly)': 'beta_5y-monthly',
                'Expense Ratio (net)': 'expense_ratio',
                'Inception Date': 'inception_date'
            })
            raw_etf_profile['symbol'] = symbol
            #raw_etf_profile['fund_family'] = etf.info.get('fundFamily')
            if 'expense_ratio' not in raw_etf_profile.columns: # 구해져도 ETF가 아닌 경우가 있음
                raw_etf_profile= None
                print(f'Not ETF: {symbol}')
        except:
            raw_etf_profile = None
            print(f'Error Ocurred While Getting Profile of: {symbol}')

        finally:
            return raw_etf_profile
    
    def collect_raw_etf_profiles(self, etf_symbols: list):
        for etf_symbol in etf_symbols:
            dirpath = ""
            fname = ""
            # fpath = os.makedirs(os.path.join(dirpath, fpath))
            raw_etf_profile = self.get_raw_etf_profile(etf_symbol)
            # raw_etf_info.to_csv(fpath, index=False)
            print(raw_etf_profile)

    @staticmethod
    def preprocess_raw_etf_profiles(raw_etf_profiles):
        if raw_etf_profiles is not None:
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
            df = table[0][['Symbol','Name']].rename(columns={
                'Symbol': 'symbol',
                'Name': 'short_name'
            })
            df['long_name'] = df['short_name'].copy()
            df['category'] = 'index'
            df['country'] = None
            df['currency'] = None
            df['exchange'] = None
            dfs.append(df)

        dfs = pd.concat(dfs).reset_index(drop=True)#[COLS_MASTER_COMMON]
        # header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        # dfs = pd.concat([header, dfs]).reset_index(drop=True)
        return dfs
    
    @staticmethod
    def get_index_masters_from_fd():
        indices = fd.select_indices(market='kr_market') # 미국은 5만개라서 한국만 수집
        df = pd.DataFrame(indices).T.reset_index().rename(columns={'index': 'symbol'})
        df['long_name'] = df['short_name'].copy()
        df['category'] = 'index'
        df['country'] = None

        # header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        # df = pd.concat([header, df]).reset_index(drop=True)
        return df
    
    @staticmethod
    def get_index_masters_from_investpy(): # 미국 핵심만 추려서 있는 듯 함 # 미국&한국 수집
        pass

    @staticmethod
    def get_index_masters_from_fred():
        pass

    @staticmethod
    def get_currency_masters_From_fred():
        pass

    @staticmethod
    def get_currency_masters_from_fd():
        pass