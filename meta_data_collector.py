import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm

import investpy
import yfinance as yf
import financedatabase as fd
import pandas_datareader.data as web
from bs4 import BeautifulSoup as bs

from fred import *
from utils import *
from config import *
from columns import *
from constants import *

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
        raw_etf_metas = raw_etf_metas.reset_index() # 번호 부여
        return raw_etf_metas

    @staticmethod
    def preprocess_raw_etf_metas(raw_etf_metas):
        pp_etf_metas = raw_etf_metas.copy()
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
        for etf_symbol in tqdm(etf_symbols, mininterval=0.5):
            raw_etf_info = self.get_raw_etf_info(etf_symbol)
            if raw_etf_info is not None:
                dirpath = os.path.join("download", "etf_info", "raw_etf_info")
                fname = f"raw_etf_info_{etf_symbol}.csv"
                fpath = os.path.join(dirpath, fname)
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                raw_etf_info.to_csv(fpath, index=False)

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
            if 'expense_ratio' not in raw_etf_profile.columns: # 구해져도 ETF가 아닌 경우가 있음
                raw_etf_profile= None
                print(f'Not ETF: {symbol}')
        except:
            raw_etf_profile = None
            print(f'Error Ocurred While Getting Profile of: {symbol}')

        finally:
            return raw_etf_profile
    
    def collect_raw_etf_profiles(self, etf_symbols: list):
        for etf_symbol in tqdm(etf_symbols, mininterval=0.5):
            raw_etf_profile = self.get_raw_etf_profile(etf_symbol)
            if raw_etf_profile is not None:
                dirpath = os.path.join("download", "etf_profile", "raw_etf_profile")
                fname = f"raw_etf_profile_{etf_symbol}.csv"
                fpath = os.path.join(dirpath, fname)
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                raw_etf_profile.to_csv(fpath, index=False)

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
            pp_etf_profiles = raw_etf_profiles.copy()
        return pp_etf_profiles
    

    @staticmethod
    def get_index_masters_from_yahoo_main():
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
            dfs.append(df)

        dfs = pd.concat(dfs).reset_index(drop=True)
        header = pd.DataFrame(columns=COL_MASTER)
        dfs = pd.concat([header, dfs]).reset_index(drop=True)[COL_MASTER]
        return dfs
    
    @staticmethod
    def get_index_masters_from_fd():
        indices = fd.select_indices(market='kr_market') # 미국은 5만개라서 한국만 수집
        df = pd.DataFrame(indices).T.reset_index().rename(columns={'index': 'symbol'})
        df['long_name'] = df['short_name'].copy()
        df['category'] = 'index'

        header = pd.DataFrame(columns=COL_MASTER)
        index_masters_fd = pd.concat([header, df]).reset_index(drop=True)[COL_MASTER]
        return index_masters_fd
    
    @staticmethod
    def get_index_masters_from_investpy(): # 미국 핵심만 추려서 있는 듯 함 # 미국&한국 수집
        countries = ['united states', 'south korea']
        df = investpy.indices.get_indices()
        df = df[df['country'].isin(countries)].reset_index(drop=True)
        df['symbol'] = '^' + df['symbol']
        df['category'] = 'index'
        df['short_name'] = df['name'].copy()
        df['long_name'] = df['name'].copy()

        header = pd.DataFrame(columns=COL_MASTER)
        index_masters_invespty = pd.concat([header, df])[COL_MASTER]
        return index_masters_invespty

    @staticmethod
    def get_index_masters_from_fred():
        df = pd.DataFrame(FRED_METAS)
        header = pd.DataFrame(columns=COL_MASTER)
        index_masters_fred = pd.concat([header, df])[COL_MASTER]
        return index_masters_fred

    @staticmethod
    def get_currency_masters(): # fd에서도 가져올 수 있으나 symbol만 알 수 있음 #investpy에서는 symbol을 만들어줘야 함
        currencies = investpy.currency_crosses.get_currency_crosses()
        base_cur = ['KRW', 'USD']
        currencies = currencies[currencies['base'].isin(base_cur)].reset_index(drop=True)
        currencies['currency'] = currencies['base']
        currencies['category'] = 'currency'
        # currency_to_country = {'USD': 'united states', 'KRW': 'south Korea'}
        # currencies['country'] = currencies['currency'].map(currency_to_country)
        def _encode_symbol(name):
            base_cur, second_cur = name.split('/')
            symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
            return symbol
        currencies['symbol'] = currencies['name'].apply(_encode_symbol)
        currencies.rename(columns={
            'name': 'short_name',
            'full_name': 'long_name'
        }, inplace=True)
        
        header = pd.DataFrame(columns=COL_MASTER)
        currency_masters = pd.concat([header, currencies])[COL_MASTER]
        return currency_masters





