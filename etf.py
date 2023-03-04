import os
import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import yahooquery
import financedatabase as fd
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from table import TableHandler
import table_config
from constants import AssetCategories
from common import *

class ETF:
    def __init__(self):
        self.src_fd_meta_table_handler = TableHandler(table_config=table_config.SRC_FD_META)
        self.trg_fd_meta_table_handler = TableHandler(table_config=table_config.TRG_FD_META)
        self.profile_table_handler = TableHandler(table_config=table_config.PROFILE)
        self.aum_table_handler = TableHandler(table_config=table_config.AUM)
        self.holdings_table_handler = TableHandler(table_config=table_config.HOLDINGS)
        self.metadata_table_handler = TableHandler(table_config=table_config.METADATA)

    def get_symbols(self) -> list:
        fd_meta = fd.select_etfs(category=None)
        fd_meta = pd.DataFrame(fd_meta).T.reset_index().rename(columns={"index": "symbol"})
        symbols = list(fd_meta["symbol"])
        symbols = [symbol for symbol in symbols if symbol.isalpha()]
        return symbols

    def get_fd_meta(self, symbol: str) -> pd.DataFrame:
        # get raw data
        symbol = symbol.lower()
        fd_meta_all = fd.select_etfs(category=None)
        fd_meta_all = pd.DataFrame(fd_meta_all).T.reset_index().rename(columns={"index": "symbol"})
        fd_meta = fd_meta_all[fd_meta_all['symbol'].str.lower()==symbol].reset_index(drop=True)

        # table handling
        table_handler = self.src_fd_meta_table_handler
        fd_meta = table_handler.rename_columns(fd_meta)
        fd_meta = table_handler.select_columns(fd_meta)
        
        if table_handler.is_empty(fd_meta):
            fd_meta = table_handler.append_na_row(fd_meta)
        
        return fd_meta

    def transfrom_fd_meta(self, fd_meta: pd.DataFrame) -> pd.DataFrame:
        def _asset_subcat_to_asset_cat(subcat):
            if subcat in AssetCategories.EQUITY:
                return "Equity"
            elif subcat in AssetCategories.BOND:
                return "Bond"
            elif subcat in AssetCategories.COMMODITY:
                return "Commodity"
            elif subcat in AssetCategories.OTHER:
                return "Other"
            else:
                return None

        if pd.isna(fd_meta['symbol'].squeeze()):
            fd_meta['asset_category'] = np.nan
            fd_meta['category'] = np.nan
        else:
            fd_meta['asset_category'] = fd_meta['asset_subcategory'].apply(_asset_subcat_to_asset_cat)
            fd_meta['category'] = 'etf'
            comm = ['pdbc', 'gld', 'gldm', 'iau'] # 카테고리 누락된 애들 중 눈에 띄는 것
            fd_meta.loc[fd_meta['symbol'].str.lower().isin(comm), "asset_category"] = "Commodity"
        
        table_handler = self.trg_fd_meta_table_handler
        table_handler.check_columns(fd_meta)
        return fd_meta



    def get_profile(self, symbol: str) -> pd.DataFrame:
        # get_raw_data
        profile = yf.Ticker(symbol.lower()).get_institutional_holders()
        # table handling
        table_handler = self.profile_table_handler
        if not isinstance(profile, pd.DataFrame): # 없는 종목일 경우 None 반환
            profile = pd.DataFrame(columns = table_handler.get_columns_to_select())
            profile = table_handler.append_na_row(profile)
        else: 
            profile = profile.T
            profile.columns = profile.iloc[0]
            profile = profile[1:].reset_index(drop=True)
            if list(profile.columns) != table_handler.get_src_columns(): # 있는 종목이어도 etf가 아닐수도 있음 
                profile = pd.DataFrame(columns = table_handler.get_columns_to_select())
                profile = table_handler.append_na_row(profile)
            else:
                profile = table_handler.rename_columns(profile)
                profile = table_handler.select_columns(profile)
        return profile

    def transform_profile(self, profile: pd.DataFrame):
        profile['expense_ratio'] = profile['expense_ratio'].apply(percentage_to_float)
        profile['net_assets'] = profile['net_assets'].apply(str_to_int)
        return profile

    def get_aum(self, symbol: str): # 2-3번에 나눠돌려야함 429에러 발생
        symbol = symbol.lower()
        url = Request(f"https://stockanalysis.com/etf/{symbol}/", headers={'User-Agent': 'Mozilla/5.0'})
        table_handler = self.aum_table_handler

        try:
            time.sleep(1)
            html = urlopen(url)
            bs_obj = bs(html, "html.parser")
            trs = bs_obj.find_all('tr')
            for tr in (trs):
                try:
                    if "Assets" in tr.find_all('td')[0].get_text():
                        aum = tr.find_all('td')[1].get_text().replace("$", "")
                        break
                except:
                    continue
            for tr in (trs):
                try:
                    if "Shares Out" in tr.find_all('td')[0].get_text():
                        shares_out = tr.find_all('td')[1].get_text()
                        break
                except:
                    continue
            
            df = {'symbol': symbol, 'aum': aum, 'shares_out': shares_out}
            df = {'aum': aum, 'shares_out': shares_out}
            df = pd.DataFrame.from_dict(df, orient='index').T.reset_index(drop=True)
            print(f'[{symbol}]')
            print(df)
            return df
        
        except:
            aum = pd.DataFrame(columns = table_handler.get_columns_to_select())
            aum = table_handler.append_na_row(aum)            
            return aum

    def transform_aum(self, aum: pd.DataFrame) -> pd.DataFrame:
        aum['aum'] = aum['aum'].apply(str_to_int)
        aum['shares_out'] = aum['shares_out'].apply(str_to_int)
        return aum

    def get_holdings(self, symbol: str) -> pd.DataFrame:
        # get raw data
        symbol = symbol.lower()
        holdings = yahooquery.Ticker(symbol).fund_holding_info[symbol]

        # table handling
        table_handler = self.holdings_table_handler
        if isinstance(holdings, str): # 없으면 str로 메시지 반환
            holdings = pd.DataFrame(columns = table_handler.get_columns_to_select())
            holdings = table_handler.append_na_row(holdings)                                 
        else: 
            holdings = pd.json_normalize(holdings)
            holdings = table_handler.rename_columns(holdings)

            header = pd.DataFrame(columns = table_handler.get_columns_to_select())
            holdings = pd.concat([header, holdings], axis=0) # maturity나 durationr같은 없는 컬럼을 미리 추가
            holdings = table_handler.select_columns(holdings)            
        return holdings

    def get_metadata(self, symbol: str) -> pd.DataFrame:
        fd_meta = self.transfrom_fd_meta(self.get_fd_meta(symbol))
        profile = self.transform_profile(self.get_profile(symbol))
        aum = self.transform_aum(self.get_aum(symbol))
        holdings = self.get_holdings(symbol)

        # table handling
        table_handler = self.metadata_table_handler
        metadata = pd.concat([fd_meta, profile, aum, holdings], axis=1)
        metadata = table_handler.rename_columns(metadata)
        metadata = table_handler.select_columns(metadata)
        return metadata