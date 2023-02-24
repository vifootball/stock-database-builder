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
from new_table import TableHandler
import new_table_config
from new_constants import AssetCategories

class ETF():
    def __init__(self):
        self.fd_meta_table_handler = TableHandler(table_config=new_table_config.FD_META)
        self.profile_table_handler = TableHandler(table_config=new_table_config.PROFILE)
        # self.holdings_table_handler = TableHandler(table_config=new_table_config.HOLDINGS)
        # self.config_fd_meta = new_table_config.FD_META
        # self.config_fd_meta = new_table_config.FD_META

    def get_symbols(self) -> list:
        fd_meta = fd.select_etfs(category=None)
        fd_meta = pd.DataFrame(fd_meta).T.reset_index().rename(columns={"index": "symbol"})
        symbols = list(fd_meta["symbol"])
        return symbols

    def get_fd_meta(self, symbol: str) -> pd.DataFrame:
        # get raw data
        symbol = symbol.lower()
        fd_meta_all = fd.select_etfs(category=None)
        fd_meta_all = pd.DataFrame(fd_meta_all).T.reset_index().rename(columns={"index": "symbol"})
        fd_meta = fd_meta_all[fd_meta_all['symbol'].str.lower()==symbol].reset_index(drop=True)

        # table handling
        table_handler = self.fd_meta_table_handler
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
        return fd_meta

    def get_profile(self, symbol):
        # get_raw_data
        profile = yf.Ticker(symbol).get_institutional_holders()
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

    # def transform_etf_profile(self, etf_profile: pd.DataFrame):
    #     # expense_ratio str to float
    #     def _convert_expense_ratio(expense_ratio):
    #         if pd.isna(expense_ratio):
    #             return expense_ratio
    #         else:
    #             return float(expense_ratio.replace('%',''))/100
    #     etf_profile['expense_ratio'] = etf_profile['expense_ratio'].apply(_convert_expense_ratio)

    #     # net_assets str to float
    #     def _convert_net_assets(net_assets):
    #         if pd.isna(net_assets):
    #              return net_assets
        
    #         multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
    #         suffix = net_assets[-1]
    #         if suffix.isdigit():
    #             return int(net_assets.replace(',', ''))
    #         else:
    #             return int(float(net_assets[:-1]) * multipliers[suffix])
    #     etf_profile['net_assets'] = etf_profile['net_assets'].apply(_convert_net_assets)

    #     return etf_profile






    # def get_etf_holdings(self, etf_symbol):
    #     # 1. 반환이 아무것도 안되면 expected column에 빈 행이 있는 데이터프레임 반환
    #     # 2. 반환되는데 missing column있으면 그 항목에만 null 채운 데이터프레임 반환
        
    #     # configure columns
    #     src_cols_info = {
    #         'maxAge': {'new_name': 'max_age', 'save': False},
    #         'stockPosition': {'new_name': 'stock_position', 'save': True},
    #         'bondPosition': {'new_name': 'bond_position', 'save': True},
    #         'holdings': {'new_name': 'holdings', 'save': True},
    #         'bondRatings': {'new_name': 'bond_ratings', 'save': True},
    #         'sectorWeightings': {'new_name': 'sector_weightings', 'save': True},
    #         'equityHoldings.priceToEarnings': {'new_name': 'price_to_earnings', 'save': False},
    #         'equityHoldings.priceToBook': {'new_name': 'price_to_book', 'save': False}, 
    #         'equityHoldings.priceToSales': {'new_name': 'price_to_sales', 'save': False},
    #         'equityHoldings.priceToCashflow': {'new_name': 'price_to_cashflow', 'save': False},
    #         'bondHoldings.maturity': {'new_name': 'maturity', 'save': True}, # 일부 채권에만 존재
    #         'bondHoldings.duration': {'new_name': 'duration', 'save': True} # 채권에만 존재
    #     }

    #     expected_cols = list(src_cols_info.keys())
    #     name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
    #     cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]

    #     # calling yahoo api
    #     etf_symbol = etf_symbol.lower()
    #     etf = yahooquery.Ticker(etf_symbol)
    #     etf_holdings = etf.fund_holding_info[etf_symbol]

    #     # If no holdings data is available, return an empty dataframe
    #     if isinstance(etf_holdings, str):                                          # 없으면 str로 메시지 반환
    #         etf_holdings = pd.DataFrame({col: [np.nan] for col in expected_cols})  # dict comprehension

    #     # If holdings data is available, check columns and return dataframe
    #     else: 
    #         etf_holdings = pd.json_normalize(etf_holdings)
    #         unexpected_cols = set(etf_holdings.columns) - set(expected_cols) 
    #         if unexpected_cols:
    #             raise ValueError(f"Unexpected columns in source data: {unexpected_cols}")
    #         # If missing values exist, set NaN
    #         header = pd.DataFrame(columns=expected_cols)
    #         etf_holdings = pd.concat([header, etf_holdings])

    #     etf_holdings = etf_holdings.rename(columns=name_mapping)[cols_to_save]
    #     return etf_holdings








# def get_etf_aum(self, etf_symbol): # 2-3번에 나눠돌려야함 429에러 발생
#     src_cols_info = {
#         'aum': {'new_name': 'aum', 'save': True},
#         'shares_out': {'new_name': 'shares_out', 'save': True}
#     }
#     expected_cols = list(src_cols_info.keys())
#     name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
#     cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]
    
#     etf_symbol = etf_symbol.lower()
#     url = Request(f"https://stockanalysis.com/etf/{etf_symbol}/", headers={'User-Agent': 'Mozilla/5.0'})


#     try:
#         time.sleep(1)
#         html = urlopen(url)
#         bs_obj = bs(html, "html.parser")
#         trs = bs_obj.find_all('tr')
#         for tr in (trs):
#             try:
#                 if "Assets" in tr.find_all('td')[0].get_text():
#                     aum = tr.find_all('td')[1].get_text().replace("$", "")
#                     break
#             except:
#                 continue
#         for tr in (trs):
#             try:
#                 if "Shares Out" in tr.find_all('td')[0].get_text():
#                     shares_out = tr.find_all('td')[1].get_text()
#                     break
#             except:
#                 continue
        
#         df = {'symbol': etf_symbol, 'aum': aum, 'shares_out': shares_out}
#         df = {'aum': aum, 'shares_out': shares_out}

#         df = pd.DataFrame.from_dict(df, orient='index').T.reset_index(drop=True)
#         return df
    
#     except:
#         print(f"Error Get AUM: {etf_symbol}")
#         etf_aum = pd.DataFrame({col: [np.nan] for col in expected_cols})
#         return etf_aum


# def transform_etf_aum(self, etf_aum: pd.DataFrame):
#     def _convert_str_to_number(num_str):
#         if pd.isna(num_str):
#              return num_str
        
#         multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
#         suffix = num_str[-1]
#         if suffix.isdigit():
#             return int(num_str.replace(',', ''))
#         else:
#             return int(float(num_str[:-1]) * multipliers[suffix])

#     etf_aum['aum'] = etf_aum['aum'].apply(_convert_str_to_number)
#     etf_aum['shares_out'] = etf_aum['shares_out'].apply(_convert_str_to_number)

#     return etf_aum



# # print(get_etf_symbols())
# print(get_etf_meta_fd("dbc"))
# # print(transfrom_etf_meta_fd(get_etf_meta_fd("gldg")))
# # print(get_etf_holdings("tlt"))
# # print(transform_etf_profile(get_etf_profile('xlv')))
# # print(transform_etf_aum(get_etf_aum('soxl')))




