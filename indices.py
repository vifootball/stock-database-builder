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
from common import *
import constants
import investpy

# 지수 데이터의 컬럼 헤더
indices_cols = [
    'type', 'symbol', 'name'
]
# etf_master와 형태를 맞추기 위한 컬럼 헤더
header_cols = [
    'type', 'symbol', 'name', 'fund_family', 'summary',
    'net_assets', 'expense_ratio', 'inception_date', 'yf_date',
    'aum', 'shares_out', 'sa_1_date',
    'holdings_count', 'top10_percentage', 'asset_class', 'sector', 'region', 'sa_2_date',
    'category', 'index_tracked', 'stock_exchange', 'description', 'sa_3_date'
    ]

def get_indices_masters_investpy() -> pd.DataFrame:
    # symbol 문자열 앞에 ^를 붙여야 함
    countries = ['united states', 'south korea']
    indices = investpy.get_indices()
    indices = indices[indices['country'].isin(countries)].reset_index(drop=True)
    indices['symbol'] = '^' + indices['symbol']
    indices['type'] = 'index'
    indices = indices[['type', 'symbol', 'full_name']]
    indices = indices.rename(columns={'full_name': 'name'})
    return indices


def get_indices_masters_fd() -> pd.DataFrame:
    # 미국은 5만개라서 한국만 수집
    indices = fd.Indices().select() 
    indices = indices[indices['market'] == 'kr_market'].reset_index()[['symbol', 'name']]
    indices['type'] = 'index'
    indices = indices[['type', 'symbol', 'name']]
    return indices


def get_indices_masters_yahoo() -> pd.DataFrame:
    indices_1 = constants.Indices.YAHOO_WORLD_INDICES
    indices_1 = pd.DataFrame(indices_1)

    indices_2 = constants.Indices.YAHOO_COMMODITIES
    indices_2 = pd.DataFrame(indices_2)

    indices = pd.concat([indices_1,indices_2]).reset_index()
    indices['type'] = 'index'
    indices = indices[['type', 'symbol', 'name']]
    return indices


def get_indices_masters_fred() -> pd.DataFrame:
    indices = constants.Indices.FRED
    indices = pd.DataFrame(indices)
    indices['type'] = 'index'
    indices = indices[['type', 'symbol', 'name']]
    return indices

def concat_indices_masters():
    # 중복제거 필요
    indices_masters_investpy = pd.read_csv('./downloads/masters_indices/masters_indices_investpy.csv')
    indices_masters_fd = pd.read_csv('./downloads/masters_indices/masters_indices_fd.csv')
    indices_masters_yahoo = pd.read_csv('./downloads/masters_indices/masters_indices_yahoo.csv')
    indices_masters_fred = pd.read_csv('./downloads/masters_indices/masters_indices_fred.csv')
    indices_masters = pd.concat([
        indices_masters_investpy,
        indices_masters_fd,
        indices_masters_yahoo,
        indices_masters_fred
    ]).reset_index(drop=True)
    indices_masters = indices_masters.drop_duplicates(subset='symbol')
    
    header = pd.DataFrame(columns=header_cols)
    indices_masters = pd.concat([header, indices_masters])
    indices_masters.to_csv('./downloads/masters_indices.csv', index=False)



if __name__ == "__main__":
    os.makedirs('./downloads/masters_indices', exist_ok=True)
    # get_indices_masters_investpy().to_csv('./downloads/masters_indices/masters_indices_investpy.csv', index=False)
    # get_indices_masters_fd().to_csv('./downloads/masters_indices/masters_indices_fd.csv', index=False)
    # get_indices_masters_yahoo().to_csv('./downloads/masters_indices/masters_indices_yahoo.csv', index=False)
    # get_indices_masters_fred().to_csv('./downloads/masters_indices/masters_indices_fred.csv', index=False)

    concat_indices_masters()


# class Indices:
#     def __init__(self):
#         self.metadata_table_handler = TableHandler(table_config=table_config.METADATA)
#         self.metadata_common_table_handler = TableHandler(table_config=table_config.METADATA_COMMON)

    # def get_metadata_from_yahoo_main(self) -> pd.DataFrame:
    #     # get raw data
    #     metadata = []
    #     urls = [
    #         'https://finance.yahoo.com/world-indices',
    #         'https://finance.yahoo.com/commodities'
    #     ]
    #     for url in urls:
    #         response = requests.get(url)
    #         html = bs(response.text, "lxml")
    #         html_table = html.select("table")
    #         table = pd.read_html(str(html_table))
    #         df = table[0][['Symbol','Name']].rename(columns={
    #             'Symbol': 'symbol_pk',
    #             'Name': 'name'
    #         })
    #         df['short_name'] = df['name'].copy()
    #         df['category'] = 'index'
    #         metadata.append(df)
    #     metadata = pd.concat(metadata).reset_index(drop=True)

    #     # table handling
    #     table_handler = self.metadata_table_handler
    #     header = pd.DataFrame(columns = table_handler.get_columns_to_select())
    #     metadata = pd.concat([header, metadata], axis=0)
    #     table_handler.check_columns(metadata)
    #     metadata = table_handler.select_columns(metadata)
    #     return metadata

    # def get_symbols_from_yahoo_main(self) -> list: # 웹스크래핑으로 가져오는 정보라 변할 수 있기 때문에 한번 불러와서 상수로 저장
    #     symbols = constants.Symbols.YAHOO_MAIN
    #     return symbols

    # def get_metadata_from_fd(self) -> pd.DataFrame:
    #     # get raw data
    #     metadata = fd.select_indices(market='kr_market') # 미국은 5만개라서 한국만 수집
    #     metadata = pd.DataFrame(metadata).T.reset_index().rename(columns={'index': 'symbol_pk'})
    #     metadata['name'] = metadata['short_name'].copy()
    #     metadata['category'] = 'index'

    #     # table handling
    #     metadata = metadata[self.metadata_common_table_handler.get_columns_to_select()]
    #     header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
    #     metadata = pd.concat([header, metadata], axis=0)
    #     self.metadata_table_handler.check_columns(metadata)
    #     metadata = self.metadata_table_handler.select_columns(metadata)
    #     return metadata

    # def get_symbols_from_fd(self) -> list:
    #     metadata = self.get_metadata_from_fd()
    #     symbols = metadata['symbol_pk'].to_list()
    #     return symbols

    # def get_metadata_from_investpy(self) -> pd.DataFrame:
    #     # get raw data
    #     countries = ['united states', 'south korea']
    #     metadata = investpy.indices.get_indices()
    #     metadata = metadata[metadata['country'].isin(countries)].reset_index(drop=True)
    #     metadata['symbol'] = '^' + metadata['symbol']
    #     metadata['category'] = 'index'
    #     metadata = metadata.rename(columns={
    #         'symbol': 'symbol_pk',
    #         'full_name': 'name',
    #         'name': 'short_name'
    #     })

    #     # table handling
    #     metadata = metadata[self.metadata_common_table_handler.get_columns_to_select()]
    #     header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
    #     metadata = pd.concat([header, metadata], axis=0)
    #     self.metadata_table_handler.check_columns(metadata)
    #     metadata = self.metadata_table_handler.select_columns(metadata)
    #     return metadata

    # def get_symbols_from_investpy(self) -> list:
    #     metadata = self.get_metadata_from_investpy()
    #     symbols = metadata['symbol_pk'].to_list()
    #     return symbols

    # def get_metadata_from_fred(self) -> pd.DataFrame:
    #     metadata = constants.Symbols.FRED
    #     metadata = pd.DataFrame.from_dict(metadata, orient='index', columns=['name'])
    #     metadata = metadata.reset_index().rename(columns={'index': 'symbol_pk'})
    #     metadata['short_name'] = metadata['name'].copy()
    #     metadata['category'] = 'index'

    #     header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
    #     metadata = pd.concat([header, metadata], axis=0)
    #     self.metadata_table_handler.check_columns(metadata)
    #     metadata = self.metadata_table_handler.select_columns(metadata)
    #     # table handling
    #     return metadata

    # def get_symbols_from_fred(self) -> list:
    #     symbols = list(constants.Symbols.FRED.keys())
    #     return symbols
