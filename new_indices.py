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
from new_common import *
import new_constants
import investpy

class Indices:
    def __init__(self):
        self.metadata_table_handler = TableHandler(table_config=new_table_config.METADATA)
        self.metadata_common_table_handler = TableHandler(table_config=new_table_config.METADATA_COMMON)

    def get_metadata_from_yahoo_main(self) -> pd.DataFrame:
        # get raw data
        metadata = []
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
                'Name': 'name'
            })
            df['long_name'] = df['name'].copy()
            df['category'] = 'index'
            metadata.append(df)
        metadata = pd.concat(metadata).reset_index(drop=True)

        # table handling
        table_handler = self.metadata_table_handler
        header = pd.DataFrame(columns = table_handler.get_columns_to_select())
        metadata = pd.concat([header, metadata], axis=0)
        table_handler.check_columns(metadata)
        metadata = table_handler.select_columns(metadata)
        return metadata

    def get_symbols_from_yahoo_main(self) -> list: # 웹스크래핑으로 가져오는 정보라 변할 수 있기 때문에 한번 불러와서 상수로 저장
        symbols = new_constants.Symbols.YAHOO_MAIN
        return symbols

    def get_metadata_from_fd(self) -> pd.DataFrame:
        # get raw data
        metadata = fd.select_indices(market='kr_market') # 미국은 5만개라서 한국만 수집
        metadata = pd.DataFrame(metadata).T.reset_index().rename(columns={'index': 'symbol'})
        metadata['name'] = metadata['short_name'].copy()
        metadata['category'] = 'index'

        # table handling
        metadata = metadata[self.metadata_common_table_handler.get_columns_to_select()]
        header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
        metadata = pd.concat([header, metadata], axis=0)
        self.metadata_table_handler.check_columns(metadata)
        metadata = self.metadata_table_handler.select_columns(metadata)
        return metadata

    def get_symbols_from_fd(self) -> list:
        metadata = self.get_metadata_from_fd()
        symbols = metadata['symbol'].to_list()
        return symbols

    def get_metadata_from_investpy(self) -> pd.DataFrame:
        # get raw data
        countries = ['united states', 'south korea']
        metadata = investpy.indices.get_indices()
        metadata = metadata[metadata['country'].isin(countries)].reset_index(drop=True)
        metadata['symbol'] = '^' + metadata['symbol']
        metadata['category'] = 'index'
        metadata = metadata.rename(columns={
            'full_name': 'name',
            'name': 'short_name'
        })

        # table handling
        metadata = metadata[self.metadata_common_table_handler.get_columns_to_select()]
        header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
        metadata = pd.concat([header, metadata], axis=0)
        self.metadata_table_handler.check_columns(metadata)
        metadata = self.metadata_table_handler.select_columns(metadata)
        return metadata

    def get_symbols_from_investpy(self) -> list:
        metadata = self.get_metadata_from_investpy()
        symbols = metadata['symbol'].to_list()
        return symbols

    def get_metadata_from_fred(self) -> pd.DataFrame:
        metadata = new_constants.Symbols.FRED
        metadata = pd.DataFrame.from_dict(metadata, orient='index', columns=['name'])
        metadata = metadata.reset_index().rename(columns={'index': 'symbol'})
        metadata['short_name'] = metadata['name'].copy()
        metadata['category'] = 'index'

        # metadata = metadata[self.metadata_common_table_handler.get_columns_to_select()]
        header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
        metadata = pd.concat([header, metadata], axis=0)
        self.metadata_table_handler.check_columns(metadata)
        metadata = self.metadata_table_handler.select_columns(metadata)
        # table handling
        return metadata

    def get_symbols_from_fred(self) -> list:
        symbols = list(new_constants.Symbols.FRED.keys())
        return symbols
