import os
import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import yahooquery
import financedatabase as fd
import investpy
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from table import TableHandler
import table_config
from common import *

def get_currency_masters() -> pd.DataFrame:
    currency = investpy.currency_crosses.get_currency_crosses()
    base_cur = ['KRW', 'USD']
    currency = currency[currency['base'].isin(base_cur)].reset_index(drop=True)
    def _encode_symbol(name):
        base_cur, second_cur = name.split('/')
        symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
        return symbol
    currency['symbol'] = currency['name'].apply(_encode_symbol)
    currency = currency.rename(columns={"full_name": "description"})
    currency['type'] = 'currency'
    currency = currency[['type', 'symbol', 'description']]

    master_header = [
    'type', 'symbol', 'name', 'fund_family', 'summary',
    'net_assets', 'expense_ratio', 'inception_date', 'yf_date',
    'aum', 'shares_out', 'sa_1_date',
    'holdings_count', 'top10_percentage', 'asset_class', 'sector', 'region', 'sa_2_date',
    'category', 'index_tracked', 'stock_exchange', 'description', 'sa_3_date'
    ]
    header = pd.DataFrame(columns=master_header)

    currency = pd.concat([header, currency])
    return currency

if __name__ == '__main__':
    print(get_currency_masters())


# class Currency():
#     def __init__(self):
#         self.metadata_table_handler = TableHandler(table_config=table_config.METADATA)
#         self.currency_table_handler = TableHandler(table_config=table_config.CURRENCY)

#     def get_symbols(self) -> list:
#         metadata = self.get_metadata()
#         symbols = metadata['symbol_pk'].to_list()
#         return symbols

#     def get_metadata(self) -> pd.DataFrame:
#         # get raw data
#         currency = investpy.currency_crosses.get_currency_crosses()
#         base_cur = ['KRW', 'USD']
#         currency = currency[currency['base'].isin(base_cur)].reset_index(drop=True)
#         def _encode_symbol(name):
#             base_cur, second_cur = name.split('/')
#             symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
#             return symbol
#         currency['symbol'] = currency['name'].apply(_encode_symbol)
#         currency['category'] = 'currency'
        
#         #table handling
#         currency = self.currency_table_handler.rename_columns(currency)
#         currency = self.currency_table_handler.select_columns(currency)
#         header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
#         metadata = pd.concat([header, currency], axis=0)
#         metadata = self.metadata_table_handler.rename_columns(metadata)
#         self.metadata_table_handler.check_columns(metadata)
#         metadata = self.metadata_table_handler.select_columns(metadata)
        
#         return metadata



