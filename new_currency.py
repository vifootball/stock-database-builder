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
import investpy

class Currency():
    def __init__(self):
        self.metadata_table_handler = TableHandler(table_config=new_table_config.METADATA)
        self.currency_table_handler = TableHandler(table_config=new_table_config.CURRENCY)

    def get_symbols(self) -> list:
        metadata = self.get_metadata()
        symbols = metadata['symbol'].to_list()
        return symbols

    def get_metadata(self) -> pd.DataFrame:
        # get raw data
        currency = investpy.currency_crosses.get_currency_crosses()
        base_cur = ['KRW', 'USD']
        currency = currency[currency['base'].isin(base_cur)].reset_index(drop=True)
        def _encode_symbol(name):
            base_cur, second_cur = name.split('/')
            symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
            return symbol
        currency['symbol'] = currency['name'].apply(_encode_symbol)
        currency['category'] = 'currency'
        
        #table handling
        currency = self.currency_table_handler.rename_columns(currency)
        currency = self.currency_table_handler.select_columns(currency)
        header = pd.DataFrame(columns = self.metadata_table_handler.get_columns_to_select())
        metadata = pd.concat([header, currency], axis=0)
        
        return metadata



        