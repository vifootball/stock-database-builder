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
from columns import *
# from preprocessor import *
from metric_calculator import *


pd.options.mode.chained_assignment = None

class HistoryCollector():
    
    @staticmethod
    def get_raw_history_from_yf(symbol):
        history = yf.Ticker(symbol).history(period='max').reset_index(drop=False) # date가 index
        history.rename(columns={
            "Date": "date",
            "Open": "open",
            "HIgh": "high",
            "Low": "low",
            "close": "close",
            "Volume": "volume",
            "Dividends": "dividend",
            "Stock Splits": "stock_split"
        }, inplace=True)

        if (len(history) > 50):
            if  (days_from_last_traded := dt.datetime.today() - history['date'].max()) < pd.Timedelta('50 days'):
                history['date'] = history['date'].astype('str')
                history['symbol'] = symbol
        else:
            history = None
         
        return history
    
    def get_raw_history_from_fred(self, symbol):
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        try: 
            history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
            history = history.reset_index()
            history.rename(columns={
                f'{symbol}': 'close',
                'DATE': 'date'
            }, inplace=True)
            history['symbol'] = symbol
            history['date'] = history['date'].astype('str')

            header = pd.DataFrame(columns=COL_HISTORY_RAW)
            history = pd.concat([header, history])[COL_HISTORY_RAW]
        
        except:
            history = None

        return history

    def preprocess_raw_history(self, raw_history):
        history = calculate_metrics(raw_history)
        return history
    
    def collect_histories(self, symbols: list, save_dirpath):
        for symbol in symbols:
            fname = f"{symbol}.csv"
            fpath = os.makedirs(save_dirpath, fname)
            raw_history = 