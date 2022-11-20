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

from utils import *
from columns import *
from constants import *
from metric_calculator import *


pd.options.mode.chained_assignment = None

class HistoryCollector():
    
    def get_raw_history_from_yf(self, symbol):
        history = yf.Ticker(symbol).history(period='max').reset_index(drop=False) # date가 index
        history.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
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
    
    def collect_histories_from_yf(self, symbols: list, save_dirpath):
        for symbol in tqdm(symbols, mininterval=0.5):    
            raw_history = self.get_raw_history_from_yf(symbol)
            if raw_history is not None:
                pp_history = self.preprocess_raw_history(raw_history)
                
                fname = f"history_{symbol}.csv"
                fpath = os.path.join(save_dirpath, fname)            
                os.makedirs(save_dirpath, exist_ok=True)
                pp_history.to_csv(fpath, index=False)
    
    def collect_histories_from_fred(self, symbols: list, save_dirpath):
        for symbol in tqdm(symbols, mininterval=0.5):    
            raw_history = self.get_raw_history_from_fred(symbol)
            if raw_history is not None:
                pp_history = self.preprocess_raw_history(raw_history)
                
                fname = f"{symbol}.csv"
                fpath = os.path.join(save_dirpath, fname)            
                os.makedirs(os.path.dirname(fpath), exist_ok=True)
                pp_history.to_csv(fpath, index=False)