from typing import Union
import pandas as pd
import pandas_datareader.data as web
import yfinance as yf
import datetime as dt
from table import TableHandler
import table_config
from metric_calculator import *

def get_history_from_yf(symbol: str) -> Union[pd.DataFrame, None]:
    # get raw data    
    history = yf.Ticker(symbol).history(period='max').reset_index(drop=False) # date가 index
    
    if len(history) > 50:
    # 데이터가 어느정도 있으면서 최근까지 업데이트 되는 종목만 수집하고자 함 
    # 정상데이터가 아니라면, 데이터가 아예 없거나 과거 데이터만 있음
        today = dt.datetime.today()
        last_traded_day = history['Date'].max().replace(tzinfo=None)
        days_from_last_traded = (today - last_traded_day)
        if days_from_last_traded < pd.Timedelta('50 days'):
            # table handling
            history['symbol'] = symbol.upper()
            history['Date'] = history['Date'].dt.strftime('%Y-%m-%d')
            history = history.rename(columns={
                'symbol' : 'symbol',
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Dividends': 'dividend',
                'Stock Splits': 'stock_split',
                'Capital Gains': 'capital_gain'
            })
            history = history[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split']]
    
        else:
            history = None
    else:
        history = None
    return history

def get_history_from_fred(symbol: str) -> Union[pd.DataFrame, None]:
    try:
        # get raw data
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
        history = history.reset_index()
        history.rename(columns={
            f'{symbol}': 'close',
            'DATE': 'date'
        }, inplace=True)
        history['symbol'] = symbol.upper()
        history['date'] = history['date'].dt.strftime('%Y-%m-%d')            

        header = pd.DataFrame(columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split'])
        history = pd.concat([header, history])
    except:
        history = None
    return history


def transform_history(history: pd.DataFrame) -> Union[pd.DataFrame, None]:
    # 1. 거래일 데이터만으로 지표 게산
    # 2. 빈 날짜 채워주기
    # 3. 컬럼 별 적절한 방법으로 결측치 보간하기
    # 4. 모든 날짜범위에서 지표 계산
    
    if history is None:
        return None
    else:
        try:
            # 1
            history = calculate_metrics_on_trading_dates(history)
            # 2
            history = fill_missing_date_index(history)
            # 3
            history = fill_na_values(history)
            # 4
            history = calculate_metrics_on_all_dates(history)
        except:
            print(f'error in transform_history: check the Symbol {history["symbol"].iloc[0]}')
            return None
    return history


# class History:
#     def __init__(self):
#         self.src_history_table_handler = TableHandler(table_config=table_config.SRC_HISTORY)
#         self.trg_history_table_handler = TableHandler(table_config=table_config.TRG_HISTORY)

#     def get_history_from_yf(self, symbol: str) -> Union[pd.DataFrame, None]:
#         # get raw data    
#         history = yf.Ticker(symbol).history(period='max').reset_index(drop=False) # date가 index
#         # 데이터가 어느정도 있으면서 최근까지 업데이트 되는 종목 # 아예 없거나 과거 데이터만 있거나
#         if len(history) > 50:
#             today = dt.datetime.today()
#             last_traded_day = history['Date'].max().replace(tzinfo=None)
#             days_from_last_traded = (today - last_traded_day)
#             if days_from_last_traded < pd.Timedelta('50 days'):
#                 # table handling
#                 history['symbol'] = symbol.upper()
#                 history['Date'] = history['Date'].dt.strftime('%Y-%m-%d')
#                 table_handler = self.src_history_table_handler
#                 history = table_handler.rename_columns(history)
#                 history = table_handler.select_columns(history)                 
#             else:
#                 history = None
#         else:
#             history = None
#         return history

#     def get_history_from_fred(self, symbol: str) -> Union[pd.DataFrame, None]:
#         try:
#             # get raw data
#             start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
#             history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
#             history = history.reset_index()
#             history.rename(columns={
#                 f'{symbol}': 'close',
#                 'DATE': 'date'
#             }, inplace=True)
#             history['symbol_fk'] = symbol.upper()
#             history['date'] = history['date'].dt.strftime('%Y-%m-%d')            

#             # table handling
#             table_handler = self.src_history_table_handler
#             header = pd.DataFrame(columns=table_handler.get_columns_to_select())
#             history = pd.concat([header, history], axis=0)
#             table_handler.check_columns(history)
#         except:
#             history = None
#         return history

#     def transform_history(self, history: pd.DataFrame) -> pd.DataFrame:
#         # 1. 거래일 데이터만으로 지표 게산
#         # 2. 빈 날짜 채워주기
#         # 3. 컬럼 별 적절한 방법으로 결측치 보간하기
#         # 4. 모든 날짜범위에서 지표 계산
        
#         if history is None:
#             return None
#         else:
#             try:
#                 # 1
#                 history = calculate_metrics_on_trading_dates(history)
#                 # 2
#                 history = fill_missing_date_index(history)
#                 # 3
#                 history = fill_na_values(history)
#                 # 4
#                 history = calculate_metrics_on_all_dates(history)
#             except:
#                 print(f'error in transform_history: check the Symbol {history["symbol_fk"].iloc[0]}')
#                 return None

#         # table handling
#         self.trg_history_table_handler.check_columns(history)
#         history = self.trg_history_table_handler.select_columns(history)
#         return history