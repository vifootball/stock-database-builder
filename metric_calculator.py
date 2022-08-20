import pandas as pd
import numpy as np

# general
def copy_column(col):
    col_copied = col.copy(deep=True)
    return col_copied

# price -> close 복사하기 # 하나씩 검증하기
def calc_price_change(price: pd.Series):
    price_change = price.diff() #.fillna(0)
    return price_change

def calc_price_change_rate(price, price_change):
    price_change_rate = price_change / price
    price_change_rate = price_change_rate.round(6)

def calc_price_change_sign(price_change):
    price_change_sign =  np.sign(price_change)
    return price_change_sign

def calc_price_all_time_high(price):
    price_all_time_high = price.cummax()
    return price_all_time_high

def calc_drawdown_current(price, price_all_time_high):
    drawdown_current = ((price / price_all_time_high) - 1).round(6)
    return drawdown_current

def calc_drawdown_max(drawdown_current):
    drawdown_max = drawdown_current.cummin().round(6)
    return drawdown_max

def calc_volume_of_dollar(price, volume_of_shares): # estimated by close price
    volume_of_dollar = int((price * volume_of_shares).round(0))
    return volume_of_dollar

def calc_volume_of_shares_3m_avg(volume_of_shares):
    volume_of_shares_3m_avg = volume_of_shares.rolling(window='90d').mean().to_numpy()
    return volume_of_shares_3m_avg

def calc_volume_of_dollar_3m_avg(volume_of_dollar):
    volume_of_dollar_3m_avg = volume_of_dollar.rolling(window='90d').mean().to_numpy()
    return volume_of_dollar_3m_avg

# dividend
def calc_dividend_paid_or_not(dividend):
    dividend_paid_or_not = np.sign(dividend)
    return dividend_paid_or_not

def calc_dividend_paid_count_ttm(dividend_paid_or_not):
    dividend_paid_count_ttm = dividend_paid_or_not.rolling(window='365d').sum().to_numpy()
    return dividend_paid_count_ttm

def calc_dividend_ttm(dividend):
    dividend_ttm = dividend.rolling(window='365d').sum().to_numpy()
    return dividend_ttm

def calc_dividend_rate(price, dividend):
    dividend_rate = (dividend / price).round(6)
    return dividend_rate
    
def calc_dividend_rate_ttm(price, dividend_ttm):
    dividend_ttm = dividend_ttm / price
    return dividend_ttm

