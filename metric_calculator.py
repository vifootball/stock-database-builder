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
    try:
        price_change_rate = price_change / price
        price_change_rate = price_change_rate.round(6)
    except ZeroDivisionError:
        price_change_rate = np.nan
    return price_change_rate

def calc_price_change_sign(price_change):
    if price_change.dtype in [int, float]:
        price_change_sign =  np.sign(price_change)
    else:
        price_change_sign = np.nan
    return price_change_sign

def calc_price_all_time_high(price):
    price_all_time_high = price.cummax()
    return price_all_time_high

def calc_drawdown_current(price, price_all_time_high):
    try:
        drawdown_current_plus1 = (price / price_all_time_high)
        if drawdown_current_plus1.dtype in [int, float]:
            drawdown_current = ((price / price_all_time_high) - 1).round(6)
        else:
            drawdown_current = np.nan
    except ZeroDivisionError:
        drawdown_current = np.nan
    return drawdown_current

def calc_drawdown_max(drawdown_current):
    drawdown_max = drawdown_current.cummin().round(6)
    return drawdown_max

def calc_volume_of_dollar(price, volume_of_shares): # estimated by close price
    volume_of_dollar = (price * volume_of_shares)
    if volume_of_dollar.dtype in [int, float]:
        volume_of_dollar = volume_of_dollar.round(0)
    return volume_of_dollar

def calc_volume_of_share_3m_avg(volume_of_share):
    volume_of_share_3m_avg = volume_of_share.rolling(window='90d').mean().round(0).to_numpy()
    return volume_of_share_3m_avg

def calc_volume_of_dollar_3m_avg(volume_of_dollar):
    volume_of_dollar_3m_avg = volume_of_dollar.rolling(window='90d').mean().round(0).to_numpy()
    return volume_of_dollar_3m_avg

# dividend
def calc_dividend_paid_or_not(dividend):
    if dividend.dtype in [int, float]:
        dividend_paid_or_not = np.sign(dividend)
    else:
        dividend_paid_or_not = np.nan
    return dividend_paid_or_not

def calc_dividend_paid_count_ttm(dividend_paid_or_not):
    dividend_paid_count_ttm = dividend_paid_or_not.rolling(window='365d').sum().to_numpy()
    return dividend_paid_count_ttm

def calc_dividend_ttm(dividend):
    dividend_ttm = dividend.rolling(window='365d').sum().to_numpy()
    return dividend_ttm

def calc_dividend_rate(price, dividend):
    try:
        dividend_rate = (dividend / price)
        if dividend_rate.dtype in [int, float]:
            dividend_rate = dividend_rate.round(6)
    except ZeroDivisionError: # price가 0을 찍는 것은 어차피 지수밖에 없음
        dividend_rate = np.nan
    return dividend_rate
    
def calc_dividend_rate_ttm(price, dividend_ttm):
    try:
        dividend_ttm = dividend_ttm / price
    except ZeroDivisionError:
        dividend_ttm = np.nan
    return dividend_ttm

def calculate_metrics(history):
    history['date'] = pd.to_datetime(history['date'])
    history = history.set_index('date')

    history['price'] = copy_column(history['close'])

    # price
    history['price_change'] = calc_price_change(price=history['price'])
    history['price_change_rate'] = calc_price_change_rate(history['price'], history['price_change'])
    history['price_change_sign'] = calc_price_change_sign(history['price_change'])
    history['price_all_time_high'] = calc_price_all_time_high(history['price'])
    history['drawdown_current'] = calc_drawdown_current(history['price'], history['price_all_time_high'])
    history['drawdown_max'] = calc_drawdown_max(history['drawdown_current'])
    
    # volume
    history['volume_of_share'] = copy_column(history['volume'])
    history['volume_of_share_3m_avg'] = calc_volume_of_share_3m_avg(history['volume_of_share'])
    history['volume_of_dollar'] = calc_volume_of_dollar(history['price'], history['volume_of_share'])
    history['volume_of_dollar_3m_avg'] = calc_volume_of_dollar_3m_avg(history['volume_of_dollar'])

    # dividend
    history['dividend_paid_or_not'] = calc_dividend_paid_or_not(history['dividend'])
    history['dividend_paid_count_ttm'] = calc_dividend_paid_count_ttm(history['dividend_paid_or_not'])
    history['dividend_ttm'] = calc_dividend_ttm(history['dividend'])
    history['dividend_rate'] = calc_dividend_rate(history['price'], history['dividend'])
    history['dividend_rate_ttm'] = calc_dividend_rate_ttm(history['price'], history['dividend_ttm'])

    history = history.reset_index()
    return history
