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

def get_etf_symbols() -> list:
    etf_meta_fd = fd.select_etfs(category=None)
    etf_meta_fd = pd.DataFrame(etf_meta_fd).T.reset_index().rename(columns={"index": "symbol"})
    etf_symbols = list(etf_meta_fd["symbol"])
    return etf_symbols


def get_etf_meta_fd(etf_symbol):
    # 모듈 내에 있는 source, 즉 etf의 이름을 뽑는 source에서 데이터를 뽑는 것이기 때문에 예외처리는 딱히 필요 없음

    # configure columns
    src_cols_info = {
        'symbol': {'new_name': 'symbol', 'save': True},
        'short_name': {'new_name': 'short_name', 'save': True},
        'long_name': {'new_name': 'long_name', 'save': True},
        'currency': {'new_name': 'currency', 'save': False},
        'family': {'new_name': 'fund_family', 'save': True},
        'exchange': {'new_name': 'exchange', 'save': False},
        'market': {'new_name': 'market', 'save': False},
        'total_assets': {'new_name': 'total_assets', 'save': True}
    }
    expected_cols = list(src_cols_info.keys())
    name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
    cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]

    etf_symbol = etf_symbol.lower()
    etf_meta_fd = fd.select_etfs(category=None)
    etf_meta_fd = pd.DataFrame(etf_meta_fd).T.reset_index().rename(columns={"index": "symbol"})
    etf_meta_fd = etf_meta_fd[etf_meta_fd['symbol'].str.lower()==etf_symbol].reset_index(drop=True)

    if len(etf_meta_fd) == 0:
        etf_meta_fd = pd.DataFrame({col: [np.nan] for col in expected_cols}) # empty row

    etf_meta_fd = etf_meta_fd.rename(columns=name_mapping)[cols_to_save]

    return etf_meta_fd


def get_etf_holdings(etf_symbol):
    # 1. 반환이 아무것도 안되면 expected column에 빈 행이 있는 데이터프레임 반환
    # 2. 반환되는데 missing column있으면 그 항목에만 null 채운 데이터프레임 반환
    
    # configure columns
    src_cols_info = {
        'maxAge': {'new_name': 'max_age', 'save': False},
        'stockPosition': {'new_name': 'stock_position', 'save': True},
        'bondPosition': {'new_name': 'bond_position', 'save': True},
        'holdings': {'new_name': 'holdings', 'save': True},
        'bondRatings': {'new_name': 'bond_ratings', 'save': True},
        'sectorWeightings': {'new_name': 'sector_weightings', 'save': True},
        'equityHoldings.priceToEarnings': {'new_name': 'price_to_earnings', 'save': False},
        'equityHoldings.priceToBook': {'new_name': 'price_to_book', 'save': False}, 
        'equityHoldings.priceToSales': {'new_name': 'price_to_sales', 'save': False},
        'equityHoldings.priceToCashflow': {'new_name': 'price_to_cashflow', 'save': False},
        'bondHoldings.maturity': {'new_name': 'maturity', 'save': True}, # 일부 채권에만 존재
        'bondHoldings.duration': {'new_name': 'duration', 'save': True} # 채권에만 존재
    }

    expected_cols = list(src_cols_info.keys())
    name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
    cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]

    # calling yahoo api
    etf_symbol = etf_symbol.lower()
    etf = yahooquery.Ticker(etf_symbol)
    etf_holdings = etf.fund_holding_info[etf_symbol]

    # If no holdings data is available, return an empty dataframe
    if isinstance(etf_holdings, str):                                          # 없으면 str로 메시지 반환
        etf_holdings = pd.DataFrame({col: [np.nan] for col in expected_cols})  # dict comprehension

    # If holdings data is available, check columns and return dataframe
    else: 
        etf_holdings = pd.json_normalize(etf_holdings)
        unexpected_cols = set(etf_holdings.columns) - set(expected_cols) 
        if unexpected_cols:
            raise ValueError(f"Unexpected columns in source data: {unexpected_cols}")
        # If missing values exist, set NaN
        header = pd.DataFrame(columns=expected_cols)
        etf_holdings = pd.concat([header, etf_holdings])

    etf_holdings = etf_holdings.rename(columns=name_mapping)[cols_to_save]
    return etf_holdings


def get_etf_profile(symbol):
    src_cols_info = {
        'Net Assets': {'new_name': 'net_assets', 'save': True}, 
        'NAV': {'new_name': 'nav', 'save': True},
        'PE Ratio (TTM)': {'new_name': 'per_ttm', 'save': False}, 
        'Yield': {'new_name': 'yield', 'save': False},
        'YTD Daily Total Return': {'new_name': 'ytd_daily_total_return', 'save': False},
        'Beta (5Y Monthly)': {'new_name': 'beta_5y-monthly', 'save': False},
        'Expense Ratio (net)': {'new_name': 'expense_ratio', 'save': True},
        'Inception Date': {'new_name': 'inception_date', 'save': True}
    }
    expected_cols = list(src_cols_info.keys())
    name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
    cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]

    # calling yahoo api
    etf_profile = yf.Ticker(symbol).get_institutional_holders()

    if not isinstance(etf_profile, pd.DataFrame): # 없는 종목일 경우 None 반환
        etf_profile = pd.DataFrame({col: [np.nan] for col in expected_cols}) # empty row

    else: 
        etf_profile = etf_profile.T
        etf_profile.columns = etf_profile.iloc[0]
        etf_profile = etf_profile[1:]
        
        if list(etf_profile.columns) != expected_cols: # 있는 종목이어도 etf가 아닐수도 있음
            etf_profile = pd.DataFrame({col: [np.nan] for col in expected_cols}) # empty row

    etf_profile = etf_profile.rename(columns=name_mapping)[cols_to_save]
    return etf_profile


def transform_etf_profile(etf_profile: pd.DataFrame):
    # expense_ratio str to float
    def _convert_expense_ratio(expense_ratio):
        if pd.isna(expense_ratio):
            return expense_ratio
        else:
            return float(expense_ratio.replace('%',''))/100
    etf_profile['expense_ratio'] = etf_profile['expense_ratio'].apply(_convert_expense_ratio)

    # net_assets str to float
    def _convert_net_assets(net_assets):
        if pd.isna(net_assets):
             return net_assets
        
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
        suffix = net_assets[-1]
        if suffix.isdigit():
            return int(net_assets.replace(',', ''))
        else:
            return int(float(net_assets[:-1]) * multipliers[suffix])
    etf_profile['net_assets'] = etf_profile['net_assets'].apply(_convert_net_assets)
    
    return etf_profile


def get_etf_aum(etf_symbol): # 2-3번에 나눠돌려야함 429에러 발생
    src_cols_info = {
        'aum': {'new_name': 'aum', 'save': True},
        'shares_out': {'new_name': 'shares_out', 'save': True}
    }
    expected_cols = list(src_cols_info.keys())
    name_mapping = {col: info['new_name'] for col, info in src_cols_info.items()}
    cols_to_save = [src_cols_info[col]['new_name'] for col in src_cols_info if src_cols_info[col]['save']]
    
    etf_symbol = etf_symbol.lower()
    url = Request(f"https://stockanalysis.com/etf/{etf_symbol}/", headers={'User-Agent': 'Mozilla/5.0'})


    try:
        time.sleep(1)
        html = urlopen(url)
        bs_obj = bs(html, "html.parser")
        trs = bs_obj.find_all('tr')
        for tr in (trs):
            try:
                if "Assets" in tr.find_all('td')[0].get_text():
                    aum = tr.find_all('td')[1].get_text().replace("$", "")
                    break
            except:
                continue
        for tr in (trs):
            try:
                if "Shares Out" in tr.find_all('td')[0].get_text():
                    shares_out = tr.find_all('td')[1].get_text()
                    break
            except:
                continue
        
        df = {'symbol': etf_symbol, 'aum': aum, 'shares_out': shares_out}
        df = {'aum': aum, 'shares_out': shares_out}

        df = pd.DataFrame.from_dict(df, orient='index').T.reset_index(drop=True)
        return df
    
    except:
        print(f"Error Get AUM: {etf_symbol}")
        etf_aum = pd.DataFrame({col: [np.nan] for col in expected_cols})
        return etf_aum


def transform_etf_aum(etf_aum: pd.DataFrame):
    def _convert_str_to_number(num_str):
        if pd.isna(num_str):
             return num_str
        
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000}
        suffix = num_str[-1]
        if suffix.isdigit():
            return int(num_str.replace(',', ''))
        else:
            return int(float(num_str[:-1]) * multipliers[suffix])

    etf_aum['aum'] = etf_aum['aum'].apply(_convert_str_to_number)
    etf_aum['shares_out'] = etf_aum['shares_out'].apply(_convert_str_to_number)

    return etf_aum



# print(get_etf_symbols())
print(get_etf_meta_fd("sche"))
# print(get_etf_holdings("tlt"))
# print(transform_etf_profile(get_etf_profile('xlv')))
# print(transform_etf_aum(get_etf_aum('soxl')))




