import os
import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import yahooquery
import financedatabase as fd

def get_etf_symbols() -> list:
    etf_meta_fd = fd.select_etfs(category=None)
    etf_meta_fd = pd.DataFrame(etf_meta_fd).T.reset_index().rename(columns={"index": "symbol"})
    etf_symbols = list(etf_meta_fd["symbol"])
    return etf_symbols

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

    # If hodings data is available, check columns and return dataframe
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


# print(get_etf_symbols())
print(get_etf_holdings("edv"))