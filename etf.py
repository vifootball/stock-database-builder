import os
import re
import time
import ray
import requests
import random
import numpy as np
import pandas as pd
import yfinance as yf
import yahooquery
import financedatabase as fd
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from table import TableHandler
import table_config
# from constants import AssetCategories
from common import *
from utils import *

def get_symbols() -> list:
    etf = fd.ETFs()
    symbols = etf.select().index.to_list()
    symbols = sorted([symbol for symbol in symbols if symbol.isalpha()])
    return symbols


def get_etf_masters_fd() -> pd.DataFrame:
    master_cols = {
        'symbol':'symbol', 
        'name': 'name',
        'currency': 'currency',
        'summary': 'summary',
        'category_group': 'category',
        'category': 'subcategory',
        'family': 'fund_family',
        'exchange': 'exchange',
        'market': 'market'
    }
    selected_cols = [
        'symbol', 'name', 'summary', 'category', 'subcategory', 'fund_family'
    ]
    etf = fd.ETFs()
    masters_fd = etf.select().reset_index().rename(columns=master_cols)[selected_cols]
    masters_fd = masters_fd.loc[masters_fd['symbol'].str.isalpha()].reset_index(drop=True)
    masters_fd = masters_fd.sort_values(by='symbol')
    return masters_fd

def get_etf_master_yf(symbol: str):
    df = yf.Ticker(symbol.lower()).get_institutional_holders()
    if not isinstance(df, pd.DataFrame): # 없는 종목일 경우 None을 반환
        print(f'{symbol.ljust(8)}: Failed to get data') 
        return None
    else:
        df = df.T
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df['symbol'] = symbol
        master_cols = {
            'symbol':'symbol',
            'Net Assets': 'net_assets', 
            'NAV': 'nav',
            'PE Ratio (TTM)': 'pe_ratio_ttm', 
            'Yield': 'div_yield',
            'YTD Daily Total Return': 'ytd_daily_total_return', 
            'Beta (5Y Monthly)': 'Beta_5y_monthly', 
            'Expense Ratio (net)': 'expense_ratio',
            'Inception Date': 'inception_date' 
            }
        header = pd.DataFrame(columns=master_cols.keys())
        df = pd.concat([header, df])[master_cols.keys()]
        df['yf_date'] = pd.Timestamp.now().strftime("%Y%m%d")
        selected_cols = [
            'symbol',
            'net_assets',
            'expense_ratio',
            'inception_date',
            'yf_date'
        ]
        master_yf = df.rename(columns=master_cols)[selected_cols]
        master_yf['expense_ratio'] = master_yf['expense_ratio'].apply(percentage_to_float)
        master_yf['net_assets'] = master_yf['net_assets'].apply(str_to_int)
    return master_yf

def get_etf_master_sa_1(symbol: str): # 2-3번에 나눠돌려야함 429에러 발생
    # sa1 date 추가
    symbol = symbol.upper()
    url = Request(f"https://stockanalysis.com/etf/{symbol}/", headers={'User-Agent': 'Mozilla/5.0'})
    
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
        
        df = {'symbol': symbol, 'aum': aum, 'shares_out': shares_out}
        df = pd.DataFrame.from_dict(df, orient='index').T.reset_index(drop=True)
        df['aum'] = df['aum'].apply(str_to_int)
        df['shares_out'] = df['shares_out'].apply(str_to_int)
        df['sa_1_date'] = pd.Timestamp.now().strftime("%Y%m%d")
        # print(f'{symbol.ljust(8)}: Success')
        return df
    
    except:
        print(f'{symbol.ljust(8)}: Failed to get data') 
        return None

def get_etf_master_sa_2(symbol: str):
    # sa2 date 추가
    symbol = symbol.upper()
    try:
        time.sleep(1)
        url = Request(f"https://stockanalysis.com/etf/{symbol}/holdings/", headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(url)
        bs_obj = bs(html, "html.parser")
    except:
        print(f'{symbol.ljust(8)}: Failed to get bsObj') 
        return None

    try:
        divs = bs_obj.find_all('div', class_=['bp:text-xl'])
        df = {}
        df['symbol'] = symbol
        df['holdings_count'] = divs[0].get_text().replace(',','')
        df['top10_percentage'] = divs[2].get_text()
        df['asset_class'] = divs[3].get_text()
        df['sa_2_date'] = pd.Timestamp.now().strftime("%Y%m%d")
        if divs[5].get_text() == "n/a" or bool(re.match(r'^[\d,.]+$', divs[5].get_text())):
            df['sector'] = None
            df['region'] = divs[4].get_text()
        else:
            df['sector'] = divs[4].get_text()
            df['region'] = divs[5].get_text()
        df = pd.json_normalize(df)

        df['top10_percentage'] = df['top10_percentage'].apply(percentage_to_float)

        selected_cols = [
            'symbol', 'holdings_count', 'top10_percentage', 'asset_class', 'sector', 'region', 'sa_2_date'
        ]
        df = df[selected_cols]
        print(f'{symbol.ljust(8)}: Success')
        return df
    
    except:
        print(f'{symbol.ljust(8)}: Failed to parse bsObj') 
        return None

def get_etf_master_sa_3(symbol: str):
    symbol = symbol.upper()
    try:
        time.sleep(1)
        url = Request(f"https://stockanalysis.com/etf/{symbol}/", headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(url)
        bs_obj = bs(html, "html.parser")
    except:
        print(f'{symbol.ljust(8)}: Failed to get bsObj') 
        return None
    try:
        profile_dict = {}
        profile_dict['symbol'] = symbol
        profile_dict['category'] = None
        profile_dict['index_tracked'] = None
        profile_dict['stock_exchange'] = None
        profile_dict['description'] = None
        profile_dict['sa_3_date'] = pd.Timestamp.now().strftime("%Y%m%d")

        description = bs_obj.find('p', {'data-test': 'overview-profile-description'}).get_text()
        profile_dict['description'] = description

        profile_values = bs_obj.find('div', {'data-test': 'overview-profile-values'})
        for i in profile_values:
            profile_value = i.get_text()
            if "Category" in profile_value:
                profile_value = profile_value.replace('Category', '').strip()
                profile_dict['category'] = profile_value
            elif "Index Tracked" in profile_value:
                profile_value = profile_value.replace('Index Tracked', '').strip()
                profile_dict['index_tracked'] = profile_value
            elif "Stock Exchange" in profile_value:
                profile_value = profile_value.replace('Stock Exchange', '').strip()
                profile_dict['stock_exchange'] = profile_value
        profile_dict = pd.json_normalize([profile_dict])
        selected_cols = [
            'symbol', 'category', 'index_tracked', 'stock_exchange', 'description', 'sa_3_date'
        ]
        profile_dict = profile_dict[selected_cols]
        print(f'{symbol.ljust(8)}: Success')
        return profile_dict
    
    except:
        print(f'{symbol.ljust(8)}: Failed to parse bsObj') 
        return None


def get_etf_holdings(symbol: str):
    symbol = symbol.upper()
    try:
        time.sleep(1)
        holdings = yahooquery.Ticker(symbol).fund_holding_info[symbol]
        holdings = pd.json_normalize(holdings)
    except:    
        print(f'{symbol.ljust(8)}: Failed to get data') 
        return None
    
    # print(holdings)
    if holdings['holdings'][0]:
        holdings = holdings['holdings'][0]
        holdings = pd.DataFrame(holdings)
        holdings['ticker'] = symbol
        holdings['holdings_date'] = pd.Timestamp.now().strftime("%Y%m%d")
        cols = {
            'ticker': 'symbol',
            'holdings_date': 'holdings_date',
            'symbol': 'holding_symbol',
            'holdingName': 'holding_name',
            'holdingPercent': 'holding_percent'
        }
        selected_cols = [
            'symbol', 'holdings_date', 'holding_symbol', 'holding_name', 'holding_percent'
        ]
        holdings = holdings.rename(columns=cols)[cols.values()][selected_cols]
        print(f'{symbol.ljust(8)}: Success')
        return(holdings)
    
    else:
        print(f'{symbol.ljust(8)}: Failed to parse data') 
        return None


def merge_etf_masters():
    etf_masters_fd = pd.read_csv('downloads/masters_etf_fd.csv')[['symbol', 'name', 'fund_family', 'summary']]
    etf_masters_yf = pd.read_csv('downloads/masters_etf_yf.csv')
    etf_masters_sa_1 = pd.read_csv('downloads/masters_etf_sa_1.csv')
    etf_masters_sa_2 = pd.read_csv('downloads/masters_etf_sa_2.csv')
    etf_masters_sa_3 = pd.read_csv('downloads/masters_etf_sa_3.csv')

    etf_masters = etf_masters_fd.merge(etf_masters_yf, how='left', on='symbol')
    etf_masters = etf_masters.merge(etf_masters_sa_1, how='left', on='symbol')
    etf_masters = etf_masters.merge(etf_masters_sa_2, how='left', on='symbol')
    etf_masters = etf_masters.merge(etf_masters_sa_3, how='left', on='symbol')

    etf_masters['type'] = 'etf'
    cols = [
        'type', 'symbol', 'name', 'fund_family', 'summary',
        'net_assets', 'expense_ratio', 'inception_date', 'yf_date',
        'aum', 'shares_out', 'sa_1_date',
        'holdings_count', 'top10_percentage', 'asset_class', 'sector', 'region', 'sa_2_date',
        'category', 'index_tracked', 'stock_exchange', 'description', 'sa_3_date'
    ]
    etf_masters = etf_masters[cols]
    etf_masters.to_csv('./downloads/masters_etf.csv', index=False)


# if __name__ == "__main__":
    # symbols = get_symbols()
    # print(symbols)

    # get_etf_masters_fd().to_csv('downloads/masters_etf_fd.csv', index=False)


    # master_yf = get_etf_master_yf(symbol='gdx')
    # print(master_yf)

    # ## master_yf
    # @ray.remote
    # def collect_etf_master_yf(symbol):
    #     time.sleep(2)
    #     master_yf = get_etf_master_yf(symbol)
    #     if master_yf is not None:
    #         os.makedirs('downloads/masters_etf_yf', exist_ok=True)
    #         master_yf.to_csv(f'downloads/masters_etf_yf/{symbol}_master_yf.csv', index=False)
    # # symbols = get_symbols()[:10]
    # symbols = get_symbols()[2190:]
    # tasks = [collect_etf_master_yf.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)
    # concat_csv_files_in_dir('downloads/masters_etf_yf').to_csv('downloads/masters_etf_yf.csv', index=False)

    # print(get_etf_master_sa_2('spy'))


    # master_sa_1 = get_etf_master_sa_1(symbol='xlb')
    # print(master_sa_1)

    ## master_sa_1
    # @ray.remote
    # def collect_etf_master_sa_1(symbol):
    #     time.sleep(2)
    #     master = get_etf_master_sa_1(symbol)
    #     if master is not None:
    #         os.makedirs('downloads/masters_etf_sa_1/', exist_ok=True)
    #         master.to_csv(f'downloads/masters_etf_sa_1/{symbol}_masters_etf_sa_1.csv', index=False)
    # symbols = get_symbols()[2600:]
    # tasks = [collect_etf_master_sa_1.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)
    # concat_csv_files_in_dir('downloads/masters_etf_sa_1/').to_csv('downloads/masters_etf_sa_1.csv', index=False)

    # ## master_sa_2
    # @ray.remote
    # def collect_etf_master_sa_2(symbol):
    #     time.sleep(round(random.uniform(10.1, 15.0), 3))
    #     time.sleep(round(random.uniform(60.1, 100.0), 3))

    #     master = get_etf_master_sa_2(symbol)
    #     if master is not None:
    #         os.makedirs('downloads/masters_etf_sa_2/', exist_ok=True)
    #         master.to_csv(f'downloads/masters_etf_sa_2/{symbol}_masters_etf_sa_2.csv', index=False)
    # symbols = get_symbols()[2000:]
    # tasks = [collect_etf_master_sa_2.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)
    # concat_csv_files_in_dir('downloads/masters_etf_sa_2/').to_csv('downloads/masters_etf_sa_2.csv', index=False)


    # print(get_etf_master_sa_3('qqq'))
    # ## master_sa_3
    # @ray.remote
    # def collect_etf_master_sa_3(symbol):
    #     time.sleep(round(random.uniform(10.1, 15.0), 3))
    #     time.sleep(round(random.uniform(60.1, 110.0), 3))

    #     master = get_etf_master_sa_3(symbol)
    #     if master is not None:
    #         os.makedirs('downloads/masters_etf_sa_3/', exist_ok=True)
    #         master.to_csv(f'downloads/masters_etf_sa_3/{symbol}_masters_etf_sa_3.csv', index=False)
    # symbols = get_symbols()[200:]
    # tasks = [collect_etf_master_sa_3.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)
    # concat_csv_files_in_dir('downloads/masters_etf_sa_3/').to_csv('downloads/masters_etf_sa_3.csv', index=False)

    # holdings
    # @ray.remote
    # def collect_etf_holdings(symbol):
    #     time.sleep(round(random.uniform(6.0, 12.0), 3))
    #     holdings = get_etf_holdings(symbol)
    #     if holdings is not None:
    #         os.makedirs('downloads/holdings/', exist_ok=True)
    #         holdings.to_csv(f'downloads/holdings/{symbol}_holdings.csv', index=False)
    # symbols = get_symbols()[1500:]
    # tasks = [collect_etf_holdings.remote(symbol) for symbol in symbols]
    # ray.init(ignore_reinit_error=True)
    # ray.get(tasks)
    # concat_csv_files_in_dir('downloads/holdings/').to_csv('downloads/holdings.csv', index=False)
    

