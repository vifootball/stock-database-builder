import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
from tqdm.notebook import tqdm
from bs4 import BeautifulSoup as bs
import investpy
import yfinance as yf
import pandas_datareader.data as web
from constants import *

# 티커가 바뀔 수 있으니, 마스터를 add 방식으로 수집하고 drop duplicates?
# price, date 결측치 보간하기


class EtlProcessor:
    def __init__(self):
        self.dir_download = DIRNAME_DOWNLOAD
        self.subdir_history_etf = SUBDIRNAME_HISTORY_ETF
        self.subdir_profile_etf = SUBDIRNAME_PROFILE_ETF
        self.subdir_info_etf = SUBDIRNAME_INFO_ETF
        self.subdir_history_indices = SUBDIRNAME_HISTORY_INDICES
        self.subdir_history_currencies = SUBDIRNAME_HISTORY_CURRENCIES
        self.subdir_summary = SUBDIRNAME_SUMMARY
        self.fname_meta_etf = FNAME_META_ETF
        self.fname_info_etf = FNAME_INFO_ETF
        self.fname_master_etf = FNAME_MASTER_ETF
        self.fname_master_indices_yahoo = FNAME_MASTER_INDICES_YAHOO
        self.fname_master_indices_investpy = FNAME_MASTER_INDICES_INVESTPY
        self.fname_master_currencies = FNAME_MASTER_CURRENCIES
        self.fname_master_indices_fred = FNAME_MASTER_INDICES_FRED
        self.fname_benchmark = FNAME_BENCHMARK
        self.fname_summary_etf = FNAME_SUMMARY_ETF
        self.fname_summary_indices = FNAME_SUMMARY_INDICES
        self.fname_summary_currencies = FNAME_SUMMARY_CURRENCIES
        # self.cols_etf_profile = COLS_PROFILE_ETF
        self.cols_etf_info_to_master = COLS_ETF_INFO_TO_MASTER
        self.cols_etf_profile_to_master = COLS_ETF_PROFILE_TO_MASTER
        self.cols_etf_master = COLS_MASTER_ETF
        self.dict_cols_etf_info = DICT_COLS_ETF_INFO
        self.dict_cols_etf_profile = DICT_COLS_ETF_PROFILE
        self.dict_cols_recession = DICT_COLS_RECESSION
        self.list_dict_symbols_fred = LIST_DICT_SYMBOLS_FRED

        os.makedirs(self.dir_download, exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_info_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_profile_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_indices), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_currencies), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_summary), exist_ok=True)


    def get_meta_etf(self):
        etf_meta = investpy.etfs.get_etfs(country='united states')
        etf_meta['category'] = 'etf'
        etf_meta.to_csv(os.path.join(self.dir_download, self.fname_meta_etf), index=False)
        return etf_meta

    def get_info_etf(self): # takes about an hour
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))#[:20]
        fnames_info = sorted(os.listdir(os.path.join(self.dir_download, self.subdir_info_etf)))
        
        etf_symbols = [x for x in etf_meta['symbol']]
        etf_symbols_already_have = [x.lstrip('info_').rstrip('.csv') for x in fnames_info if x.endswith('.csv')]        
        etf_symbols_to_get = [x for x in etf_symbols if x not in etf_symbols_already_have] # refresh mode, 이어서 모드
        
        etf_names = [x for x in etf_meta['name']]
        for i, etf_name in enumerate(tqdm(etf_names, mininterval=0.5)):
            symbol = etf_meta.loc[etf_meta['name']==etf_name]['symbol'].values[0]
            if symbol in etf_symbols_to_get:
                time.sleep(0.5)
                #if etf_name in etf_names_to_get:
                try:
                    etf_info = investpy.etfs.get_etf_information(etf_name, country='united states')
                except:
                    print(f'Loop No.{i} | Error Ocurred While Getting Profile of: {symbol}')
                    continue
                symbol = etf_meta.loc[etf_meta['name']==etf_name]['symbol'].values[0]
                etf_info['symbol'] = symbol
                
                etf_info.rename(columns=self.dict_cols_etf_info, inplace=True)
                etf_info['dividend_yield_rate'] = etf_info['dividend_yield_rate'].str.replace('%','').astype('float32')/100
                etf_info['1_year_change_rate'] = etf_info['1_year_change_rate'].str.replace('[ %]','', regex=True).astype('float32') /100 # 공백과 %기호 제거
                
                etf_info.to_csv(os.path.join(
                    self.dir_download, self.subdir_info_etf, f'info_{symbol}.csv'
                ), index=False)

    def get_profile_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))#[:15]
        fnames_profile = sorted(os.listdir(os.path.join(self.dir_download, self.subdir_profile_etf)))
        
        symbols = sorted([x for x in etf_meta['symbol']])
        symbols_already_have = sorted([x.lstrip('profile_').rstrip('.csv') for x in fnames_profile if x.endswith('.csv')])        
        symbols_to_get = sorted([x for x in symbols if x not in symbols_already_have]) # refresh mode, 이어서 모드

        for i, symbol in enumerate(tqdm(symbols_to_get, mininterval=0.5)): # test 후  try except
            time.sleep(0.5)
            #print(symbol)
            etf = yf.Ticker(symbol)
            try: # Noneype 일때
                profile = etf.get_institutional_holders().T
            except:
                print(f'Loop No.{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue
            profile.columns = profile.iloc[0]
            profile = profile[1:]
            profile['symbol'] = symbol
            profile['fund_family'] = etf.info.get('fundFamily')

            try: # ETF가 아닐때
                profile.rename(columns=self.dict_cols_etf_profile, inplace=True)
                profile = profile[self.dict_cols_etf_profile.values()]
            except:
                print(f'Loop No.{i} | Error Ocurred While Getting Profile of: {symbol}')
                continue
            
            profile['expense_ratio'] = profile['expense_ratio'].str.replace('%','').astype('float')/100
            profile.rename(columns={'net_assets': 'net_assets_original'}, inplace=True)
            profile['net_assets_original'] = profile["net_assets_original"].fillna("0")
            profile['multiplier_mil'] = (profile["net_assets_original"].str.endswith('M').astype('int') * (1000_000-1)) + 1
            profile['multiplier_bil'] = (profile["net_assets_original"].str.endswith('B').astype('int') * (1000_000_000-1)) + 1
            profile['net_assets'] = profile['net_assets_original'].str.extract('([0-9.]*)').astype('float')
            profile['net_assets'] *= profile['multiplier_mil'] * profile['multiplier_bil']

            profile.to_csv(os.path.join(
                self.dir_download, self.subdir_profile_etf, f'profile_{symbol}.csv'
            ), index=False)

    def construct_master_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))
        etf_info = pd.read_csv(os.path.join(self.dir_download, self.fname_info_etf))[self.cols_etf_info_to_master]
        etf_profile = pd.read_csv(os.path.join(self.dir_download, self.fname_profile_etf))[self.cols_etf_profile_to_master]
                
        etf_master = etf_meta.merge(etf_info, how='left', left_on='name', right_on='etf_name')
        etf_master = etf_master.merge(etf_profile, how='left', left_on='symbol', right_on='symbol')
        etf_master = etf_master[self.cols_etf_master]
        etf_master.to_csv(os.path.join(self.dir_download, self.fname_master_etf), index=False)
        return etf_master

    def get_master_indices_yahoo(self):
        dfs = []
        urls = [
            'https://finance.yahoo.com/world-indices',
            'https://finance.yahoo.com/commodities'
        ]

        for url in urls:
            response = requests.get(url)
            html = bs(response.text, "lxml")
            html_table = html.select("table")
            table = pd.read_html(str(html_table))
            df_indices = table[0][['Symbol','Name']]
            df_indices['full_name'] = df_indices['Name']
            df_indices['country'] = None
            df_indices['currency'] = None
            df_indices['category'] = 'index'
            df_indices.columns = df_indices.columns.str.lower().to_list()
            dfs.append(df_indices)

        df_yahoo = pd.concat(dfs).reset_index(drop=True)[COLS_MASTER_OTHERS]
        df_yahoo.to_csv(os.path.join(self.dir_download, self.fname_master_indices_yahoo), index=False)

        return df_yahoo

    def get_master_indices_investpy(self):
        countries = ['united states', 'south korea']
        df_indices = investpy.indices.get_indices()
        df_indices = df_indices[df_indices['country'].isin(countries)].reset_index(drop=True)
        df_indices['symbol'] = '^' + df_indices['symbol']
        df_indices['category'] = 'index'
        df_indices = df_indices[COLS_MASTER_OTHERS]
        
        df_indices.to_csv(os.path.join(self.dir_download, self.fname_master_indices_investpy), index=False)
        return df_indices

    def get_master_currencies(self):
        currencies = investpy.currency_crosses.get_currency_crosses()
        base_cur = ['KRW', 'USD']
        currencies = currencies[currencies['base'].isin(base_cur)].reset_index(drop=True)
        currencies['currency'] = currencies['base']
        currencies['category'] = 'currency'
        currency_to_country = {'USD': 'united states', 'KRW': 'south Korea'}
        currencies['country'] = currencies['currency'].map(currency_to_country)
        def _encode_symbol(name):
            base_cur, second_cur = name.split('/')
            if base_cur == 'USD':
                symbol = f'{second_cur}=X'
            else:
                symbol = f'{base_cur}{second_cur}=X'
            return symbol
        currencies['symbol'] = currencies['name'].apply(_encode_symbol)
        currencies = currencies[COLS_MASTER_OTHERS]
        
        currencies.to_csv(os.path.join(self.dir_download, self.fname_master_currencies), index=False)
        return currencies

    def get_master_indices_fred(self):
        df = pd.DataFrame(self.list_dict_symbols_fred)[COLS_MASTER_OTHERS]
        df.to_csv(os.path.join(self.dir_download,self.fname_master_indices_fred), index=False)
        return df

    def integrate_master():
        master_etf = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_etf))
        master_indices_yahoo = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_indices_yahoo))
        master_indices_investpy = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_indices_investpy))
        master_currencies = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_currencies))
        master_indices_fred = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_indices_fred))
        
        integrated_df = pd.concat([
            master_etf,
            master_indices_yahoo,
            master_indices_investpy,
            master_currencies,
            master_indices_fred
        ]).reset_index(drop=True)
        return integrated_df

    def get_benchmark(self):
        # recession
        rec_start, rec_end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        recession = web.DataReader('USREC', 'fred', rec_start, rec_end).asfreq(freq = "1d", method = 'ffill')
        # others
        snp_500 = yf.Ticker('^GSPC').history(period='max')['Close']
        kospi = yf.Ticker('^ks11').history(period='max')['Close']
        usd_krw = yf.Ticker('krw=x').history(period='max')['Close']
        usd_idx = yf.Ticker('DX-Y.NYB').history(period='max')['Close']
        # 모두 인덱스가 날짜라서 공통인덱스를 이용하여 컬럼방향 conccat
        benchmark = pd.concat([recession, snp_500, kospi, usd_krw, usd_idx], axis = 'columns').reset_index(drop=False)
        benchmark.columns = ['date', 'recession', 'snp_500', 'kospi', 'usd_krw', 'usd_idx']
        benchmark.to_csv(os.path.join(self.dir_download, self.fname_benchmark), index=False)
        return benchmark

    def _join_benchmark(self, history):
        benchmark = pd.read_csv(os.path.join(self.dir_download, self.fname_benchmark))
        history = history.merge(benchmark, how='left', on='date', suffixes=[None, '_y'])
        return history

    def _preprocess_history(self, df):
        df['date'] = pd.to_datetime(df['date'])

        # dividends
        df['dividends_paid'] = np.sign(df['dividends'])
        df['dividends_count_12m'] = df.set_index('date')['dividends_paid'].rolling(window='365d').sum().to_numpy() # tonumpy 안하며 nan 반환..
        df['dividends_trailing_12m'] = df.set_index('date')['dividends'].rolling(window='365d').sum().to_numpy()
        df['dividends_trailing_6m'] = (df.set_index('date')['dividends'].rolling(window='183d').sum().to_numpy())*2
         # prices
        df['change'] = df['close'].diff().fillna(0)
        df['change_sign'] = np.sign(df['change'])
        df['all_time_high'] = df['close'].cummax()
        
        # derived variables
        try:
            df['drawdown'] = (df['close']/df['all_time_high']) - 1    
            df['dividends_rate_trailing_12m'] = df['dividends_trailing_12m']/df['close']
            df['dividends_rate_trailing_6m'] = df['dividends_trailing_6m']/df['close']
            df['change_rate'] = df['change']/df['close']
        except ZeroDivisionError:
            df['drawdown'] = None
            df['dividends_rate_trailing_12m'] = None
            df['dividends_rate_trailing_6m'] = None
            df['change_rate'] = None

        try:
            df['momentum_rolling_1y'] = df['close'] / df['close_rolling_1y']
            df['momentum_rolling_6m'] = df['close'] / df['close_rolling_6m']
        except:
            df['momentum_rolling_1y'] = None
            df['momentum_rolling_6m'] = None
        # try except null -> 나중에 list comprehension?
        try:
            df['close_rolling_1y'] = df.set_index('date')['close'].rolling(window='365d').mean().to_numpy()
            df['momentum_rolling_1y'] = df['close'] / df['close_rolling_1y']
        except:
            df['close_rolling_1y'] = None
            df['momentum_rolling_1y'] = None
        try:
            df['close_rolling_6m'] = df.set_index('date')['close'].rolling(window='180d').mean().to_numpy()
        except:
            df['close_rolling_6m'] = None
        try:
            df['close_rolling_3m'] = df.set_index('date')['close'].rolling(window='90d').mean().to_numpy()
        except:
            df['close_rolling_3m'] = None
        try:
            df['close_rolling_1m'] = df.set_index('date')['close'].rolling(window='30d').mean().to_numpy()
        except:
            df['close_rolling_1m'] = None

        # momentum 1년 이평선 가격과 비교
        # momentum 1년전 가격과 비교 <- 정의상은 이게 맞지만 노이즈가 많이 껴서 이평선과 비교하는게 맞는 것 같음
        # or 30일 정도 기준 정해서 이동평균 구하기?


        #df['momentum_score_rolling_1y'] = np.sign(df['momentum_rolling_1y'])
        #df['momentum_score_rolling_6m'] = np.sign(df['momentum_rolling_6m'])


        # df['bull_bear'] = df['change_rate']
        # df['bull_bear'] = df['drawdown'].apply(lambda x: 'bear' if x>=-0.03 else 'bull') # 일주일 이동평균 이용해도 괜찮을듯 # B_B_w, B_B_m
        # df['all_time_drawdown'] = df['drawdown'].cummin()
        # df['drawdown_max'] = df['drawdown'].max()
        
        # momentum

        # df['drawdown_00']
        # df['drawdown_08']
        # df['drawdown_20']
        # df['drawdown_22']

        # df['change_1year']
        # df['change_month']
        return df

    def get_history_from_yf(self, master_df, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            dir_history = os.path.join(self.dir_download, self.subdir_history_indices)
        elif category == "etf":
            dir_history = os.path.join(self.dir_download, self.subdir_history_etf)
        elif category == "currency":
            dir_history = os.path.join(self.dir_download, self.subdir_history_currencies)

        for row in tqdm(master_df.itertuples(), total=len(master_df), mininterval=0.5):
            time.sleep(0.1)
            # i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            history = yf.Ticker(symbol).history(period='max').asfreq(freq = "1d").reset_index()
            history.columns = history.columns.str.lower()
            history.rename(columns={'stock splits':'stock_splits'}, inplace=True)
            history['date'] = history['date'].astype('str')
            history['country'] = getattr(row, 'country')
            history['symbol'] = getattr(row, 'symbol')
            history['full_name'] = getattr(row, 'full_name')
            
            history = self._join_benchmark(history)
            history = self._preprocess_history(history)

            if len(history) >= 2:
                history.to_csv(os.path.join(dir_history, f'history_{symbol}.csv'), index=False)
            else:
                print(f'Empty DataFrame at Loop {i}: {symbol}')

    def get_history_from_fred(self, master_df):
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        for row in tqdm(master_df.itertuples(), total=len(master_df)):
            # i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            history = web.DataReader(symbol, 'fred', start, end).asfreq(freq='1d', method='ffill').reset_index(drop=False)
            history['country'] = getattr(row, 'country')
            history['symbol'] = getattr(row, 'symbol')
            history['full_name'] = getattr(row, 'full_name')
            history.rename(columns={f'{symbol}':'close'}, inplace=True)
            history.rename(columns={'DATE':'date'}, inplace=True)
            history['date'] = history['date'].astype('str')

            header = pd.DataFrame(columns=COLS_HISTORY_STAGE_1)
            history = pd.concat([header, history])
            #history.loc[:, ['dividends', 'volume', 'stock_splits']] = 0

            history = self._join_benchmark(history)
            # history = self._preprocess_history(history) #-> 다른 히스토리컬럼이랑 구조 맞추기: FRED 지수는 굳이?

            history.to_csv(os.path.join(self.dir_download, self.subdir_history_indices, f'history_{symbol}.csv'),  index=False)
            # print(f'Error Occured at Loop {i}: {symbol}')
    
    def _summarize_history(self, df):
        df['date'] = pd.to_datetime(df['date'])

        summary = {}
        summary['symbol'] = df['symbol'][0]
        summary['all_time_High'] = df['close'].max()
        summary['maximal_drawdown'] = df['drawdown'].min()

        recent = df[df['date']==df['date'].max()]
        summary['recent_close'] = recent['close'].values[0]
        summary['recent_div_trailing_12m'] = recent['dividends_trailing_12m'].values[0]
        summary['recent_div_rate_trailing_12m'] = recent['dividends_rate_trailing_12m'].values[0]
        summary['recent_volume'] = recent['volume'].values[0]
        summary['drawdown'] = recent['drawdown'].values[0]

        recent_30d = df[df['date'] > df['date'].max() - pd.to_timedelta("30day")]
        summary['close_1m_high'] = recent_30d['close'].max()
        summary['close_1m_low'] = recent_30d['close'].min()
        summary['close_1m_avg'] = recent_30d['close'].mean()
        summary['volume_1m_avg'] = recent_30d['volume'].mean()
        
        summary = pd.DataFrame([summary])
        return summary

    def get_summary_from_history(self, category): # 중간에 끼워넣기 # recession과 겨랗ㅂ
        assert category in ['etf', 'index', 'currency'], 'category must be one of ("etf", "index", "currency")'
            
        dir_summary = os.path.join(self.dir_download, self.subdir_summary)
        if category == "etf":
            dir_history = os.path.join(self.dir_download, self.subdir_history_etf)
            fname_summary = self.fname_summary_etf
        elif category == "index":
            dir_history = os.path.join(self.dir_download, self.subdir_history_indices)
            fname_summary = self.fname_summary_indices
        elif category == "currency":
            dir_history = os.path.join(self.dir_download, self.subdir_history_currencies)
            fname_summary =self.fname_summary_currencies
        
        fnames_history = [x for x in os.listdir(dir_history) if x.endswith('.csv')]
        summary = []
        for fname_history in tqdm(fnames_history, mininterval=0.5):
            df = pd.read_csv(os.path.join(dir_history, fname_history))
            summary.append(self._summarize_history(df))
        summary = pd.concat(summary)

        summary.to_csv(os.path.join(dir_summary, fname_summary), index=False)
        return summary