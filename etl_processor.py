import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt
#from tqdm.notebook import tqdm
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
import investpy
import yfinance as yf
import pandas_datareader.data as web
from constants import *
from utils import *

# 티커가 바뀔 수 있으니, 마스터를 add 방식으로 수집하고 drop duplicates?
# price, date 결측치 보간하기


class EtlProcessor:
    def __init__(self):
        self.bq_project_id = BQ_PROJECT_ID
        self.bq_dataset_id = BQ_DATASET_ID
        self.bq_table_id_summary = BQ_TABLE_ID_SUMMARY
        self.bq_table_id_history = BQ_TABLE_ID_HISTORY

        self.dir_download = DIRNAME_DOWNLOAD
        self.subdir_profile_etf = SUBDIRNAME_PROFILE_ETF
        self.subdir_info_etf = SUBDIRNAME_INFO_ETF
        self.subdir_master_indices = SUBDIRNAME_MASTER_INDICES
        self.subdir_master = SUBDIRNAME_MASTER
        self.subdir_history_raw_etf = SUBDIRNAME_HISTORY_RAW_ETF
        self.subdir_history_raw_indices = SUBDIRNAME_HISTORY_RAW_INDICES
        self.subdir_history_raw_currencies = SUBDIRNAME_HISTORY_RAW_CURRENCIES
        self.subdir_history_pp_etf = SUBDIRNAME_HISTORY_PP_ETF
        self.subdir_history_pp_indices = SUBDIRNAME_HISTORY_PP_INDICES
        self.subdir_history_pp_currencies = SUBDIRNAME_HISTORY_PP_CURRENCIES
        self.subdir_history_pp_concatenated = SUBDIRNAME_HISTORY_PP_CONCATENATED
        self.subdir_recent = SUBDIRNAME_RECENT
        self.subdir_summary = SUBDIRNAME_SUMMARY
        
        self.fname_meta_etf = FNAME_META_ETF
        self.fname_info_etf = FNAME_INFO_ETF
        self.fname_profile_etf = FNAME_PROFILE_ETF
        self.fname_master_etf = FNAME_MASTER_ETF
        self.fname_master_currencies = FNAME_MASTER_CURRENCIES
        self.fname_master_indices_yahoo = FNAME_MASTER_INDICES_YAHOO
        self.fname_master_indices_investpy = FNAME_MASTER_INDICES_INVESTPY
        self.fname_master_indices_fred = FNAME_MASTER_INDICES_FRED
        self.fname_master_indices = FNAME_MASTER_INDICES
        
        self.fname_history_pp_etf = FNAME_HISTORY_PP_ETF
        self.fname_history_pp_currencies = FNAME_HISTORY_PP_CURRENCIES
        self.fname_history_pp_indices = FNAME_HISTORY_PP_INDICES
        
        self.fname_recent_etf = FNAME_RECENT_ETF
        self.fname_recent_indices = FNAME_RECENT_INDICES
        self.fname_recent_currencies = FNAME_RECENT_CURRENCIES
        
        self.fname_benchmark = FNAME_BENCHMARK
        
        self.fname_summary_etf = FNAME_SUMMARY_ETF
        self.fname_summary_indices = FNAME_SUMMARY_INDICES
        self.fname_summary_currencies = FNAME_SUMMARY_CURRENCIES

        # self.cols_etf_profile = COLS_PROFILE_ETF
        self.cols_etf_info_to_master = COLS_ETF_INFO_TO_MASTER
        self.cols_etf_profile_to_master = COLS_ETF_PROFILE_TO_MASTER
        self.cols_etf_entire = COLS_MASTER_ENTIRE
        self.dict_cols_etf_info = DICT_COLS_ETF_INFO
        self.dict_cols_etf_profile = DICT_COLS_ETF_PROFILE
        self.dict_cols_history_raw = DICT_COLS_HISTORY_RAW
        self.dict_cols_recession = DICT_COLS_RECESSION
        self.list_dict_symbols_fred = LIST_DICT_SYMBOLS_FRED

        os.makedirs(self.dir_download, exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_info_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_profile_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_raw_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_raw_indices), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_raw_currencies), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_pp_etf), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_pp_indices), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_pp_currencies), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_history_pp_concatenated), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_recent), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_summary), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_master), exist_ok=True)
        os.makedirs(os.path.join(self.dir_download, self.subdir_master_indices), exist_ok=True)

    @measure_time
    def get_meta_etf(self):
        etf_meta = investpy.etfs.get_etfs(country='united states')
        etf_meta['category'] = 'etf'
        etf_meta.to_csv(os.path.join(self.dir_download, self.fname_meta_etf), index=False)
        print("Finished Getting Metadata of ETFs")
        return etf_meta

    @measure_time
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

    @measure_time
    def get_profile_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))#[:15]
        # fnames_profile = sorted(os.listdir(os.path.join(self.dir_download, self.subdir_profile_etf)))
        symbols = sorted([x for x in etf_meta['symbol']])
        # symbols_already_have = sorted([x.lstrip('profile_').rstrip('.csv') for x in fnames_profile if x.endswith('.csv')])        
        # symbols_to_get = sorted([x for x in symbols if x not in symbols_already_have]) # refresh mode, 이어서 모드

        #for i, symbol in enumerate(tqdm(symbols_to_get, mininterval=0.5)):
        for i, symbol in enumerate(tqdm(symbols, mininterval=0.5)):
            fpath = os.path.join(self.dir_download, self.subdir_profile_etf, f'profile_{symbol}.csv')
            if not os.path.exists(fpath):
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
                profile['multiplier_tril'] = (profile["net_assets_original"].str.endswith('T').astype('int') * (1000_000_000_000-1)) + 1
                profile['net_assets'] = profile['net_assets_original'].str.extract('([0-9.]*)').astype('float')
                profile['net_assets'] *= profile['multiplier_mil'] * profile['multiplier_bil'] * profile['multiplier_tril']
                
                profile.to_csv(fpath, index=False)

    @measure_time
    def concat_info_etf(self):
        concat_csv_files_in_dir(
            get_dir=os.path.join(self.dir_download, self.subdir_info_etf),
            put_dir=self.dir_download,
            fname='info_etf.csv'
        )   

    @measure_time
    def concat_profile_etf(self):
        concat_csv_files_in_dir(
            get_dir=os.path.join(self.dir_download, self.subdir_profile_etf),
            put_dir=self.dir_download,
            fname='profile_etf.csv'
        )

    @measure_time
    def construct_master_etf(self):
        etf_meta = pd.read_csv(os.path.join(self.dir_download, self.fname_meta_etf))
        etf_info = pd.read_csv(os.path.join(self.dir_download, self.fname_info_etf))[self.cols_etf_info_to_master]
        etf_profile = pd.read_csv(os.path.join(self.dir_download, self.fname_profile_etf))[self.cols_etf_profile_to_master]
                
        etf_master = etf_meta.merge(etf_info, how='left', left_on='name', right_on='etf_name')
        etf_master = etf_master.merge(etf_profile, how='left', left_on='symbol', right_on='symbol')
        etf_master = etf_master[self.cols_etf_entire]
        etf_master.to_csv(os.path.join(self.dir_download, self.subdir_master, self.fname_master_etf), index=False)
        return etf_master

    @measure_time
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

        df_yahoo = pd.concat(dfs).reset_index(drop=True)[COLS_MASTER_BASIC]
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df_yahoo = pd.concat([header, df_yahoo])
        
        fpath = os.path.join(self.dir_download, self.subdir_master_indices, self.fname_master_indices_yahoo)
        df_yahoo.to_csv(fpath, index=False)

        return df_yahoo

    @measure_time
    def get_master_indices_investpy(self):
        countries = ['united states', 'south korea']
        df_indices = investpy.indices.get_indices()
        df_indices = df_indices[df_indices['country'].isin(countries)].reset_index(drop=True)
        df_indices['symbol'] = '^' + df_indices['symbol']
        df_indices['category'] = 'index'
        df_indices = df_indices[COLS_MASTER_BASIC]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df_indices = pd.concat([header, df_indices])
        
        fpath = os.path.join(self.dir_download, self.subdir_master_indices, self.fname_master_indices_investpy)
        df_indices.to_csv(fpath, index=False)
        return df_indices

    @measure_time
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
            symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
            # if base_cur == 'USD':
            #     symbol = f'{second_cur}=X'
            # else:
            #     symbol = f'{base_cur}{second_cur}=X'
            return symbol
        currencies['symbol'] = currencies['name'].apply(_encode_symbol)
        currencies = currencies[COLS_MASTER_BASIC]
        
        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        currencies = pd.concat([header, currencies]) 

        currencies.to_csv(os.path.join(self.dir_download, self.subdir_master, self.fname_master_currencies), index=False)
        return currencies

    @measure_time
    def get_master_indices_fred(self):
        df = pd.DataFrame(self.list_dict_symbols_fred)[COLS_MASTER_BASIC]

        header = pd.DataFrame(columns=COLS_MASTER_ENTIRE)
        df = pd.concat([header, df]
        )
        fpath = os.path.join(self.dir_download, self.subdir_master_indices, self.fname_master_indices_fred)
        df.to_csv(fpath, index=False)
        return df

    @measure_time
    def integrate_master(self):
        master_etf = pd.read_csv(os.path.join(self.dir_download, self.fname_master_etf))
        master_indices_yahoo = pd.read_csv(os.path.join(self.dir_download, self.fname_master_indices_yahoo))
        master_indices_investpy = pd.read_csv(os.path.join(self.dir_download, self.fname_master_indices_investpy))
        master_currencies = pd.read_csv(os.path.join(self.dir_download, self.fname_master_currencies))
        master_indices_fred = pd.read_csv(os.path.join(self.dir_download, self.fname_master_indices_fred))
        
        integrated_df = pd.concat([
            master_etf,
            master_indices_yahoo,
            master_indices_investpy,
            master_currencies,
            master_indices_fred
        ]).reset_index(drop=True)
        integrated_df.to_csv(os.path.join(self.dir_download, 'master.csv'), index=False, encoding='utf-8-sig')
        return integrated_df

    @measure_time
    def get_history_from_yf(self, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            master_df = pd.read_csv(os.path.join(self.dir_download, self.fname_master_indices))
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_indices)
        elif category == "etf":
            master_df = pd.read_csv(os.path.join(self.dir_download, self.fname_master_etf))
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_etf)
        elif category == "currency":
            master_df = pd.read_csv(os.path.join(self.dir_download, self.fname_master_currencies))
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_currencies)

        #있으면 하고 없으면 말기 # os.path.exists()
        for row in tqdm(master_df.itertuples(), total=len(master_df), mininterval=0.5):
            symbol = getattr(row, 'symbol')
            fpath = os.path.join(dir_history_raw, f'history_raw_{symbol}.csv')
            if not os.path.exists(fpath):
                time.sleep(0.1)
                i = getattr(row, 'Index') # enumerate i 의 용도
                history = yf.Ticker(symbol).history(period='max')
                history = history.reset_index()
                history.rename(columns=self.dict_cols_history_raw, inplace=True)
                if len(history) > 0:
                    time_since_last_traded = dt.datetime.today() - history['date'].max()
                    if time_since_last_traded < pd.Timedelta('30 days'):
                        # history = history.asfreq(freq = "1d").reset_index()
                        history['date'] = history['date'].astype('str')
                        # history['country'] = getattr(row, 'country') # 마스터에 있어서 필요 없음
                        history['symbol'] = getattr(row, 'symbol')
                        history['full_name'] = getattr(row, 'full_name')
                        history.to_csv(fpath, index=False)
                else:
                    print(f'Empty DataFrame at Loop {i}: {symbol}')

    @measure_time
    def get_history_from_fred(self, master_df):
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        for row in tqdm(master_df.itertuples(), total=len(master_df)):
            i = getattr(row, 'Index')
            symbol = getattr(row, 'symbol')
            history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
            history = history.reset_index()
            history.rename(columns={f'{symbol}':'close'}, inplace=True)
            history.rename(columns={'DATE':'date'}, inplace=True)
            history['symbol'] = getattr(row, 'symbol')
            history['full_name'] = getattr(row, 'full_name')
            history['date'] = history['date'].astype('str')

            header = pd.DataFrame(columns=COLS_HISTORY_RAW)
            history = pd.concat([header, history])
            history.to_csv(os.path.join(self.dir_download, self.subdir_history_raw_indices, f'history_raw_{symbol}.csv'), index=False)

    @measure_time
    def get_benchmark(self): 
        # recession
        fred_start, fred_end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        recession = web.DataReader('USREC', 'fred', fred_start, fred_end).asfreq(freq = "1d", method = 'ffill')
        cpi = web.DataReader('CPIAUCSL', 'fred', fred_start, fred_end).asfreq(freq = "1d", method = 'ffill')
        # others
        snp_500 = yf.Ticker('^GSPC').history(period='max')['Close']
        kospi = yf.Ticker('^ks11').history(period='max')['Close']
        usd_krw = yf.Ticker('krw=x').history(period='max')['Close']
        usd_idx = yf.Ticker('DX-Y.NYB').history(period='max')['Close']
        # 모두 인덱스가 날짜라서 공통인덱스를 이용하여 컬럼방향 conccat
        benchmark = pd.concat([recession, snp_500, kospi, usd_krw, usd_idx], axis = 'columns').reset_index(drop=False)
        benchmark.columns = ['date', 'recession', 'cpi', 'snp_500', 'kospi', 'usd_krw', 'usd_idx']
        benchmark.to_csv(os.path.join(self.dir_download, self.fname_benchmark), index=False)
        return benchmark

    def _join_benchmark(self, history):
        benchmark = pd.read_csv(os.path.join(self.dir_download, self.fname_benchmark))
        history = history.merge(benchmark, how='left', on='date', suffixes=[None, '_y'])
        return history

    def _calculate_features(self, history):
        history['date'] = pd.to_datetime(history['date'])
        history = history.set_index('date')

        # dividends
        history['dividends_paid'] = np.sign(history['dividends'])
        history['dividends_paid_count_12m'] = history['dividends_paid'].rolling(window='365d').sum().to_numpy() # tonumpy 안하며 nan 반환..
        history['dividends_trailing_12m'] = history['dividends'].rolling(window='365d').sum().to_numpy()
        history['dividends_trailing_6m'] = (history['dividends'].rolling(window='183d').sum().to_numpy())*2
        history['dividends_rate_trailing_12m'] = history['dividends_trailing_12m']/history['close']
        history['dividends_rate_trailing_6m'] = history['dividends_trailing_6m']/history['close']

        # prices
        history['change'] = history['close'].diff().fillna(0)
        history['change_sign'] = np.sign(history['change'])
        history['all_time_high'] = history['close'].cummax()
        history['drawdown'] = ((history['close']/history['all_time_high']) - 1).round(6)
        history['max_drawdown'] =history['drawdown'].cummin().round(6)
        history['change_rate'] = history['change']/history['close']

        history['close_avg_1y'] = history['close'].rolling(window='365d').mean().to_numpy()
        history['close_max_1y'] = history['close'].rolling(window='365d').max().to_numpy()
        history['close_min_1y'] = history['close'].rolling(window='365d').min().to_numpy()

        # volumes
        history['volume_avg_1y'] = history['volume'].rolling(window='365d').mean().to_numpy().round(0)
        history['volume_avg_6m'] = history['volume'].rolling(window='180d').mean().to_numpy().round(0)
        history['volume_avg_3m'] = history['volume'].rolling(window='90d').mean().to_numpy().round(0)

        # rounding
        cols_round_0 = [
            'volume', 'volume_avg_1y', 'volume_avg_6m', 'volume_avg_3m'
        ]
        cols_round_3 = [
            'open', 'high', 'low', 'close',
            'dividends_trailing_12m', 'dividends_trailing_6m',
            'close_avg_1y', 'close_max_1y', 'close_min_1y',
            'change', 'all_time_high'
        ]
        cols_round_6 = [
            'dividends_rate_trailing_12m','dividends_rate_trailing_6m',
            'drawdown', 'max_drawdown', 'change_rate'
        ]
        history[cols_round_0] = history[cols_round_0].round(0)
        history[cols_round_3] = history[cols_round_3].round(3)
        history[cols_round_6] = history[cols_round_6].round(6)

        # percentile (시간 많이 걸릴 것 같음)

        # df['drawdown_00']
        # df['drawdown_08']
        # df['drawdown_20']
        # df['drawdown_22']

        # df['change_1year']
        # df['change_month']

        # df['momentum_score_rolling_1y'] = np.sign(df['momentum_rolling_1y'])
        # df['momentum_score_rolling_6m'] = np.sign(df['momentum_rolling_6m'])
        # df['bull_bear'] = df['change_rate']
        # df['bull_bear'] = df['drawdown'].apply(lambda x: 'bear' if x>=-0.03 else 'bull') # 일주일 이동평균 이용해도 괜찮을듯 # B_B_w, B_B_m

        '''
        '''
        # 직전 거래일 기준으로 지표를 계산하는 경우가 있으므로 (change_sign 등) 1일주기로 데이터를 바꾸는 것은 맨 나중에 해줌
        history = history.asfreq(freq = "1d").reset_index()
        return history
    
    @measure_time
    def preprocess_history(self, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_indices)
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_indices)
        elif category == "etf":
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_etf)
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_etf)
        elif category == "currency":
            dir_history_raw = os.path.join(self.dir_download, self.subdir_history_raw_currencies)
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_currencies)
        
        fnames_history_raw = [x for x in os.listdir(dir_history_raw) if x.endswith('.csv')]
        for fname_history_raw in tqdm(fnames_history_raw, mininterval=0.5):
            history = pd.read_csv(os.path.join(dir_history_raw, fname_history_raw))
            symbol = history['symbol'].iat[0]
            
            history = self._calculate_features(history)
            #history = self._join_benchmark
            history.to_csv(os.path.join(dir_history_pp, f'history_pp_{symbol}.csv'), index=False)
        
        print(f"Finished Preprocessing History: {category}")

    @measure_time
    def get_recent_from_history(self, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_indices)
            dir_recent = os.path.join(self.dir_download, self.subdir_recent)
            fname_recents = self.fname_recent_indices
        elif category == "etf":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_etf)
            dir_recent = os.path.join(self.dir_download, self.subdir_recent)
            fname_recents = self.fname_recent_etf
        elif category == "currency":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_currencies)
            dir_recent = os.path.join(self.dir_download, self.subdir_recent)
            fname_recents = self.fname_recent_currencies

        recents = []
        fnames_history_pp = [x for x in os.listdir(dir_history_pp) if x.endswith('.csv')]
        for fname_history_pp in tqdm(fnames_history_pp, mininterval=0.5):
            history = pd.read_csv(os.path.join(dir_history_pp, fname_history_pp))
            history = history.loc[history['close'].notnull()] # 휴장일 제외 최근
            recent = history.iloc[-1]
            recents.append(recent)
        recents = pd.DataFrame(recents).reset_index(drop=True)
        recents.to_csv(os.path.join(dir_recent, fname_recents), index=False)
        print(f"Finished Extracting Recent Data of Histories: {category}")
        return recents
    
    @measure_time
    def concat_master_indices(self):
        concat_csv_files_in_dir(
            get_dir = os.path.join(self.dir_download, self.subdir_master_indices),
            put_dir = os.path.join(self.dir_download, self.subdir_master),
            fname = self.fname_master_indices
        )
    
    @measure_time
    def concat_history(self, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_indices)
            fname = self.fname_history_pp_indices
        elif category == "etf":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_etf)
            fname = self.fname_history_pp_etf
        elif category == "currency":
            dir_history_pp = os.path.join(self.dir_download, self.subdir_history_pp_currencies)
            fname = self.fname_history_pp_currencies

        put_dir = os.path.join(self.dir_download, self.subdir_history_pp_concatenated)
        
        concat_csv_files_in_dir(
            get_dir=dir_history_pp,
            put_dir=put_dir,
            fname=fname
        )

    @measure_time
    def construct_summary(self, category): # Master + Recent
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'
        if category == "index":
            master = pd.read_csv(os.path.join(self.dir_download, self.subdir_master, self.fname_master_indices))
            recent = pd.read_csv(os.path.join(self.dir_download, self.subdir_recent, self.fname_recent_indices))
            fpath_summary = os.path.join(self.dir_download, self.subdir_summary, self.fname_summary_indices)
        elif category == "etf":
            master = pd.read_csv(os.path.join(self.dir_download, self.subdir_master, self.fname_master_etf))
            recent = pd.read_csv(os.path.join(self.dir_download, self.subdir_recent, self.fname_recent_etf))
            fpath_summary = os.path.join(self.dir_download, self.subdir_summary, self.fname_summary_etf)
        elif category == "currency":
            master = pd.read_csv(os.path.join(self.dir_download, self.subdir_master, self.fname_master_currencies))
            recent = pd.read_csv(os.path.join(self.dir_download, self.subdir_recent, self.fname_recent_currencies))
            fpath_summary = os.path.join(self.dir_download, self.subdir_summary, self.fname_summary_currencies)

        summary = pd.merge(master, recent, how='inner', on=['symbol', 'full_name'])
        summary.to_csv(fpath_summary, index=False)
        return summary
    # 데이터 확인
    # 즐겨야 되는데 스트레스를 받는다..?

    def load_summary_to_bq(self):
        pass

    def load_history_to_bq(self):
        pass

    def load_summary_from_bq(self):
        pass

    def load_history_from_bq(self):
        pass