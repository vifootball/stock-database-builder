# bigquery
BQ_PROJECT_ID = 'between-buy-and-sell'
BQ_DATASET_ID = 'stock'
BQ_TABLE_ID_SUMMARY = 'summary'
BQ_TABLE_ID_HISTORY = 'history'

# name of dir
DIR_DOWNLOAD = 'download'

# name of subdir
SUBDIR_ETF_META = 'eff_meta'
SUBDIR_ETF_INFO = 'etf_info'
SUBDIR_RAW_ETF_INFO = 'raw_etf_info'
SUBDIR_ETF_PROFILE = 'etf_profile'
SUBDIR_RAW_ETF_PROFILE = 'raw_etf_profile'

SUBDIR_MASTER = 'master'

SUBDIR_ETF_HISTORY = 'etf_history'
SUBDIR_CURRENCY_HISTORY = 'currency_history'
SUBDIR_INDEX_YAHOO_HISTORY = 'index_yahoo_history'
SUBDIR_INDEX_INVESTPY_HISTORY = 'index_investpy_history'
SUBDIR_INDEX_FRED_HISTORY = 'index_fred_history'

SUBDIR_HISTORY_CHUNK = 'history_chunk'

SUBDIR_SUMMARY = 'summary'
SUBDIR_RECENT = 'recent'

# name of file
FNAME_ETF_METAS = 'etf_metas.csv'
FNAME_ETF_INFOS = 'etf_infos.csv'
FNAME_ETF_PROFILES = 'etf_profiles.csv'

FNAME_ETF_MASTERS = 'etf_masters.csv'
FNAME_INDEX_YAHOO_MASTERS = 'index_yahoo_masters.csv'
FNAME_INDEX_INVESTPY_MASTERS = 'index_investpy_masters.csv'
FNAME_INDEX_FRED_MASTERS = 'index_fred_mastersd.csv'
FNAME_CURRENCY_MASTERS = 'currency_masters.csv'

FNAME_RECENT_ETF = 'recent_etf.csv'
FNAME_RECENT_CURRENCIES = 'recent_currencies.csv'
FNAME_RECENT_INDICES = 'recent_indices.csv'
FNAME_HISTORY_PP_ETF = 'history_pp_etf.csv'
FNAME_HISTORY_PP_CURRENCIES = 'history_pp_currencies.csv'
FNAME_HISTORY_PP_INDICES = 'history_pp_indices.csv'

FNAME_ETF_SUMMARIES = 'etf_summaries.csv'
FNAME_INDEX_YAHOO_SUMMARIES = 'index_yahoo_summaries.csv'
FNAME_INDEX_INVESTPY_SUMMARIES = 'index_investpy_summaries.csv'
FNAME_INDEX_FRED_SUMMARIES = 'index_fred_summaries.csv'
FNAME_CURRENCY_SUMMARIES = 'currency_summaries.csv'


# Meta ETF
COL_MAPPER_RAW_ETF_META  = { # for rename
    'country': 'country', 
    'name': 'name', 
    'full_name': 'full_name', 
    'symbol': 'symbol', 
    'isin': 'isin', 
    'asset_class': 'asset_class', 
    'currency': 'currency', 
    'stock_exchange': 'stock_exchange', 
    'def_stock_exchange': 'def_stock_exchange'
}
COLS_PP_ETF_META_ORIG = [
    'country', 'name', 'full_name', 'symbol', 'isin',
    'asset_class', 'currency', 'stock_exchange', 'def_stock_exchange'
]
COLS_PP_ETF_META_DROP = [
    'def_stock_exchange'
]
COLS_PP_ETF_META_ADD = [
    'category'
]
COLS_PP_ETF_META = list(
    set(COLS_PP_ETF_META_ORIG + COLS_PP_ETF_META_ADD) - set(COLS_PP_ETF_META_DROP)
)

# Info ETF
COL_MAPPER_RAW_ETF_INFO = {
    'ETF Name': 'name',
    'Prev. Close': 'prev_close',
    'Todays Range': 'todays_range',
    'ROI (TTM)': 'roi_ttm',
    'Open': 'open',
    '52 wk Range': '52_week_range',
    'Dividends (TTM)': 'dividend_ttm',
    'Volume': 'volume',
    'Market Cap': 'market_cap',
    'Dividend Yield': 'dividend_yield_rate',
    'Average Vol. (3m)': 'volume_3m_avg',
    'Total Assets': 'total_assets',
    'Beta': 'beta',
    '1-Year Change': '1_year_change_rate',
    'Shares Outstanding': 'shares_outstanding',
    'Asset Class': 'asset_class'
}
COLS_PP_ETF_INFO_ORIG = [
    'name', 'market_cap', 'shares_outstanding',
    'prev_close', 'todays_range', 'roi_ttm', 'open', '52_week_range', 
    'dividend_ttm', 'volume', 'dividend_yield_rate', 'volume_3m_avg', 'total_assets', 
    'beta', '1_year_change_rate', 'asset_class'
]
COLS_PP_ETF_INFO_DROP = [
    'prev_close', 'todays_range', 'roi_ttm', 'open', '52_week_range', 
    'dividend_ttm', 'volume', 'dividend_yield_rate', 'volume_3m_avg', 'total_assets', 
    'beta', '1_year_change_rate', 'asset_class'
]
COLS_PP_ETF_INFO = list(
    set(COLS_PP_ETF_INFO_ORIG) - set(COLS_PP_ETF_INFO_DROP)
)

# Profile ETF
COL_MAPPER_RAW_ETF_PROFILE = {
    'Net Assets': 'net_assets_abbv',
    'NAV': 'nav',
    'PE Ratio (TTM)': 'per_ttm',
    'Yield': 'yield',
    'YTD Daily Total Return': 'ytd_daily_total_return',
    'Beta (5Y Monthly)': 'beta_5y-monthly',
    'Expense Ratio (net)': 'expense_ratio',
    'Inception Date': 'inception_date'
}
COLS_PP_ETF_PROFILE_ORIG = [
    'net_assets_abbv', 'nav', 'expense_ratio', 'inception_date',
    'per_ttm', 'yield', 'ytd_daily_total_return', 'beta_5y-monthly', 'net_assets_sig_figs',
    'multiplier_mil', 'multiplier_bil', 'multiplier_tril'
]
COLS_PP_ETF_PROFILE_DROP = [
    'per_ttm', 'yield', 'ytd_daily_total_return', 'beta_5y-monthly', 'net_assets_sig_figs',
    'multiplier_mil', 'multiplier_bil', 'multiplier_tril'
]
COLS_PP_ETF_PROFILE_ADD = [
    'symbol', 'fun_family', 'elapsed_day', 'net_assets'
]
COLS_PP_ETF_PROFILE = list(
    set(COLS_PP_ETF_PROFILE_ORIG + COLS_PP_ETF_PROFILE_ADD) - set(COLS_PP_ETF_PROFILE_DROP)
)

# Master
COLS_MASTER_COMMON = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'category'
]
COLS_MASTER_ENTIRE = sorted(list(
    set(COLS_MASTER_COMMON + COLS_PP_ETF_META + COLS_PP_ETF_INFO + COLS_PP_ETF_PROFILE)
))

# History
COLS_MAPPER_RAW_HISTORY = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
    'Dividends': 'dividend',
    'Stock Splits': 'stock_split'
}
COLS_HISTORY_RAW = [
 'date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split'
] 


FRED_METAS = [
    {
     'symbol'   : 'CPIAUCSL',
     'name'     : 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average',
     'full_name': 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'EFFR',
     'name'     : 'Effective Federal Funds Rate',
     'full_name': 'Effective Federal Funds Rate', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'T10Y2Y',
     'name'     : '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity',
     'full_name': '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },    
    {
     'symbol'   : 'T10Y3M',
     'name'     : '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity',
     'full_name': '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'T10YIE',
     'name'     : '10-Year Breakeven Inflation Rate',
     'full_name': '10-Year Breakeven Inflation Rate', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'USREC',
     'name'     : 'NBER based Recession Indicators for the United States from the Period following the Peak through the Trough',
     'full_name': 'NBER based Recession Indicators for the United States from the Period following the Peak through the Trough', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'USRECM',
     'name'     : 'NBER based Recession Indicators for the United States from the Peak through the Trough',
     'full_name': 'NBER based Recession Indicators for the United States from the Peak through the Trough', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'USRECP',
     'name'     : 'NBER based Recession Indicators for the United States from the Peak through the Period preceding the Trough',
     'full_name': 'NBER based Recession Indicators for the United States from the Peak through the Period preceding the Trough', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'USAREC',
     'name'     : 'OECD based Recession Indicators for the United States from the Period following the Peak through the Trough',
     'full_name': 'OECD based Recession Indicators for the United States from the Period following the Peak through the Trough', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'USARECM',
     'name'     : 'OECD based Recession Indicators for the United States from the Peak through the Trough',
     'full_name': 'OECD based Recession Indicators for the United States from the Peak through the Trough', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'T10YIE',
     'name'     : '10-Year Breakeven Inflation Rate',
     'full_name': '10-Year Breakeven Inflation Rate', 
     'country'  : 'united states', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'DEXKOUS',
     'name'     : 'South Korean Won to U.S. Dollar Spot Exchange Rate',
     'full_name': 'South Korean Won to U.S. Dollar Spot Exchange Rate', 
     'country'  : 'south korea', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'INTDSRKRM193N',
     'name'     : 'Interest Rates, Discount Rate for Republic of Korea',
     'full_name': 'Interest Rates, Discount Rate for Republic of Korea', 
     'country'  : 'south korea', 
     'currency' : None, 
     'category' : 'index'
    },
    {
     'symbol'   : 'KORCPIALLMINMEI',
     'name'     : 'Consumer Price Index: All Items for Korea',
     'full_name': 'Consumer Price Index: All Items for Korea', 
     'country'  : 'south korea', 
     'currency' : None, 
     'category' : 'index'
    }
]