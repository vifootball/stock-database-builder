# bigquery
BQ_PROJECT_ID = 'between-buy-and-sell'
BQ_DATASET_ID = 'stock'
BQ_TABLE_ID_SUMMARY = 'summary'
BQ_TABLE_ID_HISTORY = 'history'

# name of dir
DIR_DOWNLOAD = 'download'

# name of subdir
SUBDIR_INFO_ETF = 'info_etf'
SUBDIR_PROFILE_ETF = 'profile_etf'

SUBDIR_HISTORY_RAW_ETF = 'history_raw_etf'
SUBDIR_HISTORY_RAW_CURRENCIES = 'history_raw_currencies'
SUBDIR_HISTORY_RAW_INDICES = 'history_raw_indices'

SUBDIR_HISTORY_PP_ETF = 'history_pp_etf'
SUBDIR_HISTORY_PP_CURRENCIES = 'history_pp_currencies'
SUBDIR_HISTORY_PP_INDICES = 'history_pp_indices'
SUBDIR_HISTORY_PP_CONCATENATED = 'history_pp_concatenated'

SUBDIR_SUMMARY = 'summary'
SUBDIR_RECENT = 'recent'

SUBDIR_MASTER = 'master'
SUBDIR_MASTER_INDICES = 'master_indices'

# name of file
FNAME_META_ETF = 'meta_etf.csv'
FNAME_INFO_ETF = 'info_etf.csv'
FNAME_PROFILE_ETF = 'profile_etf.csv'
FNAME_HISTORY_ETF = 'history_etf.csv'
FNAME_MASTER_ETF = 'master_etf.csv'
FNAME_MASTER_INDICES_YAHOO = 'master_indices_yahoo.csv'
FNAME_MASTER_INDICES_INVESTPY = 'master_indices_investpy.csv'
FNAME_MASTER_INDICES_FRED = 'master_indices_fred.csv'
FNAME_MASTER_INDICES = 'master_indices.csv'
FNAME_MASTER_CURRENCIES = 'master_currencies.csv'
FNAME_RECENT_ETF = 'recent_etf.csv'
FNAME_RECENT_CURRENCIES = 'recent_currencies.csv'
FNAME_RECENT_INDICES = 'recent_indices.csv'
FNAME_HISTORY_PP_ETF = 'history_pp_etf.csv'
FNAME_HISTORY_PP_CURRENCIES = 'history_pp_currencies.csv'
FNAME_HISTORY_PP_INDICES = 'history_pp_indices.csv'
FNAME_SUMMARY_ETF = 'summary_etf.csv'
FNAME_SUMMARY_INDICES = 'summary_indices.csv'
FNAME_SUMMARY_CURRENCIES = 'summary_currencies.csv'
FNAME_BENCHMARK = 'benchmark.csv'

# Meta ETF
COLS_META_ETF = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'asset_class',
    'isin', 'stock_exchange' 
]
_COLS_META_ETF = [
    'def_stock_exchange', 'category'
]

# Info ETF
DICT_COLS_INFO_ETF = {
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
COLS_INFO_ETF = [
    'name', 'market_cap', 'shares_outstanding'
]
_COLS_INFO_ETF =[
    'prev_close', 'todays_range', 'roi_ttm', 'open', '52_week_range', 
    'dividend_ttm', 'volume', 'dividend_yield_rate', 'volume_3m_avg', 'total_assets', 
    'beta', '1_year_change_rate', 'asset_class'
]

# Profile ETF
DICT_COLS_PROFILE_ETF = {
    'Net Assets': 'net_assets',
    'NAV': 'nav',
    'PE Ratio (TTM)': 'per_ttm',
    'Yield': 'yield',
    'YTD Daily Total Return': 'ytd_daily_total_return',
    'Beta (5Y Monthly)': 'beta_5y-monthly',
    'Expense Ratio (net)': 'expense_ratio',
    'Inception Date': 'inception_date'
}
COLS_PROFILE_ETF = [
    'symbol', 'fund_family',
    'net_assets', 'nav', 'expense_ratio', 'inception_date', 'elapsed_day'
]
_COLS_PROFILE_ETF = [
    'per_ttm', 'yield', 'ytd_daily_total_return', 'beta_5y-monthly', 
]



COLS_MASTER_BASIC = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'category'
]
COLS_MASTER_ENTIRE = sorted(list(set(COLS_MASTER_BASIC + COLS_META_ETF + COLS_INFO_ETF + COLS_PROFILE_ETF)))


COLS_HISTORY_RAW = [
 'date', 'symbol', 'full_name', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split'
] 

DICT_COLS_HISTORY_RAW = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
    'Dividends': 'dividend',
    'Stock Splits': 'stock_split'
}

DICT_COLS_RECESSION = {
    'DATE': 'date',
    'USREC': 'recession'
}

LIST_DICT_SYMBOLS_FRED = [
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