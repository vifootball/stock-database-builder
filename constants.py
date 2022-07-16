# bigquery
BQ_PROJECT_ID = 'between-buy-and-sell'
BQ_DATASET_ID = 'stock'
BQ_TABLE_ID_SUMMARY = 'summary'
BQ_TABLE_ID_HISTORY = 'history'

# name of dir
DIRNAME_DOWNLOAD = 'download'

# name of subdir
SUBDIRNAME_INFO_ETF = 'info_etf'
SUBDIRNAME_PROFILE_ETF = 'profile_etf'

SUBDIRNAME_HISTORY_RAW_ETF = 'history_raw_etf'
SUBDIRNAME_HISTORY_RAW_CURRENCIES = 'history_raw_currencies'
SUBDIRNAME_HISTORY_RAW_INDICES = 'history_raw_indices'

SUBDIRNAME_HISTORY_PP_ETF = 'history_pp_etf'
SUBDIRNAME_HISTORY_PP_CURRENCIES = 'history_pp_currencies'
SUBDIRNAME_HISTORY_PP_INDICES = 'history_pp_indices'
SUBDIRNAME_HISTORY_PP_CONCATENATED = 'history_pp_concatenated'

SUBDIRNAME_SUMMARY = 'summary'
SUBDIRNAME_RECENT = 'recent'

SUBDIRNAME_MASTER = 'master'
SUBDIRNAME_MASTER_INDICES = 'master_indices'

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

COLS_META_ETF = [
    'coutry', 'symbol', 'name', 'full_name', 'currency' 'asset_class',
    'isin', 'stock_exchange', 
]
COLS_INFO_ETF = [
    'ETF Name', 'ROI (TTM)', '52 wk Range', 'Dividends (TTM)', 'Volume', 
    'Market Cap', 'Dividend Yield', 'Average Vol. (3m)', 'Total Assets',
    '1-Year Change', 'Asset Class'
]
COLS_PROFILE_ETF = [
    'Symbol','Fund Family', 'Yield', 'Expense Ratio (net)', 'Inception Date', 'Net Assets', 'NAV'
]
COLS_ETF_INFO_TO_MASTER = [
    'etf_name', 'market_cap', 'total_assets', 'shares_outstanding'
]
COLS_ETF_PROFILE_TO_MASTER =[
    'symbol', 'fund_family', 'expense_ratio', 'inception_date', 'net_assets', 'nav'
]

COLS_MASTER_BASIC = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'category'
]
COLS_MASTER_ENTIRE = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'category', 'asset_class', 
    'fund_family', 'expense_ratio', 'inception_date', 'net_assets', 'nav', 'shares_outstanding',
    'market_cap', 'isin', 'stock_exchange',
]



COLS_HISTORY_RAW = [
 'date', 'symbol', 'full_name', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits'
] 

DICT_COLS_ETF_INFO = {
    'ETF Name': 'etf_name',
    'Prev. Close': 'Prev. Close',
    'Todays Range': 'todays_range',
    'ROI (TTM)': 'roi_ttm',
    'Open': 'open',
    '52 wk Range': '52_week_range',
    'Dividends (TTM)': 'dividends_ttm',
    'Volume': 'volume',
    'Market Cap': 'market_cap',
    'Dividend Yield': 'dividend_yield_rate',
    'Average Vol. (3m)': 'average_vol_3m',
    'Total Assets': 'total_assets',
    'Beta': 'beta',
    '1-Year Change': '1_year_change_rate',
    'Shares Outstanding': 'shares_outstanding'
}
DICT_COLS_ETF_PROFILE = {
    'Symbol': 'symbol',
    'Fund Family': 'fund_family',
    'Expense Ratio (net)': 'expense_ratio',
    'Inception Date': 'inception_date',
    'Net Assets': 'net_assets',
    'NAV': 'nav'
}
DICT_COLS_HISTORY_RAW = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
    'Dividends': 'dividends',
    'Stock Splits': 'stock_splits'
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