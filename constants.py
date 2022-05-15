DIRNAME_DOWNLOAD = 'download'  # mac
DIRNAME_DOWNLOAD = 'download' # window

SUBDIRNAME_ETF_HISTORY = 'etf_history'

FNAME_ETF_META = 'etf_meta.csv'
FNAME_ETF_INFO = 'etf_info.csv'
FNAME_ETF_PROFILE = 'etf_profile.csv'
FNAME_ETF_HISTORY = 'etf_history.csv.gzip'
FNAME_ETF_MASTER = 'etf_master.csv'

COLS_ETF_META = [
    'coutry', 'symbol', 'name', 'full_name', 'currency' 'asset_class',
    'isin', 'stock_exchange', 
]
COLS_ETF_INFO = [
    'ETF Name', 'ROI (TTM)', '52 wk Range', 'Dividends (TTM)', 'Volume', 
    'Market Cap', 'Dividend Yield', 'Average Vol. (3m)', 'Total Assets',
    '1-Year Change', 'Asset Class'
]
COLS_ETF_PROFILE = [
    'Symbol','Fund Family', 'Expense Ratio (net)', 'Inception Date', 'Net Assets', 'NAV'
]
COLS_ETF_MASTER = [
    'country', 'symbol', 'name', 'full_name', 'currency', 'category',
    'fund_family', 'expense_ratio', 'inception_date', 'net_assets', 'nav',
    'isin', 'stock_exchange', '52_week_range', 'dividends_ttm', 'volume', 'market_cap',
    'dividend_yield_rate', 'average_vol_3m', 'total_assets', '1_year_change_rate'
]
COLS_INDICES_MASTER = [
    'coutry', 'symbol', 'name', 'full_name', 'currency' 'category'
]
COLS_HISTORY = [
    'symbol', 'full_name', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits'
]

DICT_COLS_ETF_INFO = {
    'ETF Name': 'etf_name',
    '52 wk Range': '52_week_range',
    'Dividends (TTM)': 'dividends_ttm',
    'Volume': 'volume',
    'Market Cap': 'market_cap',
    'Dividend Yield': 'dividend_yield_rate',
    'Average Vol. (3m)': 'average_vol_3m',
    'Total Assets': 'total_assets',
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
DICT_COLS_HISTORY = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
    'Dividends': 'dividends',
    'Stock Splits': 'stock_splits'
}
DICT_INDICES_INVESTPY_TO_YFINANCE = {
    'SPX': 'GSPC'
}

DICT_SYMBOLS_FRED = {
    'CPIAUCSL': 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average',
    'EFFR': 'Effective Federal Funds Rate',
    'T10Y2Y': '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity',
    'T10Y3M': '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity',
    'T10YIE': '10-Year Breakeven Inflation Rate',
    'USREC': 'NBER based Recession Indicators for the United States from the Period following the Peak through the Trough',
    'USRECM': 'NBER based Recession Indicators for the United States from the Peak through the Trough',
    'USRECP': 'NBER based Recession Indicators for the United States from the Peak through the Period preceding the Trough',
    'USAREC': 'OECD based Recession Indicators for the United States from the Period following the Peak through the Trough',
    'USARECM': 'OECD based Recession Indicators for the United States from the Peak through the Trough',
    'DEXKOUS': 'South Korean Won to U.S. Dollar Spot Exchange Rate',
    'INTDSRKRM193N': 'Interest Rates, Discount Rate for Republic of Korea',
    'KORCPIALLMINMEI': 'Consumer Price Index: All Items for Korea',
    
}
