COL_ETF_META_ORIG = [
    'symbol', 'short_name', 'long_name', 'currency', 'summary',
    'asset_subcategory', 'fund_family', 'exchange', 'market',  'total_assets'
]
COL_ETF_META_ADD = [
    'asset_category', 'category'
]
COL_ETF_META_DROP = [
    'total_assets' # info에도 있는데 그게 더 최신
]
COL_ETF_META = list(set(COL_ETF_META_ORIG + COL_ETF_META_ADD) - set(COL_ETF_META_DROP))

COL_ETF_INFO = [
    'symbol', 'total_assets', 'sector_weight', 'holdings', 'bond_rating'
]

COL_ETF_PROFILE_ORIG = [
    'net_assets_abbv', 'nav', 'per_ttm', 'yield', 'ytd_daily_total_return',
    'beta_5y-monthly', 'expense_ratio', 'inception_date', 'symbol'
]
COL_ETF_PROFILE_ADD = [
    'elapsed_year', 'net_assets_sig_figs', 'multiplier_mil','multiplier_bil', 'multiplier_tril', 
    'net_assets'
]
COL_ETF_PROFILE_DROP = [
    'net_assets_abbv', 'per_ttm', 'yield', 'ytd_daily_total_return',
    'beta_5y-monthly', 'net_assets_sig_figs', 'multiplier_mil','multiplier_bil', 'multiplier_tril' 
]
COL_ETF_PROFILE = list(set(COL_ETF_PROFILE_ORIG + COL_ETF_PROFILE_ADD) - set(COL_ETF_PROFILE_DROP))

COL_ETF_AUM = [
    'symbol', 'aum', 'shares_out'
]

COL_MASTER = list(set(COL_ETF_META + COL_ETF_INFO + COL_ETF_PROFILE + COL_ETF_AUM))

COL_HISTORY_RAW = [
    'date', 'symbol', 'open', 'high', 'low', 
    'close', 'volume', 'dividend', 'stock_split'
]