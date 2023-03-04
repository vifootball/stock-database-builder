# 'symbol' = COLUMN(name_adj='symbol', select=True, dtype='str')
class ColumnConfig:
	def __init__(self, name_adj, select=True, bq_dtype='STRING'):
		self.name_adj = name_adj
		self.select = select
		self.bq_dtype = bq_dtype
		# self.is_index = is_index

SRC_FD_META = {
	'symbol': ColumnConfig(name_adj='symbol', select=True),
	'short_name': ColumnConfig(name_adj='short_name', select=True),
	'long_name': ColumnConfig(name_adj='name', select=True),
	'currency': ColumnConfig(name_adj='currency', select=False),
	'summary': ColumnConfig(name_adj='description', select=True),
	'category': ColumnConfig(name_adj='asset_subcategory', select=True),
	'family': ColumnConfig(name_adj='fund_family', select=True),
	'exchange': ColumnConfig(name_adj='exchange', select=False),
	'market': ColumnConfig(name_adj='market', select=False),
	'total_assets': ColumnConfig(name_adj='total_assets', select=True)
}

TRG_FD_META = {
	'symbol': ColumnConfig(name_adj='symbol', select=True),
	'short_name': ColumnConfig(name_adj='short_name', select=True),
	'name': ColumnConfig(name_adj='name', select=True),
	'description': ColumnConfig(name_adj='description', select=True),
	'category': ColumnConfig(name_adj='category', select=True),
	'asset_category': ColumnConfig(name_adj='asset_category', select=True),
	'asset_subcategory': ColumnConfig(name_adj='asset_subcategory', select=True),
	'family': ColumnConfig(name_adj='fund_family', select=True),
	'total_assets': ColumnConfig(name_adj='total_assets', select=True)
}

PROFILE = {
	'Net Assets': ColumnConfig(name_adj='net_assets', select=True),
	'NAV': ColumnConfig(name_adj='nav', select=True),
	'PE Ratio (TTM)': ColumnConfig(name_adj='per_ttm', select=False),
	'Yield': ColumnConfig(name_adj='yield', select=True),
	'YTD Daily Total Return': ColumnConfig(name_adj='ytd_datily_total_return', select=False),
	'Beta (5Y Monthly)': ColumnConfig(name_adj='beta_5y_monthly', select=False),
	'Expense Ratio (net)': ColumnConfig(name_adj='expense_ratio', select=True),
	'Inception Date': ColumnConfig(name_adj='inception_date', select=True),
}

AUM = {
	'aum': ColumnConfig(name_adj='aum', select=True),
	'shares_out': ColumnConfig(name_adj='shares_out', select=True)
}

HOLDINGS = {
	'maxAge': ColumnConfig(name_adj='maxAge', select=False),
	'stockPosition': ColumnConfig(name_adj='stock_position', select=True),
	'bondPosition': ColumnConfig(name_adj='bond_position', select=True),
	'holdings': ColumnConfig(name_adj='holdings', select=True),
	'bondRatings': ColumnConfig(name_adj='bond_ratings', select=True),
	'sectorWeightings': ColumnConfig(name_adj='sector_weightings', select=True),
	'equityHoldings.priceToEarnings': ColumnConfig(name_adj='price_to_earnings', select=False),
	'equityHoldings.priceToBook': ColumnConfig(name_adj='price_to_book', select=False),
	'equityHoldings.priceToSales': ColumnConfig(name_adj='price_to_sales', select=False),
	'equityHoldings.priceToCashflow': ColumnConfig(name_adj='price_to_cashflow', select=False),
	'bondHoldings.maturity': ColumnConfig(name_adj='maturity', select=True), # 일부 채권에만 존재
	'bondHoldings.duration': ColumnConfig(name_adj='duration', select=True) # 채권에만 존재
}

METADATA_COMMON = {
	'symbol': ColumnConfig(name_adj='symbol_pk'),
	'name': ColumnConfig(name_adj='name'),
	'short_name': ColumnConfig(name_adj='short_name'),
	'category': ColumnConfig(name_adj='category')
}

CURRENCY = {
	'symbol': ColumnConfig(name_adj='symbol_pk', select=True),
	'name': ColumnConfig(name_adj='short_name', select=True),
	'full_name': ColumnConfig(name_adj='name', select=True),
	'base': ColumnConfig(name_adj='base', select=False),
	'category': ColumnConfig(name_adj='category', select=True),
	'base_name': ColumnConfig(name_adj='base_name', select=False),
	'second': ColumnConfig(name_adj='second', select=False),
	'second_name': ColumnConfig(name_adj='second_name', select=False)
}

METADATA = {
	# FD_META
	'symbol': ColumnConfig(name_adj='symbol_pk', bq_dtype='STRING'),
	'short_name': ColumnConfig(name_adj='short_name', bq_dtype='STRING'),
	'name': ColumnConfig(name_adj='name', bq_dtype='STRING'),
	'category': ColumnConfig(name_adj='category', bq_dtype='STRING'),
	'description': ColumnConfig(name_adj='description', bq_dtype='STRING'),
	'asset_category': ColumnConfig(name_adj='asset_category', bq_dtype='STRING'),
	'asset_subcategory': ColumnConfig(name_adj='asset_subcategory', bq_dtype='STRING'),
	'fund_family': ColumnConfig(name_adj='fund_family', bq_dtype='STRING'),
	'total_assets': ColumnConfig(name_adj='total_assets', bq_dtype='FLOAT64'),
	# PROFILE
	'net_assets': ColumnConfig(name_adj='net_assets', bq_dtype='FLOAT64'),
	'nav': ColumnConfig(name_adj='nav', select=False),
	'yield': ColumnConfig(name_adj='yield', select=False),
	'expense_ratio': ColumnConfig(name_adj='expense_ratio', bq_dtype='FLOAT64'),
	'inception_date': ColumnConfig(name_adj='inception_date', bq_dtype='STRING'),
	# AUM
	'aum': ColumnConfig(name_adj='aum', bq_dtype='FLOAT64'),
	'shares_out': ColumnConfig(name_adj='shares_out', bq_dtype='FLOAT64'),
	# HOLDINGS
	'stock_position': ColumnConfig(name_adj='stock_position', bq_dtype='FLOAT64'),
	'bond_position': ColumnConfig(name_adj='bond_position', bq_dtype='FLOAT64'),
	'holdings': ColumnConfig(name_adj='holdings', bq_dtype='STRING'),
	'bond_ratings': ColumnConfig(name_adj='bond_ratings', bq_dtype='STRING'),
	'sector_weightings': ColumnConfig(name_adj='sector_weightings', bq_dtype='STRING'),
	'maturity': ColumnConfig(name_adj='maturity', bq_dtype='FLOAT64'), # 일부 채권에만 존재
	'duration': ColumnConfig(name_adj='duration', bq_dtype='FLOAT64') # 채권에만 존재
}

SRC_HISTORY = {
	'symbol': ColumnConfig(name_adj='symbol_fk', select=True),
	'Date': ColumnConfig(name_adj='date', select=True),
	'Open': ColumnConfig(name_adj='open', select=True),
	'High': ColumnConfig(name_adj='high', select=True),
	'Low': ColumnConfig(name_adj='low', select=True),
	'Close': ColumnConfig(name_adj='close', select=True),
	'Volume': ColumnConfig(name_adj='volume', select=True),
	'Dividends': ColumnConfig(name_adj='dividend', select=True),
	'Stock Splits': ColumnConfig(name_adj='stock_split', select=True),
	'Capital Gains': ColumnConfig(name_adj='capital_gain', select=False)
}

TRG_HISTORY = {
	'transaction_pk': ColumnConfig(name_adj='transaction_pk', bq_dtype='STRING'),
	'date_fk': ColumnConfig(name_adj='date_fk', bq_dtype='INT64'),
	'date': ColumnConfig(name_adj='date', bq_dtype='STRING'),
	'symbol_fk': ColumnConfig(name_adj='symbol_fk', bq_dtype='STRING'),
	'open': ColumnConfig(name_adj='open', bq_dtype='FLOAT64'),
	'high': ColumnConfig(name_adj='high', bq_dtype='FLOAT64'),
	'low': ColumnConfig(name_adj='low', bq_dtype='FLOAT64') ,
	'close': ColumnConfig(name_adj='close', bq_dtype='FLOAT64'),
	'volume': ColumnConfig(name_adj='volume', bq_dtype='FLOAT64'),
	'dividend': ColumnConfig(name_adj='dividend', bq_dtype='FLOAT64'),
	'stock_split': ColumnConfig(name_adj='stock_split', bq_dtype='FLOAT64'),
	'price': ColumnConfig(name_adj='price', bq_dtype='FLOAT64'),
	'price_all_time_high': ColumnConfig(name_adj='price_all_time_high', bq_dtype='FLOAT64'),
	'drawdown_current': ColumnConfig(name_adj='drawdown_current', bq_dtype='FLOAT64'),
	'drawdown_max': ColumnConfig(name_adj='drawdown_max', bq_dtype='FLOAT64'),
	'volume_of_share': ColumnConfig(name_adj='volume_of_share', bq_dtype='FLOAT64'),
	'volume_of_share_3m_avg': ColumnConfig(name_adj='volume_of_share_3m_avg', bq_dtype='FLOAT64'),
	'volume_of_dollars': ColumnConfig(name_adj='volume_of_dollar', bq_dtype='FLOAT64'),
	'volume_of_dollars_3m_avg': ColumnConfig(name_adj='volume_of_dollar_3m_avg', bq_dtype='FLOAT64'),
	'dividend_paid_or_not': ColumnConfig(name_adj='dividend_paid_or_not', bq_dtype='FLOAT64'),
	'dividend_paid_count_ttm': ColumnConfig(name_adj='dividend_paid_count_ttm', bq_dtype='FLOAT64'),
	'dividend_ttm': ColumnConfig(name_adj='dividend_ttm', bq_dtype='FLOAT64'),
	'dividend_rate': ColumnConfig(name_adj='dividend_rate', bq_dtype='FLOAT64'),
	'dividend_rate_ttm': ColumnConfig(name_adj='dividend_rate_ttm', bq_dtype='FLOAT64'),
	'is_normal_date': ColumnConfig(name_adj='is_normal_date', bq_dtype='FLOAT64'),
	'price_change': ColumnConfig(name_adj='price_change', bq_dtype='FLOAT64'),
	'price_change_rate': ColumnConfig(name_adj='price_change_rate', bq_dtype='FLOAT64'),
	'price_change_sign': ColumnConfig(name_adj='price_change_sign', bq_dtype='FLOAT64'),
	'price_7d_ago': ColumnConfig(name_adj='price_7d_ago', bq_dtype='FLOAT64'),
	'weekly_price_change': ColumnConfig(name_adj='weekly_price_change', bq_dtype='FLOAT64'),
	'weekly_price_change_rate': ColumnConfig(name_adj='weekly_price_change_rate', bq_dtype='FLOAT64'),
	'price_30d_ago': ColumnConfig(name_adj='price_30d_ago', bq_dtype='FLOAT64'),
	'monthly_price_change': ColumnConfig(name_adj='monthly_price_change', bq_dtype='FLOAT64'),
	'monthly_price_change_rate': ColumnConfig(name_adj='monthly_price_change_rate', bq_dtype='FLOAT64'),
}

DATE_DIM = {
	'date_pk': ColumnConfig(name_adj='date_pk', bq_dtype="INT64"),
	'date': ColumnConfig(name_adj='date', bq_dtype="STRING"),
	'year': ColumnConfig(name_adj='year', bq_dtype="INT64"),
	'quarter': ColumnConfig(name_adj='quarter', bq_dtype="INT64"),
	'month': ColumnConfig(name_adj='month', bq_dtype="INT64"),
	'month_name': ColumnConfig(name_adj='month_name', bq_dtype="STRING"),
	'week_of_year': ColumnConfig(name_adj='week_of_year', bq_dtype="INT64"),
	'day_of_year': ColumnConfig(name_adj='day_of_year', bq_dtype="INT64"),
	'day': ColumnConfig(name_adj='day', bq_dtype="INT64"),
	'day_name': ColumnConfig(name_adj='day_name', bq_dtype="STRING"),
	'day_of_week': ColumnConfig(name_adj='day_of_week', bq_dtype="INT64"),
	'is_weekday': ColumnConfig(name_adj='is_weekday', bq_dtype="INT64"),
	'is_month_end': ColumnConfig(name_adj='is_month_end', bq_dtype="INT64"),
	'is_month_start': ColumnConfig(name_adj='is_month_start', bq_dtype="INT64"),
	'is_year_start': ColumnConfig(name_adj='is_year_start', bq_dtype="INT64"),
	'is_year_end': ColumnConfig(name_adj='is_year_end', bq_dtype="INT64"),
	'days_in_month': ColumnConfig(name_adj='days_in_month', bq_dtype="INT64")
}

SUMMARY_GRD = {
	'last_update': ColumnConfig(name_adj='last_update', bq_dtype='STRING'),
	'summary_grade_pk': ColumnConfig(name_adj='summary_grade_pk', bq_dtype='STRING'),
	'symbol_fk': ColumnConfig(name_adj='symbol_fk', bq_dtype='STRING'),
	'unit_period': ColumnConfig(name_adj='unit_period', bq_dtype='STRING'),
	'track_records_d': ColumnConfig(name_adj='track_records_d', bq_dtype='FLOAT64'),
	'track_records_y': ColumnConfig(name_adj='track_records_y', bq_dtype='FLOAT64'),
	'expense_ratio': ColumnConfig(name_adj='expense_ratio', bq_dtype='FLOAT64'),
	'nav': ColumnConfig(name_adj='nav', bq_dtype='FLOAT64'),
	'shares_out': ColumnConfig(name_adj='shares_out', bq_dtype='FLOAT64'),
	'aum': ColumnConfig(name_adj='aum', bq_dtype='FLOAT64'),
	'total_return': ColumnConfig(name_adj='total_return', bq_dtype='FLOAT64'),
	'cagr': ColumnConfig(name_adj='cagr', bq_dtype='FLOAT64'),
	'std_yearly_return': ColumnConfig(name_adj='std_yearly_return', bq_dtype='FLOAT64'),
	'drawdown_max': ColumnConfig(name_adj='drawdown_max', bq_dtype='FLOAT64'),
	'div_ttm': ColumnConfig(name_adj='div_ttm', bq_dtype='FLOAT64'),
	'div_yield_ttm': ColumnConfig(name_adj='div_yield_ttm', bq_dtype='FLOAT64'),
	'div_count_ttm': ColumnConfig(name_adj='div_count_ttm', bq_dtype='FLOAT64'),
	'market_corr_daily': ColumnConfig(name_adj='market_corr_daily', bq_dtype='FLOAT64'),
	'market_corr_weekly': ColumnConfig(name_adj='market_corr_weekly', bq_dtype='FLOAT64'),
	'market_corr_monthly': ColumnConfig(name_adj='market_corr_monthly', bq_dtype='FLOAT64'),
	'market_corr_yearly': ColumnConfig(name_adj='market_corr_yearly', bq_dtype='FlOAT64'),
	'vol_dollar_3m_avg': ColumnConfig(name_adj='vol_dollar_3m_avg', bq_dtype='FLOAT64')
}

SUMMARY_GRD_PIV = {
	'summary_grd_piv_pk': ColumnConfig(name_adj='summary_grd_piv_pk', bq_dtype='STRING'),
	'symbol_fk': ColumnConfig(name_adj='symbol_fk', bq_dtype='STRING'),
	'unit_period': ColumnConfig(name_adj='unit_period', bq_dtype='STRING'),
	'var_name': ColumnConfig(name_adj='var_name', bq_dtype='STRING'),
	'value': ColumnConfig(name_adj='value', bq_dtype='FLOAT64'),
}

SUMMARY_CORR = {
	'symbol_fk': ColumnConfig(name_adj='symbol_fk', bq_dtype='STRING'),
	'target_symbol_fk': ColumnConfig(name_adj='target_symbol_fk', bq_dtype='STRING'),
	'start_date': ColumnConfig(name_adj='start_date', bq_dtype='STRING'),
	'end_date': ColumnConfig(name_adj='end_date', bq_dtype='STRING'),
	'corr_yearly': ColumnConfig(name_adj='corr_yearly', bq_dtype='FLOAT64'),
	'unit_period': ColumnConfig(name_adj='unit_period', bq_dtype='STRING'),
	'summary_corr_pk': ColumnConfig(name_adj='summary_corr_pk', bq_dtype='STRING'),
	'rank_desc': ColumnConfig(name_adj='rank_desc', bq_dtype='FLOAT64'),
	'rank_asc': ColumnConfig(name_adj='rank_asc', bq_dtype='FLOAT64')
}