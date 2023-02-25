# 'symbol' = COLUMN(name_adj='symbol', select=True, dtype='str')
class ColumnConfig:
	def __init__(self, name_adj, select=True):
		self.name_adj = name_adj
		self.select = select
		# self.bq_dtype = dtype <- default auto 
		# self.is_index = is_index

SRC_FD_META = {
	'symbol': ColumnConfig(name_adj='symbol', select=True),
	'short_name': ColumnConfig(name_adj='short_name', select=True),
	'long_name': ColumnConfig(name_adj='name', select=True),
	'currency': ColumnConfig(name_adj='currency', select=False),
	'summary': ColumnConfig(name_adj='summary', select=True),
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
	'summary': ColumnConfig(name_adj='summary', select=True),
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
	'Inception Date': ColumnConfig(name_adj='inception_date', select=False),
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

METADATA = {
	# FD_META
	'symbol': ColumnConfig(name_adj='symbol'),
	'short_name': ColumnConfig(name_adj='short_name'),
	'name': ColumnConfig(name_adj='name'),
	'summary': ColumnConfig(name_adj='summary'),
	'asset_subcategory': ColumnConfig(name_adj='asset_subcategory'),
	'asset_category': ColumnConfig(name_adj='asset_category'),
	'asset_subcategory': ColumnConfig(name_adj='asset_subcategory'),
	'fund_family': ColumnConfig(name_adj='fund_family'),
	'total_assets': ColumnConfig(name_adj='total_assets'),
	# PROFILE
	'net_assets': ColumnConfig(name_adj='net_assets'),
	'nav': ColumnConfig(name_adj='nav', select=False),
	'yield': ColumnConfig(name_adj='yield', select=False),
	'expense_ratio': ColumnConfig(name_adj='expense_ratio'),
	# AUM
	'aum': ColumnConfig(name_adj='aum'),
	'shares_out': ColumnConfig(name_adj='shares_out'),
	# HOLDINGS
	'stock_position': ColumnConfig(name_adj='stock_position'),
	'bond_position': ColumnConfig(name_adj='bond_position'),
	'holdings': ColumnConfig(name_adj='holdings'),
	'bond_ratings': ColumnConfig(name_adj='bond_ratings'),
	'sector_weightings': ColumnConfig(name_adj='sector_weightings'),
	'maturity': ColumnConfig(name_adj='maturity'), # 일부 채권에만 존재
	'duration': ColumnConfig(name_adj='duration') # 채권에만 존재
}

METADATA_COMMON = {
	'symbol': ColumnConfig(name_adj='symbol'),
	'name': ColumnConfig(name_adj='name'),
	'short_name': ColumnConfig(name_adj='short_name'),
	'category': ColumnConfig(name_adj='category')
}

CURRENCY = {
	'symbol': ColumnConfig(name_adj='symbol', select=True),
	'name': ColumnConfig(name_adj='short_name', select=True),
	'full_name': ColumnConfig(name_adj='name', select=True),
	'base': ColumnConfig(name_adj='base', select=False),
	'category': ColumnConfig(name_adj='category', select=True),
	'base_name': ColumnConfig(name_adj='base_name', select=False),
	'second': ColumnConfig(name_adj='second', select=False),
	'second_name': ColumnConfig(name_adj='second_name', select=False)
}

