# 'symbol' = COLUMN(name_adj='symbol', select=True, dtype='str')
class ColumnConfig:
	def __init__(self, name_adj, select):
		self.name_adj = name_adj
		self.select = select
		# self.bq_dtype = dtype <- default auto 
		# self.is_index = is_index

FD_META = {
	'symbol': ColumnConfig(name_adj='symbol', select=True),
	'short_name': ColumnConfig(name_adj='short_name', select=True),
	'long_name': ColumnConfig(name_adj='long_name', select=True),
	'currency': ColumnConfig(name_adj='currency', select=False),
	'summary': ColumnConfig(name_adj='summary', select=True),
	'category': ColumnConfig(name_adj='category', select=True),
	'family': ColumnConfig(name_adj='fund_family', select=True),
	'exchange': ColumnConfig(name_adj='exchange', select=False),
	'market': ColumnConfig(name_adj='market', select=False),
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

# HOLDINGS = {
# 	'maxAge': {'new_name': 'max_age', 'save': False},
# 	'stockPosition': {'new_name': 'stock_position', 'save': True},
# 	'bondPosition': {'new_name': 'bond_position', 'save': True},
# 	'holdings': {'new_name': 'holdings', 'save': True},
# 	'bondRatings': {'new_name': 'bond_ratings', 'save': True},
# 	'sectorWeightings': {'new_name': 'sector_weightings', 'save': True},
# 	'equityHoldings.priceToEarnings': {'new_name': 'price_to_earnings', 'save': False},
# 	'equityHoldings.priceToBook': {'new_name': 'price_to_book', 'save': False}, 
# 	'equityHoldings.priceToSales': {'new_name': 'price_to_sales', 'save': False},
# 	'equityHoldings.priceToCashflow': {'new_name': 'price_to_cashflow', 'save': False},
# 	'bondHoldings.maturity': {'new_name': 'maturity', 'save': True}, # 일부 채권에만 존재
# 	'bondHoldings.duration': {'new_name': 'duration', 'save': True} # 채권에만 존재
# }


HOLDINGS = {

}

METADATA = {

}

# print( list(ETF_META_FD.items())[0] )