import os

# bigquery
BQ_PROJECT_ID = 'between-buy-and-sell'
BQ_DATASET_ID = 'stock'

BQ_TABLE_ID_DIM_DATE = 'dim_date'
BQ_TABLE_ID_DIM_ETF = 'dim_etf'
BQ_TABLE_ID_FACT_ETF = 'fact_etf'

# directories
DIR_BASE = os.path.dirname(os.path.abspath(__file__))
DIR_DOWNLOAD = os.path.join(DIR_BASE, 'download')

DIR_METADATA_ETF = os.path.join(DIR_DOWNLOAD, 'metadata', 'etf')
DIR_METADATA_CHUNK = os.path.join(DIR_DOWNLOAD, 'metadata', 'chunk')

DIR_HISTORY_ETF = os.path.join(DIR_DOWNLOAD, 'history', 'etf')
DIR_HISTORY_CURRENCY = os.path.join(DIR_DOWNLOAD, 'history', 'currency')
DIR_HISTORY_INDICES = os.path.join(DIR_DOWNLOAD, 'history', 'indices')
DIR_HISTORY_INDICES_FRED = os.path.join(DIR_DOWNLOAD, 'history', 'indices_fred')
DIR_HISTORY_CHUNK = os.path.join(DIR_DOWNLOAD, 'history', 'chunk')

DIR_DATEDIM = os.path.join(DIR_DOWNLOAD, 'date_dim')

# DIR_SUMMARY