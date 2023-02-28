import os

# bigquery
BQ_PROJECT_ID = 'between-buy-and-sell'
BQ_DATASET_ID = 'stock'
BQ_TABLE_ID_SUMMARY = 'summary'
BQ_TABLE_ID_HISTORY = 'history'

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