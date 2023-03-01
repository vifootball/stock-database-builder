import os
import pandas as pd
from tqdm import tqdm
from utils import *
import config
import table_config
from indices import Indices
from currency import Currency
from etf import ETF
from history import History
from date_dim import DateDim
from bigquery_helper import BigQuery

def etl_metadata():
    _etl_metadata_etf()
    _etl_metadata_currency()
    _etl_metadata_indices()

    bq = BigQuery()    
    bq.copy_local_csv_files_to_bq_table(
        local_dirpath=config.DIR_METADATA_CHUNK,
        bq_table_id=config.BQ_TABLE_ID_DIM_ETF,
        table_config=table_config.METADATA
    )


def _etl_metadata_etf():
    etf = ETF()
    symbols = etf.get_symbols()

    dirpath_metadata = config.DIR_METADATA_ETF
    os.makedirs(dirpath_metadata, exist_ok=True)

    for symbol in tqdm(symbols[:], mininterval=0.5):
        metadata = etf.get_metadata(symbol=symbol)
        fname = os.path.join(dirpath_metadata, f"metadata_etf_{symbol}.csv")
        fpath = os.path.join(dirpath_metadata, fname)
        metadata.to_csv(fpath, index=False)
    
    dirpath_metadata_chunk = config.DIR_METADATA_CHUNK
    os.makedirs(dirpath_metadata_chunk, exist_ok=True)
    fname = 'metadata_etf_chunk.csv'
    fpath = os.path.join(dirpath_metadata_chunk, fname)
    metadata_chunk = concat_csv_files_in_dir(get_dirpath = config.DIR_METADATA_ETF)
    metadata_chunk.to_csv(fpath, index=False)
    

def _etl_metadata_indices():
    indices = Indices()
    metadata_from_fd = indices.get_metadata_from_fd()
    metadata_from_investpy = indices.get_metadata_from_investpy()
    metadata_from_yahoo_main = indices.get_metadata_from_yahoo_main()
    metadata_from_fred = indices.get_metadata_from_fred()

    metadata = pd.concat([
        metadata_from_fd, metadata_from_investpy, metadata_from_yahoo_main, metadata_from_fred
    ], axis=0).drop_duplicates(subset='symbol_pk', keep='first') # 40개 가량 중복

    dirpath = config.DIR_METADATA_CHUNK
    os.makedirs(dirpath, exist_ok=True)
    fname = 'metdata_indices.csv'
    fpath = os.path.join(dirpath, fname)
    metadata.to_csv(fpath, index=False)
    

def _etl_metadata_currency():
    currency = Currency()
    metadata = currency.get_metadata()

    dirpath = config.DIR_METADATA_CHUNK
    os.makedirs(dirpath, exist_ok=True)
    fname = 'metadata_currency.csv'
    fpath = os.path.join(dirpath, fname)
    metadata.to_csv(fpath, index=False)


def etl_datedim():
    # extract
    datedim = DateDim()
    df = datedim.get_date_dim()

    # stage
    dirpath = config.DIR_DATEDIM
    os.makedirs(dirpath, exist_ok=True)
    fname = 'date_dim.csv'
    fpath = os.path.join(dirpath, fname)
    df.to_csv(fpath, index=False)

    # load
    bq = BigQuery()
    bq.copy_local_csv_files_to_bq_table(
        local_dirpath=config.DIR_DATEDIM,
        bq_table_id=config.BQ_TABLE_ID_DIM_DATE,
        table_config=table_config.DATE_DIM
    )


def etl_history():
    etf = ETF()
    indices = Indices()
    currency = Currency()
    history = History()

    etf_symbols = etf.get_symbols()
    currency_symbols = currency.get_symbols()
    indices_symbols_from_yahoo_main = indices.get_symbols_from_yahoo_main()
    indices_symbols_from_fd = indices.get_symbols_from_fd()
    indices_symbols_from_investpy = indices.get_symbols_from_investpy()
    indices_symbols_yf = indices_symbols_from_yahoo_main + indices_symbols_from_fd + indices_symbols_from_investpy
    indices_symbols_fred = indices.get_symbols_from_fred()
    
    dirpath_history_etf = config.DIR_HISTORY_ETF
    dirpath_history_currency = config.DIR_HISTORY_CURRENCY
    dirpath_history_indices = config.DIR_HISTORY_INDICES
    os.makedirs(dirpath_history_etf, exist_ok=True)
    os.makedirs(dirpath_history_currency, exist_ok=True)
    os.makedirs(dirpath_history_indices, exist_ok=True)
    
    for symbol in tqdm(etf_symbols[1900:], mininterval=0.5):
        print(symbol)
        df = history.transform_history(history.get_history_from_yf(symbol=symbol))
        if df is not None:
            fname = f'history_{symbol}.csv'
            fpath = os.path.join(dirpath_history_etf, fname)
            df.to_csv(fpath, index=False)
    
    for symbol in tqdm(currency_symbols, mininterval=0.5):
        df = history.transform_history(history.get_history_from_yf(symbol=symbol))
        if df is not None:
            fname = f'history_{symbol}.csv'
            fpath = os.path.join(dirpath_history_currency, fname)
            df.to_csv(fpath, index=False)
    
    for symbol in tqdm(indices_symbols_yf, mininterval=0.5):
        df = history.transform_history(history.get_history_from_yf(symbol=symbol))
        if df is not None:
            fname = f'history_{symbol}.csv'
            fpath = os.path.join(dirpath_history_indices, fname)
            df.to_csv(fpath, index=False)
    
    for symbol in tqdm(indices_symbols_fred, mininterval=0.5):
        df = history.transform_history(history.get_history_from_fred(symbol=symbol))
        if df is not None:
            fname = f'history_{symbol}.csv'
            fpath = os.path.join(dirpath_history_indices, fname)
            df.to_csv(fpath, index=False)
    
    # save df by chunk
    dirpath_history_chunk = config.DIR_HISTORY_CHUNK
    save_dfs_by_chunk(dirpath_history_etf, dirpath_history_chunk, prefix_chunk="concatenated_history_etf")
    save_dfs_by_chunk(dirpath_history_currency, dirpath_history_chunk, prefix_chunk="concatenated_history_currency")
    save_dfs_by_chunk(dirpath_history_indices, dirpath_history_chunk, prefix_chunk="concatenated_history_indices")

    # load
    bq = BigQuery()
    bq.copy_local_csv_files_to_bq_table(
        local_dirpath=config.DIR_HISTORY_CHUNK,
        bq_table_id=config.BQ_TABLE_ID_FACT_ETF,
        table_config=table_config.TRG_HISTORY
    )


if __name__ == '__main__':
    # etl_metadata_etf()
    # etl_metadata_currency()
    # etl_metadata_indices()
    # etl_datedim()
    # etl_history()
    pass