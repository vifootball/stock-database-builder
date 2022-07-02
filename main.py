import os
import time
import datetime as dt
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

import investpy
import yfinance as yf
import pandas_datareader
import pandas_datareader.data as web

import etl_processor
from utils import *
from constants import *

pd.options.mode.chained_assignment = None



if __name__ == '__main__':
    print('hi')

    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    processor = etl_processor.EtlProcessor()

    ### ETF ###
    processor.get_meta_etf()
    processor.get_info_etf()
    processor.get_profile_etf()
    concat_csv_files_in_dir( # 그냥 한줄로 만들기: concat_info_etf
        get_dir=os.path.join(processor.dir_download, processor.subdir_profile_etf),
        put_dir=processor.dir_download,
        fname='profile_etf.csv'
    )
    concat_csv_files_in_dir( # concat_profile_etf
        get_dir=os.path.join(processor.dir_download, processor.subdir_info_etf),
        put_dir=processor.dir_download,
        fname='info_etf.csv'
    )
    
    processor.construct_master_etf()

    ### Indices ###
    processor.get_master_indices_investpy()
    #processor.get_master_indices_fred()

    ### Currencies ###
    #processor.get_master_currencies()
    
    ### Get Hisotries ###
    #master_etf = ''
    #master_currencies = ''
    #master_indices_yahoo = ''
    #master_indices_investpy = ''
    #master_indices_fred = ''
    #processor.get_history_from_yf(master_etf, category='etf')
    #processor.get_history_from_yf(master_currencies, category='currency')
    #processor.get_history_from_yf(master_indices_yahoo, category='index')
    #processor.get_history_from_yf(master_indices_investpy, category='index')
    #processor.get_history_from_fred(master_indices_fred)

    ### Preprocess Hisotries ###
    # processor.preprocess_history(category='etf')
    # processor.preprocess_history(category='currency')
    # processor.preprocess_history(category='index')

    ### Get Recent Data from Hisotires ###
    processor.get_recent_from_history(category='etf')
    processor.get_recent_from_history(category='currency')
    processor.get_recent_from_history(category='index')


    # processor.construct_summary_table()

    print('bye')