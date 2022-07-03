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
    # processor.get_meta_etf()
    # processor.get_info_etf()
    # processor.get_profile_etf()
    # processor.concat_info_etf()                              # complete
    # processor.concat_profile_etf()                           # complete
    # processor.construct_master_etf()                         # complete

    ### Indices ###
    # processor.get_master_indices_investpy()
    # processor.get_master_indices_yahoo()
    # processor.get_master_indices_fred()
    # processor.concat_master_indices()                        # complete



    ### Currencies ###
    #processor.get_master_currencies()
    
    ### Get Hisotries ###
    # master_etf = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_etf))
    # master_currencies = pd.read_csv(os.path.join(processor.dir_download, processor.fname_master_currencies))
    # master_indices_yahoo = pd.read_csv(os.path.join(processor.dir_download, processor.subdir_master_indices, processor.fname_master_indices_yahoo))
    # master_indices_investpy = pd.read_csv(os.path.join(processor.dir_download, processor.subdir_master_indices, processor.fname_master_indices_investpy))
    # master_indices_fred = pd.read_csv(os.path.join(processor.dir_download, processor.subdir_master_indices, processor.fname_master_indices_fred))
    #processor.get_history_from_yf(master_etf, category='etf')
    #processor.get_history_from_yf(master_currencies, category='currency')
    #processor.get_history_from_yf(master_indices_yahoo, category='index')
    #processor.get_history_from_yf(master_indices_investpy, category='index')
    #processor.get_history_from_fred(master_indices_fred)

    ### Preprocess Hisotries ###
    # processor.preprocess_history(category='etf')             # complete
    # processor.preprocess_history(category='currency')        # complete
    # processor.preprocess_history(category='index')           # complete

    ### Get Recent Data from Hisotires ###
    processor.get_recent_from_history(category='etf')        # complete
    processor.get_recent_from_history(category='currency')   # complete
    processor.get_recent_from_history(category='index')      # complete

    # processor.concat_history(category='etf')                 # complete
    # processor.concat_history(category='index')               # complete
    # processor.concat_history(category='currency')            # complete

    # 히스토리 다시 구하고 df 형태 학인하기  suffix
    # processor.construct_summary(category='etf')              
    # processor.construct_summary(category='index')            
    # processor.construct_summary(category='currency')          

    print('bye')