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

    ### Get Master of ETF ###
    # processor.get_meta_etf()                                 # complete
    # processor.get_info_etf()                                 # complete
    # processor.get_profile_etf()                              # complete
    
    # processor.concat_info_etf()                              # complete
    # processor.concat_profile_etf()                           # complete
    # processor.construct_master_etf()                         # complete

    ### Get Master of Currencies ###
    # processor.get_master_currencies()                        # complete  

    ### Get Master of Indices ###
    # processor.get_master_indices_investpy()                  # complete
    # processor.get_master_indices_yahoo()                     # complete
    # processor.get_master_indices_fred()                      # complete
    # processor.concat_master_indices()                        # complete

                     
    
    ### Get Histories ###
    # processor.get_history_from_yf(category='etf')            # complete
    # processor.get_history_from_yf(category='currency')       # complete
    # processor.get_history_from_yf(category='index')          # complete

    ### Preprocess Histories ###
    # processor.preprocess_history(category='etf')             # complete
    # processor.preprocess_history(category='index')           # complete
    # processor.preprocess_history(category='currency')        # complete

    # processor.concat_history(category='etf')                 # complete
    # processor.concat_history(category='index')               # complete
    # processor.concat_history(category='currency'  )          # complete

    ### Get Recent Data from Hisotires ###
    # processor.get_recent_from_history(category='etf')        # complete
    # processor.get_recent_from_history(category='index')      # complete
    # processor.get_recent_from_history(category='currency')   # complete

    # processor.construct_summary(category='etf')              # complete    
    # processor.construct_summary(category='index')            # complete
    # processor.construct_summary(category='currency')         # complete

    # processor.load_summary_to_bq()
    processor.load_history_to_bq()

    print('bye')