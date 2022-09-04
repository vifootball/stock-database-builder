import os
import numpy as np
import pandas as pd
import datetime as dt

from utils import *
from constants import *

class DirectoryBuilder: # DirectoryHelper??
    def __init__(self):
        # 그냥 함수로 만들면 어떨까 했지만 속성을 이용하려면 클래스를 만들어야함...
        self.dirpath_download = DIR_DOWNLOAD
        self.dirpath_profile_etf = os.path.join(self.dirpath_download, SUBDIR_PROFILE_ETF)
        self.dirpath_info_etf = os.path.join(self.dirpath_download, SUBDIR_INFO_ETF)
        self.dirpath_master = os.path.join(self.dirpath_download, SUBDIR_MASTER)
        self.dirpath_master_indices = os.path.join(self.dirpath_download, SUBDIR_MASTER_INDICES)
        self.dirpath_history_raw_etf = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_ETF)
        self.dirpath_history_raw_indices = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_INDICES)
        self.dirpath_history_raw_currencies = os.path.join(self.dirpath_download, SUBDIR_HISTORY_RAW_CURRENCIES)
        self.dirpath_history_pp_etf = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_ETF)
        self.dirpath_history_pp_indices = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_INDICES)
        self.dirpath_history_pp_currencies = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_CURRENCIES)
        self.dirpath_history_pp_concatenated = os.path.join(self.dirpath_download, SUBDIR_HISTORY_PP_CONCATENATED)
        self.dirpath_recent = os.path.join(self.dirpath_download, SUBDIR_RECENT)
        self.dirpath_summary = os.path.join(self.dirpath_download, SUBDIR_SUMMARY)

        os.makedirs(self.dirpath_download, exist_ok=True)
        os.makedirs(self.dirpath_profile_etf, exist_ok=True)
        os.makedirs(self.dirpath_info_etf, exist_ok=True)
        os.makedirs(self.dirpath_master, exist_ok=True)
        os.makedirs(self.dirpath_master_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_etf, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_raw_currencies, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_etf, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_indices, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_currencies, exist_ok=True)
        os.makedirs(self.dirpath_history_pp_concatenated, exist_ok=True)
        os.makedirs(self.dirpath_recent, exist_ok=True)
        os.makedirs(self.dirpath_summary, exist_ok=True)

        self.fpath_meta_etf = os.path.join(self.dirpath_download, FNAME_META_ETF)
        self.fpath_info_etf = os.path.join(self.dirpath_download, FNAME_INFO_ETF)
        self.fpath_profile_etf = os.path.join(self.dirpath_download, FNAME_PROFILE_ETF)
        self.fpath_master_etf = os.path.join(self.dirpath_master, FNAME_MASTER_ETF)
        self.fpath_master_currencies = os.path.join(self.dirpath_master, FNAME_MASTER_CURRENCIES)
        self.fpath_master_indices = os.path.join(self.dirpath_master, FNAME_MASTER_INDICES)
        self.fpath_master_indices_yahoo = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_YAHOO)
        self.fpath_master_indices_investpy = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_INVESTPY)
        self.fpath_master_indices_fred = os.path.join(self.dirpath_master_indices, FNAME_MASTER_INDICES_FRED)
        self.fpath_history_pp_etf = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_ETF)
        self.fpath_history_pp_indices = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_INDICES)
        self.fpath_history_pp_currencies = os.path.join(self.dirpath_history_pp_concatenated, FNAME_HISTORY_PP_CURRENCIES)
        self.fpath_recent_etf = os.path.join(self.dirpath_recent, FNAME_RECENT_ETF)
        self.fpath_recent_indices = os.path.join(self.dirpath_recent, FNAME_RECENT_INDICES)
        self.fpath_recent_currencies = os.path.join(self.dirpath_recent, FNAME_RECENT_CURRENCIES)
        self.fpath_summary_etf = os.path.join(self.dirpath_summary, FNAME_SUMMARY_ETF)
        self.fpath_summary_indices = os.path.join(self.dirpath_summary, FNAME_SUMMARY_INDICES)
        self.fpath_summary_currencies = os.path.join(self.dirpath_summary, FNAME_SUMMARY_CURRENCIES)
    
    def get_path_dict_by_category(self, category):
        assert category in ['etf', 'index', 'currency'], 'category must be one of ["etf", "index", "currency"]'

        if category == "index":
            path_dict = {
                'fpath_master': self.fpath_master_indices,
                'dirpath_history_raw': self.dirpath_history_raw_indices,
                'dirpath_history_pp': self.dirpath_history_pp_indices,
                'fpath_recent': self.fpath_recent_indices,
                'fpath_summary': self.fpath_summary_indices
            }
        elif category == "etf":
            path_dict = {
                'fpath_master': self.fpath_master_etf,
                'dirpath_history_raw': self.dirpath_history_raw_etf,
                'dirpath_history_pp': self.dirpath_history_pp_etf,
                'fpath_recent': self.fpath_recent_etf,
                'fpath_summary': self.fpath_summary_etf
            }
        elif category == "currency":
            path_dict = {
                'fpath_master': self.fpath_master_currencies,
                'dirpath_history_raw': self.dirpath_history_raw_currencies,
                'dirpath_history_pp': self.dirpath_history_pp_currencies,
                'fpath_recent': self.fpath_recent_currencies,
                'fpath_summary': self.fpath_summary_currencies
            }

        return path_dict
    