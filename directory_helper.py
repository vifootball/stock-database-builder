import os
from constants import *

class DirectoryHelper:

    @staticmethod
    def get_path_dict(category=None):
        path_dict_common = {
            'dirpath_download': DIR_DOWNLOAD,
            'dirpath_master': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER),
            'dirpath_history_pp_concatenated': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_CONCATENATED),
            'dirpath_recent': os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT),
            'dirpath_summary': os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY)
        }

        if category == "etf":
            path_dict_by_category = {
                'dirpath_history_raw': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_RAW_ETF),
                'dirpath_history_pp': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_ETF),          

                'fpath_master_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_MASTER_ETF),
                'fpath_history_pp_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_CONCATENATED, FNAME_HISTORY_PP_ETF),
                'fpath_recent_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_RECENT_ETF),
                'fpath_summary_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_SUMMARY_ETF),

                'dirpath_profile_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_PROFILE_ETF),
                'dirpath_info_etf': os.path.join(DIR_DOWNLOAD, SUBDIR_INFO_ETF),

                'fpath_meta_etf': os.path.join(DIR_DOWNLOAD, FNAME_META_ETF),
                'fpath_info_etf': os.path.join(DIR_DOWNLOAD, FNAME_INFO_ETF),
                'fpath_profile_etf': os.path.join(DIR_DOWNLOAD, FNAME_PROFILE_ETF)
            }
        elif category == "index":
            path_dict_by_category = {
                'dirpath_history_raw': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_RAW_INDICES),
                'dirpath_history_pp': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_INDICES),
                
                'fpath_master': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_MASTER_INDICES),
                'fpath_history_pp': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_CONCATENATED, FNAME_HISTORY_PP_INDICES),
                'fpath_recent': os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_RECENT_INDICES),
                'fpath_summary': os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_SUMMARY_INDICES),

                'dirpath_master_indices': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER_INDICES),
                'fpath_master_indices_yahoo': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES_YAHOO),
                'fpath_master_indices_investpy': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES_INVESTPY),
                'fpath_master_indices_fred': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER_INDICES, FNAME_MASTER_INDICES_FRED)
            }
        elif category == "currency":
            path_dict_by_category = {
                'dirpath_history_raw': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_RAW_CURRENCIES),
                'dirpath_history_pp':  os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_CURRENCIES),
                                
                'fpath_master': os.path.join(DIR_DOWNLOAD, SUBDIR_MASTER, FNAME_MASTER_CURRENCIES),
                'fpath_history_pp': os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_PP_CONCATENATED, FNAME_HISTORY_PP_CURRENCIES),
                'fpath_recent': os.path.join(DIR_DOWNLOAD, SUBDIR_RECENT, FNAME_RECENT_CURRENCIES),
                'fpath_summary': os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY, FNAME_SUMMARY_CURRENCIES)
            }
        elif category == None:
            path_dict_by_category = {}

        path_dict = {**path_dict_common, **path_dict_by_category}
        return path_dict
        

    def build_directoreis(self):
        pass