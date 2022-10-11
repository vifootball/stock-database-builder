import os
import numpy as np
import pandas as pd
# from tqdm.notebook import tqdm
from tqdm import tqdm
from datetime import datetime

def export_df_to_csv(df, fpath): # None 처리 # makedirs를 해주기 위해 만듬
    if isinstance(df, pd.DataFrame) and (len(df) > 0):
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        df.to_csv(fpath, index=False, encoding='utf-8-sig')
    else:
        print("Export Nothing: arg 'df' is empty or not a pd.DataFrame")

def concat_csv_files_in_dir(get_dirpath):
    df = []
    csv_file_generator = (pd.read_csv(os.path.join(get_dirpath, csv_fname)) for csv_fname in os.listdir(get_dirpath) if csv_fname.endswith('csv'))
    for csv_file in csv_file_generator:
        df.append(csv_file)
    df = pd.concat(df)
    return df

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        str_start_time = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
        print(f"Process [{func.__name__}] Started at : {str_start_time}")

        result = func(*args, **kwargs)
        
        end_time = datetime.now()
        str_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
        interval = end_time - start_time
        str_interval = str(np.round(interval.total_seconds(), 3))
        print(f"Process [{func.__name__}] Ended at   : {str_end_time}")
        print(f"Process [{func.__name__}] Runned for : {str_interval} sec")
        print()

        return result
    return wrapper

def get_memory_usage(df):
    # compare with df.info(memory_usage='deep')
    if isinstance(df, pd.DataFrame):
        mem = df.memory_usage(deep=True).sum()/(1024**2)
    elif isinstance(df, pd.Series):
        mem = df.memory_usage(deep=True)/(1024**2)
    return(str(round(mem,3))+" MB" )


def downcast_df(df, apply_int=True, apply_float=True, apply_string=False, print_size=True):
    before = get_memory_usage(df)
    df_copied = df.copy(deep=True)
    if apply_int:
        df_int = df_copied.select_dtypes(include=['int']).apply(pd.to_numeric, downcast='integer')
        df_copied.loc[:,df_int.columns] = df_int
    if apply_float:
        df_float = df_copied.select_dtypes(include=['float']).apply(pd.to_numeric, downcast='float')
        df_copied.loc[:,df_float.columns] = df_float
    if apply_string:
        df_string = df_copied.select_dtypes(include=['object']).astype('category')
        df_copied.loc[:,df_string.columns] = df_string
    after = get_memory_usage(df_copied)
    if print_size:
        print(f"DataFrame Downcasted: {before} -> {after}")
    return df_copied