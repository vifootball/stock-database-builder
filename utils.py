import os
import numpy as np
import pandas as pd
# from tqdm.notebook import tqdm
from tqdm import tqdm
from datetime import datetime

def concat_csv_files_in_dir(get_dirpath):
    df = []
    csv_file_generator = (pd.read_csv(os.path.join(get_dirpath, csv_fname)) for csv_fname in os.listdir(get_dirpath) if csv_fname.endswith('csv'))
    for csv_file in tqdm(csv_file_generator, mininterval=0.5, total=len(os.listdir(get_dirpath))):
        df.append(csv_file)
    df = pd.concat(df, ignore_index=True)
    return df


def edit_csv_files_in_dir(get_dirpath):
    # 임시활용
    
    fpath_generator = (os.path.join(get_dirpath, csv_fname) for csv_fname in os.listdir(get_dirpath) if csv_fname.endswith('csv'))
    for csv_fpath in tqdm(fpath_generator, mininterval=0.5, total=len(os.listdir(get_dirpath))):
        csv_file = pd.read_csv(csv_fpath)
        
        # Define Edit Task 
        # csv_file = csv_file.rename(columns={'aum_date': 'sa_1_date'})

        csv_file.to_csv(csv_fpath, index=False)


def save_dfs_by_chunk(get_dirpath, put_dirpath, prefix_chunk): # n행씩 분할저장.. # 마지막거는 어케하지..?
    os.makedirs(put_dirpath, exist_ok=True)
    df_list = []
    total_len = 0
    chunk_num = 1

    df_generator = (fname for fname in os.listdir(get_dirpath) if fname.endswith('csv'))
    for fname in df_generator:
        print(f'Open: {fname}') # 에러 디버그용: pandas.errors.ParserError: Error tokenizing data. C error: Expected 37 fields in line 5398, saw 46
        df = pd.read_csv(os.path.join(get_dirpath, fname))
        total_len += len(df)
        print(f'Concatenating Dfs in {get_dirpath} | Chunk No.{chunk_num} | Total Length: {total_len: <8} | File Name: {fname}')
        df_list.append(df)
        
        if total_len > 1000_000:
            df_concatenated = pd.concat(df_list)
            fpath = os.path.join(put_dirpath, f'{prefix_chunk}_{chunk_num}.csv')
            df_concatenated.to_csv(fpath, index=False)

            df_list = []
            total_len = 0
            chunk_num += 1
    
    if total_len > 0: # 나눠 떨어지지 않은 마지막 사이클 저장
        df_concatenated = pd.concat(df_list)
        fpath = os.path.join(put_dirpath, f'{prefix_chunk}_{chunk_num}.csv')
        df_concatenated.to_csv(fpath, index=False)


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