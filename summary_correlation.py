import os
import pandas as pd
from utils import *
from tqdm import tqdm
import time
from table import TableHandler
import table_config

def get_corr(history, target_history):
    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
    num_of_years = (end_date - start_date).days / 365.25

    target_start_date = target_history.loc[target_history['date'] == target_history['date'].min(), 'date'].squeeze()
    target_end_date = target_history.loc[target_history['date'] == target_history['date'].max(), 'date'].squeeze()
    target_num_of_years = (target_end_date - target_start_date).days / 365.25

    if num_of_years <= target_num_of_years: # 나보다 길면 비교
        corr_row = {}
        corr_row['symbol_fk'] = history['symbol_fk'][0]
        corr_row['target_symbol_fk'] = target_history['symbol_fk'][0]
        corr_row['start_date'] = start_date.strftime('%Y-%m-%d')
        corr_row['end_date'] = end_date.strftime('%Y-%m-%d')
        corr_row['corr_yearly'] = history.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
        corr_row = pd.DataFrame([corr_row])
        # display(corr_row)
        return corr_row
    else:
        return None

local_dirpath = "./download/history/etf/"
symbols = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv')]
symbols = sorted(symbols)[730:750]


for symbol in tqdm(symbols):
    time.sleep(2)
    # symbol = 'QQQ'
    # load
    history = pd.read_csv(f'./download/history/etf/history_{symbol}.csv')
    history['date'] = pd.to_datetime(history['date'])

    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
    num_of_years = (end_date - start_date).days / 365.25

    ### get corr all time
    corr_df = []
    target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
    for target_symbol in tqdm(target_symbols, mininterval=0.5):
        target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
        target_history['date'] = pd.to_datetime(target_history['date'])

        corr = get_corr(history, target_history)
        corr_df.append(corr)
    corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
    corr_df['unit_period'] = 'all_time'
    corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
    corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
    corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
    print(corr_df)
    os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
    corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_all_{symbol}.csv', index=False)

    ### get corr recent 15 year    
    if num_of_years > 15:
        fifteen_year_ago = end_date - pd.DateOffset(years=15)
        corr_df = []
        target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
        for target_symbol in tqdm(target_symbols, mininterval=0.5):
            target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
            target_history['date'] = pd.to_datetime(target_history['date'])

            corr = get_corr(history.loc[history['date'] > fifteen_year_ago].reset_index(drop=True), target_history)
            corr_df.append(corr)
        corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
        corr_df['unit_period'] = 'recent_15_year'
        corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
        corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
        corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
        os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
        corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_15y_{symbol}.csv', index=False)
        print(corr_df)
    
    ### get corr recent 10 year    
    if num_of_years > 10:
        ten_year_ago = end_date - pd.DateOffset(years=10)
        corr_df = []
        target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
        for target_symbol in tqdm(target_symbols, mininterval=0.5):
            target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
            target_history['date'] = pd.to_datetime(target_history['date'])

            corr = get_corr(history.loc[history['date'] > ten_year_ago].reset_index(drop=True), target_history)
            corr_df.append(corr)
        corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
        corr_df['unit_period'] = 'recent_10_year'
        corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
        corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
        corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
        os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
        corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_10y_{symbol}.csv', index=False)
        print(corr_df)

    ### get corr recent 5 year    
    if num_of_years > 5:
        five_year_ago = end_date - pd.DateOffset(years=5)
        corr_df = []
        target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
        for target_symbol in tqdm(target_symbols, mininterval=0.5):
            target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
            target_history['date'] = pd.to_datetime(target_history['date'])

            corr = get_corr(history.loc[history['date'] > five_year_ago].reset_index(drop=True), target_history)
            corr_df.append(corr)
        corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
        corr_df['unit_period'] = 'recent_5_year'
        corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
        corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
        corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
        os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
        corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_5y_{symbol}.csv', index=False)
        print(corr_df)
    


# dirpath_corr_etf = './download/summary_corr/etf/'
# dirpath_corr_chunk = './download/summary_corr/chunk/'
# save_dfs_by_chunk(dirpath_corr_etf, dirpath_corr_chunk, prefix_chunk="concatenated_summary_corr")
