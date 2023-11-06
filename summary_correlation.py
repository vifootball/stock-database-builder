import os
import pandas as pd
from utils import *
from tqdm import tqdm
import ray
import time
from datetime import datetime
# from table import TableHandler
# import table_config

def get_corr(history, target_history):

    history['date'] = pd.to_datetime(history['date'])
    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()    # 날짜 최소
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()      # 날짜 최대
    total_years = (end_date - start_date).days / 365.25                                     # 해당 ETF의 총 기간

    target_history['date'] = pd.to_datetime(target_history['date'])
    target_start_date = target_history.loc[target_history['date'] == target_history['date'].min(), 'date'].squeeze()    # 날짜 최소
    target_end_date = target_history.loc[target_history['date'] == target_history['date'].max(), 'date'].squeeze()      # 날짜 최대
    target_total_years = (target_end_date - target_start_date).days / 365.25                                            # 해당 ETF의 총 기간

    fifteen_year_ago = end_date - pd.DateOffset(years=15)
    ten_year_ago = end_date - pd.DateOffset(years=10)
    five_year_ago = end_date - pd.DateOffset(years=5)


    corr_data_list = []
    # All time
    if total_years <= target_total_years:
        corr_data = {}
        corr_data['unit_period'] = "all_time"
        corr_data['symbol'] = history['symbol'][0]
        corr_data['target_symbol'] = target_history['symbol'][0]
        corr_data['start_date'] = start_date
        corr_data['end_date'] = end_date
        corr_data['corr_yearly'] = history.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
        corr_data['corr_monthly'] = history.set_index('date')['price'].resample('M').last().pct_change().corr(target_history.set_index('date')['price'].resample('M').last().pct_change())
        corr_data['corr_weekly'] = history.set_index('date')['price'].resample('W').last().pct_change().corr(target_history.set_index('date')['price'].resample('W').last().pct_change())
        corr_data['corr_daily'] = history.set_index('date')['price'].resample('D').last().pct_change().corr(target_history.set_index('date')['price'].resample('D').last().pct_change())
        corr_data = pd.DataFrame([corr_data])
        corr_data_list.append(corr_data)

    # Recent 15 Year
    if total_years >= 15 and target_total_years >= 15:
        corr_data = {}
        corr_data['unit_period'] = "recent_15_year"
        corr_data['symbol'] = history['symbol'][0]
        corr_data['target_symbol'] = target_history['symbol'][0]
        corr_data['start_date'] = fifteen_year_ago
        corr_data['end_date'] = end_date
        history_fifteen_year_ago = history.loc[history['date'] > fifteen_year_ago]
        corr_data['corr_yearly'] = history_fifteen_year_ago.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
        corr_data['corr_monthly'] = history_fifteen_year_ago.set_index('date')['price'].resample('M').last().pct_change().corr(target_history.set_index('date')['price'].resample('M').last().pct_change())
        corr_data['corr_weekly'] = history_fifteen_year_ago.set_index('date')['price'].resample('W').last().pct_change().corr(target_history.set_index('date')['price'].resample('W').last().pct_change())
        corr_data['corr_daily'] = history_fifteen_year_ago.set_index('date')['price'].resample('D').last().pct_change().corr(target_history.set_index('date')['price'].resample('D').last().pct_change())
        corr_data = pd.DataFrame([corr_data])
        corr_data_list.append(corr_data)

    # Recent 10 Year
    if total_years >= 10 and target_total_years >= 10:
        corr_data = {}
        corr_data['unit_period'] = "recent_10_year"
        corr_data['symbol'] = history['symbol'][0]
        corr_data['target_symbol'] = target_history['symbol'][0]
        corr_data['start_date'] = ten_year_ago
        corr_data['end_date'] = end_date
        history_ten_year_age = history.loc[history['date'] > ten_year_ago]
        corr_data['corr_yearly'] = history_ten_year_age.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
        corr_data['corr_monthly'] = history_ten_year_age.set_index('date')['price'].resample('m').last().pct_change().corr(target_history.set_index('date')['price'].resample('M').last().pct_change())
        corr_data['corr_weekly'] = history_ten_year_age.set_index('date')['price'].resample('W').last().pct_change().corr(target_history.set_index('date')['price'].resample('W').last().pct_change())
        corr_data['corr_daily'] = history_ten_year_age.set_index('date')['price'].resample('D').last().pct_change().corr(target_history.set_index('date')['price'].resample('D').last().pct_change())
        corr_data = pd.DataFrame([corr_data])
        corr_data_list.append(corr_data)

    # Recent 5 Year
    if total_years >= 5 and target_total_years >= 5:
        corr_data = {}
        corr_data['unit_period'] = "recent_5_year"
        corr_data['symbol'] = history['symbol'][0]
        corr_data['target_symbol'] = target_history['symbol'][0]
        corr_data['start_date'] = five_year_ago
        corr_data['end_date'] = end_date
        history_five_year_ago = history.loc[history['date'] > five_year_ago]
        corr_data['corr_yearly'] = history_five_year_ago.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
        corr_data['corr_monthly'] = history_five_year_ago.set_index('date')['price'].resample('m').last().pct_change().corr(target_history.set_index('date')['price'].resample('M').last().pct_change())
        corr_data['corr_weekly'] = history_five_year_ago.set_index('date')['price'].resample('W').last().pct_change().corr(target_history.set_index('date')['price'].resample('W').last().pct_change())
        corr_data['corr_daily'] = history_five_year_ago.set_index('date')['price'].resample('D').last().pct_change().corr(target_history.set_index('date')['price'].resample('D').last().pct_change())
        corr_data = pd.DataFrame([corr_data])
        corr_data_list.append(corr_data)

    if len(corr_data_list) >= 1 :
        df = pd.concat(corr_data_list)
        df['summary_corr_pk'] = df['symbol'] + "-" + df['target_symbol'] + "-" + df['unit_period']
    # if len(df) >= 1:
        return df
    else:
        return None


@ray.remote
def collect_corr(symbol, target_symbols):
    current_time = datetime.now().strftime('%y-%m-%d %H:%M:%S.%f')[:-4]
    time.sleep(2)
    print(f"[{current_time}] Start to get correlations of: {symbol}")
    history = pd.read_csv(f'./downloads/history/etf/{symbol}_history.csv')
    corrs = []

    for target_symbol in target_symbols:
        target_history = pd.read_csv(f'./downloads/history/etf/{target_symbol}_history.csv')
        corr = get_corr(history=history, target_history=target_history)
        if corr is not None:
            corrs.append(corr)

    if corrs is not None:
        corrs = pd.concat(corrs)
        os.makedirs(f'./downloads/summary_corr/etf', exist_ok=True)
        corrs.to_csv(f'./downloads/summary_corr/etf/summary_corr_{symbol}.csv', index=False)


if __name__ == '__main__':
    symbols = [x.split('_')[0] for x in os.listdir("./downloads/history/etf") if x.endswith('csv')]
    symbols = sorted(symbols)[48:80]
    print(symbols)

    target_symbols = [x.split('_')[0] for x in os.listdir("./downloads/history/etf") if x.endswith('csv')]
    target_symbols = sorted(target_symbols)

    ray.init(ignore_reinit_error=True,  num_cpus=8)
    ray_task = [collect_corr.remote(symbol, target_symbols) for symbol in symbols]
    ray.get(ray_task) # n_cpus단위로 수행, 마지막 작업이 끝날때까지 대기.. 인줄 알았는데 알아서 받아서 하는듯? ncpus*3 이상의 태스크를 돌려보면 알 듯 #해보니 4번씩 있어야하는데 5번있는 pid 존재했음 -> 알아서 바로 받아서 함
    # ray.wait(ray_task)  # 내 작업 끝나면 바로 다음 작업 시작

    # for symbol in tqdm(symbols[:], mininterval=0.5):
    #     break # ray 아닌 버전
    #     history = pd.read_csv(f'./downloads/history/etf/{symbol}_history.csv')
    
    #     corrs = []
    #     for target_symbol in tqdm(target_symbols[:], mininterval=0.5): 
    #         target_history = pd.read_csv(f'./downloads/history/etf/{target_symbol}_history.csv')
    #         corr = get_corr(history=history, target_history=target_history)
    #         if corr is not None:
    #             corrs.append(corr)
        
    #     if corrs is not None:
    #         corrs = pd.concat(corrs)
    #         os.makedirs(f'./downloads/summary_corr/etf', exist_ok=True) # chunk도 모아야 함 # unit period 별 탑 200만 남길까?
    #         corrs.to_csv(f'./downloads/summary_corr/etf/summary_corr_{symbol}.csv', index=False)
            
    #         # unit period 별 rank 정렬해줘야 함
    #         print(corrs)
        


# def get_corr(history, target_history):
#     start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
#     end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
#     num_of_years = (end_date - start_date).days / 365.25

#     target_start_date = target_history.loc[target_history['date'] == target_history['date'].min(), 'date'].squeeze()
#     target_end_date = target_history.loc[target_history['date'] == target_history['date'].max(), 'date'].squeeze()
#     target_num_of_years = (target_end_date - target_start_date).days / 365.25

#     if num_of_years <= target_num_of_years: # 나보다 길면 비교
#         corr_row = {}
#         corr_row['symbol_fk'] = history['symbol_fk'][0]
#         corr_row['target_symbol_fk'] = target_history['symbol_fk'][0]
#         corr_row['start_date'] = start_date.strftime('%Y-%m-%d')
#         corr_row['end_date'] = end_date.strftime('%Y-%m-%d')
#         corr_row['corr_yearly'] = history.set_index('date')['price'].resample('Y').last().pct_change().corr(target_history.set_index('date')['price'].resample('Y').last().pct_change())
#         corr_row = pd.DataFrame([corr_row])
#         # display(corr_row)
#         return corr_row
#     else:
#         return None

# local_dirpath = "./download/history/etf/"
# symbols = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv')]
# symbols = sorted(symbols)[:]
# symbols = ['SPY']
# # SPY 다시 해야 함


# for symbol in tqdm(symbols):
#     time.sleep(2)
#     # symbol = 'QQQ'
#     # load
#     history = pd.read_csv(f'./download/history/etf/history_{symbol}.csv')
#     history['date'] = pd.to_datetime(history['date'])

#     start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
#     end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
#     num_of_years = (end_date - start_date).days / 365.25

#     ## get corr all time
#     corr_df = []
#     target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
#     for target_symbol in tqdm(target_symbols, mininterval=0.5):
#         target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
#         target_history['date'] = pd.to_datetime(target_history['date'])

#         corr = get_corr(history, target_history)
#         corr_df.append(corr)
#     corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
#     corr_df['unit_period'] = 'all_time'
#     corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
#     corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
#     corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
#     print(corr_df)
#     os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
#     corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_all_{symbol}.csv', index=False)

#     ### get corr recent 15 year    
#     if num_of_years > 15:
#         fifteen_year_ago = end_date - pd.DateOffset(years=15)
#         corr_df = []
#         target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
#         for target_symbol in tqdm(target_symbols, mininterval=0.5):
#             target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
#             target_history['date'] = pd.to_datetime(target_history['date'])

#             corr = get_corr(history.loc[history['date'] > fifteen_year_ago].reset_index(drop=True), target_history)
#             corr_df.append(corr)
#         corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
#         corr_df['unit_period'] = 'recent_15_year'
#         corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
#         corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
#         corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
#         os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
#         corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_15y_{symbol}.csv', index=False)
#         print(corr_df)
    
#     ### get corr recent 10 year    
#     if num_of_years > 10:
#         ten_year_ago = end_date - pd.DateOffset(years=10)
#         corr_df = []
#         target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
#         for target_symbol in tqdm(target_symbols, mininterval=0.5):
#             target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
#             target_history['date'] = pd.to_datetime(target_history['date'])

#             corr = get_corr(history.loc[history['date'] > ten_year_ago].reset_index(drop=True), target_history)
#             corr_df.append(corr)
#         corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
#         corr_df['unit_period'] = 'recent_10_year'
#         corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
#         corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
#         corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
#         os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
#         corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_10y_{symbol}.csv', index=False)
#         print(corr_df)

#     ### get corr recent 5 year    
#     if num_of_years > 5:
#         five_year_ago = end_date - pd.DateOffset(years=5)
#         corr_df = []
#         target_symbols  = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv') if symbol not in x]
#         for target_symbol in tqdm(target_symbols, mininterval=0.5):
#             target_history = pd.read_csv(f'./download/history/etf/history_{target_symbol}.csv')
#             target_history['date'] = pd.to_datetime(target_history['date'])

#             corr = get_corr(history.loc[history['date'] > five_year_ago].reset_index(drop=True), target_history)
#             corr_df.append(corr)
#         corr_df = pd.concat(corr_df).sort_values(by='corr_yearly', ascending=False).reset_index(drop=True)
#         corr_df['unit_period'] = 'recent_5_year'
#         corr_df['summary_corr_pk'] = corr_df['symbol_fk'] + '-' + corr_df['target_symbol_fk'] + '-' + corr_df['unit_period']
#         corr_df['rank_desc'] = corr_df['corr_yearly'].rank(ascending=False)
#         corr_df['rank_asc'] = corr_df['corr_yearly'].rank()
        
#         os.makedirs(f'./download/summary_corr/etf/', exist_ok=True)
#         corr_df.to_csv(f'./download/summary_corr/etf/summary_corr_5y_{symbol}.csv', index=False)
#         print(corr_df)
    

# dirpath_corr_etf = './download/summary_corr/etf/'
# dirpath_corr_chunk = './download/summary_corr/chunk/'
# save_dfs_by_chunk(dirpath_corr_etf, dirpath_corr_chunk, prefix_chunk="concatenated_summary_corr")
