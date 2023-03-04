import pandas as pd
import datetime as dt
from tqdm import tqdm
from utils import *
from table import TableHandler
import table_config

def get_summary_grade(metadata, history, history_market):
    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
    num_of_years = (end_date - start_date).days / 365.25
        
    df = {}
    # df['unit_period'] = 'all_time'
    df['last_update'] = end_date.strftime('%Y-%m-%d')
    df['symbol_fk'] = metadata['symbol_pk'].squeeze()
    df['track_records_d'] = (dt.datetime.today() - pd.to_datetime(metadata['inception_date'])).dt.days.squeeze()
    df['track_records_y'] = df['track_records_d'] / 365
    df['expense_ratio'] = metadata['expense_ratio'].squeeze()
    df['nav'] = history.loc[history['date'] == end_date]['price'].squeeze()
    df['shares_out'] = metadata['shares_out'].squeeze()
    df['aum'] = (df['nav'] * df['shares_out']).squeeze()
    df['total_return' ] = (history['price'].loc[history['date'] == end_date].squeeze() / history['price'].loc[history['date'] == start_date].squeeze()) - 1
    df['cagr'] = (1 + df['total_return']) ** (1 / num_of_years) - 1
    df['std_yearly_return'] = history.set_index('date')['price'].resample('Y').last().pct_change().std()
    df['drawdown_max'] = ((history['price'] / history['price'].cummax()) - 1).min()
    df['div_ttm'] = history['dividend_ttm'].loc[history['date'] == end_date].squeeze()
    df['div_yield_ttm'] = history['dividend_rate_ttm'].loc[history['date'] == end_date].squeeze()
    df['div_count_ttm'] = history['dividend_paid_count_ttm'].loc[history['date'] == end_date].squeeze()

    df['market_corr_daily'] = history.set_index('date')['price'].pct_change().corr(history_market.set_index('date')['price'].pct_change())
    df['market_corr_weekly'] = history.set_index('date')['price'].resample('W-FRI').last().pct_change().corr(history_market.set_index('date')['price'].resample('W-FRI').last().pct_change())
    df['market_corr_monthly'] = history.set_index('date')['price'].resample('M').last().pct_change().corr(history_market.set_index('date')['price'].resample('M').last().pct_change())
    df['market_corr_yearly'] = history.set_index('date')['price'].resample('Y').last().pct_change().corr(history_market.set_index('date')['price'].resample('Y').last().pct_change())
    df['vol_dollar_3m_avg'] = history['volume_of_dollar_3m_avg'].ffill().loc[history['date'] == end_date].squeeze()

    df = pd.DataFrame(df, index=[0])
    return df

def get_summary_grade_pivotted(summary_grade):
    cols = [
        'symbol_fk', 'unit_period', 
        'track_records_y', 'expense_ratio', 'aum', 'total_return',
        'cagr', 'std_yearly_return', 'drawdown_max', 'div_yield_ttm', 'div_count_ttm',
        'market_corr_yearly', 'vol_dollar_3m_avg'
        
    ]
    id_vars = ['symbol_fk', 'unit_period']
    summary_grade_piv = pd.melt(summary_grade[cols], id_vars=id_vars, var_name='var_name').sort_values(by=id_vars).reset_index(drop=True)
    summary_grade_piv['summary_grd_piv_pk'] = summary_grade_piv['symbol_fk'] + '-' + summary_grade_piv['unit_period']
    return summary_grade_piv


# get summary grade
local_dirpath = "./download/history/etf"
symbols = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv')]
# symbol = symbols[0]
history_market = pd.read_csv(f'./download/history/etf/history_spy.csv')
history_market['date'] = pd.to_datetime(history_market['date'])

for symbol in tqdm(symbols[:], mininterval=0.5):
    # load
    history = pd.read_csv(f'./download/history/etf/history_{symbol}.csv')
    history['date'] = pd.to_datetime(history['date'])
    metadata = pd.read_csv(f'./download/metadata/etf/metadata_etf_{symbol}.csv')

    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
    num_of_years = (end_date - start_date).days / 365.25

    five_year_ago = end_date - pd.DateOffset(years=5)
    ten_year_ago = end_date - pd.DateOffset(years=10)
    fifteen_year_ago = end_date - pd.DateOffset(years=15)


    summaries = []
    summary_all_time = get_summary_grade(metadata=metadata, history=history, history_market=history_market)
    summary_all_time['unit_period'] = 'all_time'
    summary_all_time['summary_grade_pk'] = summary_all_time['symbol_fk'] + '-' + summary_all_time['unit_period']
    summaries.append(summary_all_time)
    if num_of_years > 5:
        summary_5_year = get_summary_grade(metadata=metadata, history=history.loc[history['date'] > five_year_ago], history_market=history_market)
        summary_5_year['unit_period'] = 'recent_5_year'
        summary_5_year['summary_grade_pk'] = summary_5_year['symbol_fk'] + '-' + summary_5_year['unit_period']
        summaries.append(summary_5_year)
    if num_of_years > 10:
        summary_10_year = get_summary_grade(metadata=metadata, history=history.loc[history['date'] > ten_year_ago], history_market=history_market)
        summary_10_year['unit_period'] = 'recent_10_year'
        summary_10_year['summary_grade_pk'] = summary_10_year['symbol_fk'] + '-' + summary_10_year['unit_period']
        summaries.append(summary_10_year)
    if num_of_years > 15:
        summary_15_year = get_summary_grade(metadata=metadata, history=history.loc[history['date'] > fifteen_year_ago], history_market=history_market)
        summary_15_year['unit_period'] = 'recent_15_year'
        summary_15_year['summary_grade_pk'] = summary_15_year['symbol_fk'] + '-' + summary_15_year['unit_period']
        summaries.append(summary_15_year)

    summaries = pd.concat(summaries).reset_index(drop=True)
    os.makedirs(f'./download/summary_grade/etf', exist_ok=True)
    summaries.to_csv(f'./download/summary_grade/etf/summary_grade_{symbol}.csv', index=False)


summaries_chunk = concat_csv_files_in_dir(get_dirpath = f'./download/summary_grade/etf')
summaries_chunk = TableHandler(table_config=table_config.SUMMARY_GRD).select_columns(summaries_chunk)
os.makedirs(f'./download/summary_grade/chunk', exist_ok=True)
summaries_chunk.to_csv(f'./download/summary_grade/chunk/summary_grade_etf_chunk.csv', index=False)


# get summary grade pivotted
summaries_chunk = pd.read_csv(f'./download/summary_grade/chunk/summary_grade_etf_chunk.csv')
summary_grade_piv = get_summary_grade_pivotted(summaries_chunk)
summary_grade_piv = TableHandler(table_config=table_config.SUMMARY_GRD_PIV).select_columns(summary_grade_piv)
os.makedirs(f'./download/summary_grade_piv/chunk', exist_ok=True)
summary_grade_piv.to_csv(f'./download/summary_grade_piv/chunk/summary_grade_piv.csv', index=False)