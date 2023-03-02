import pandas as pd
import datetime as dt
from tqdm import tqdm
from utils import *


def get_summary(metadata, history, history_market):
    start_date = history.loc[history['date'] == history['date'].min(), 'date'].squeeze()
    end_date = history.loc[history['date'] == history['date'].max(), 'date'].squeeze()
    num_of_years = (end_date - start_date).days / 365.25
        
    df = {}
    # df['unit_period'] = 'all_time'
    df['symbol_fk'] = metadata['symbol_pk'].squeeze()
    df['track_records_d'] = (dt.datetime.today() - pd.to_datetime(metadata['inception_date'])).dt.days.squeeze()
    df['track_records_y'] = df['track_records_d'] / 365
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

local_dirpath = "./download/history/etf"
symbols = [x.split('.')[0].split('_')[1] for x in os.listdir(local_dirpath) if x.endswith('csv')]
# symbol = symbols[0]
history_market = pd.read_csv(f'./download/history/etf/history_spy.csv')
history_market['date'] = pd.to_datetime(history_market['date'])

for symbol in tqdm(symbols[:]):
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
    summary_all_time = get_summary(metadata=metadata, history=history, history_market=history_market)
    summary_all_time['unit_period'] = 'all_time'
    summary_all_time['summary_grade_pk'] = summary_all_time['symbol_fk'] + '-' + summary_all_time['unit_period']
    summaries.append(summary_all_time)
    if num_of_years > 5:
        summary_5_year = get_summary(metadata=metadata, history=history.loc[history['date'] > five_year_ago], history_market=history_market)
        summary_5_year['unit_period'] = '5_year'
        summary_5_year['summary_grade_pk'] = summary_5_year['symbol_fk'] + '-' + summary_5_year['unit_period']
        summaries.append(summary_5_year)
    if num_of_years > 10:
        summary_10_year = get_summary(metadata=metadata, history=history.loc[history['date'] > ten_year_ago], history_market=history_market)
        summary_10_year['unit_period'] = '10_year'
        summary_10_year['summary_grade_pk'] = summary_10_year['symbol_fk'] + '-' + summary_10_year['unit_period']
        summaries.append(summary_10_year)
    if num_of_years > 15:
        summary_15_year = get_summary(metadata=metadata, history=history.loc[history['date'] > fifteen_year_ago], history_market=history_market)
        summary_15_year['unit_period'] = '15_year'
        summary_15_year['summary_grade_pk'] = summary_15_year['symbol_fk'] + '-' + summary_15_year['unit_period']
        summaries.append(summary_15_year)
        
    summaries = pd.concat(summaries).reset_index(drop=True)
    os.makedirs(f'./download/summary/etf', exist_ok=True)
    summaries.to_csv(f'./download/summary/etf/summary_{symbol}.csv', index=False)

summaries_chunk = concat_csv_files_in_dir(get_dirpath = f'./download/summary/etf')
os.makedirs(f'./download/summary/chunk', exist_ok=True)
summaries_chunk.to_csv(f'./download/summary/chunk/summary_etf_chunk.csv', index=False)