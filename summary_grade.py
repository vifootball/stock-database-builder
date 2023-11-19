import pandas as pd
import datetime as dt
from tqdm import tqdm
from utils import *
from table import TableHandler
import table_config
pd.options.mode.chained_assignment = None

def get_summary_grade(masterdata, history_symbol, history_market):

    history_symbol['date'] = pd.to_datetime(history_symbol['date'])
    history_market['date'] = pd.to_datetime(history_market['date'])

    first_date = history_symbol.loc[history_symbol['date'] == history_symbol['date'].min(), 'date'].squeeze()
    last_date = history_symbol.loc[history_symbol['date'] == history_symbol['date'].max(), 'date'].squeeze()

    df = {}
    df['symbol'] = history_symbol['symbol'].iloc[0]
    df['last_update'] = last_date.strftime("%Y-%m-%d")
    df['fund_age_day'] = (dt.datetime.today() - pd.to_datetime(masterdata['inception_date'])).dt.days.squeeze()
    df['fund_age_year'] = df['fund_age_day'] / 365.25
    df['expense_ratio'] = masterdata['expense_ratio'].squeeze()
    df['nav'] = history_symbol.loc[history_symbol['date'] == last_date]['price'].squeeze()
    df['shares_out'] = masterdata['shares_out'].squeeze()
    df['aum'] = (df['nav'] * df['shares_out']).squeeze()
    df['total_return' ] = (history_symbol['price'].loc[history_symbol['date'] == last_date].squeeze() / history_symbol['price'].loc[history_symbol['date'] == first_date].squeeze()) - 1
    df['cagr'] = (1 + df['total_return']) ** (1 / df['fund_age_year']) - 1
    df['std_yearly_return'] = history_symbol.set_index('date')['price'].resample('Y').last().pct_change().std() # 올해는 최근일 기준
    df['drawdown_max'] = ((history_symbol['price'] / history_symbol['price'].cummax()) - 1).min()
    df['div_ttm'] = history_symbol['dividend_ttm'].loc[history_symbol['date'] == last_date].squeeze()
    df['div_yield_ttm'] = history_symbol['dividend_rate_ttm'].loc[history_symbol['date'] == last_date].squeeze()
    df['div_paid_cnt_ttm'] = history_symbol['dividend_paid_count_ttm'].loc[history_symbol['date'] == last_date].squeeze()

    df['mkt_corr_daily'] = history_symbol.set_index('date')['price'].pct_change().corr(history_market.set_index('date')['price'].pct_change())
    df['mkt_corr_weekly'] = history_symbol.set_index('date')['price'].resample('W-FRI').last().pct_change().corr(history_market.set_index('date')['price'].resample('W-FRI').last().pct_change())
    df['mkt_corr_monthly'] = history_symbol.set_index('date')['price'].resample('M').last().pct_change().corr(history_market.set_index('date')['price'].resample('M').last().pct_change())
    df['mkt_corr_yearly'] = history_symbol.set_index('date')['price'].resample('Y').last().pct_change().corr(history_market.set_index('date')['price'].resample('Y').last().pct_change())
    df['volume_dollar_3m_avg'] = history_symbol['volume_of_dollar_3m_avg'].ffill().loc[history_symbol['date'] == last_date].squeeze()

    df=pd.DataFrame(df, index=[0])
    return df


def pivot_summary_grade(summary_grade):
    measures = [
        'symbol', 
        'unit_period', 
        'fund_age_day', 'fund_age_year', 'expense_ratio', 'nav', 'aum',
        'total_return', 'cagr', 'std_yearly_return', 'drawdown_max', 
        'div_ttm', 'div_yield_ttm', 'div_paid_cnt_ttm',
        'mkt_corr_daily', 'mkt_corr_weekly', 'mkt_corr_monthly', 'mkt_corr_yearly',
        'volume_dollar_3m_avg'
    ]

    # dims = ['symbol']
    dims = ['symbol', 'unit_period']
    summary_grade_piv = pd.melt(summary_grade[measures], id_vars=dims, var_name='var_name').sort_values(by=dims).reset_index(drop=True)
    # summary_grade_piv['summary_grd_piv_pk'] = summary_grade_piv['symbol'] + '-' + summary_grade_piv['unit_period']
    return summary_grade_piv


def run_summary_main():
    # get summary grade
    symbols = [x.split('_')[0] for x in os.listdir("./downloads/history/etf") if x.endswith('csv')][:]
    # print(symbols)
    history_market = pd.read_csv(f'./downloads/history/etf/SPY_history.csv')
    masters =  pd.read_csv("./downloads/master_symbols/masters_etf.csv")

    for symbol in tqdm(symbols[:], mininterval=0.5):
        # load
        print(symbol)
        history_symbol = pd.read_csv(f'./downloads/history/etf/{symbol}_history.csv')
        master = masters[masters['symbol']==symbol]

        first_date = pd.to_datetime(history_symbol.loc[history_symbol['date'] == history_symbol['date'].min(), 'date'].squeeze())
        last_date = pd.to_datetime(history_symbol.loc[history_symbol['date'] == history_symbol['date'].max(), 'date'].squeeze())
        
        five_year_ago = last_date - pd.DateOffset(years=5)
        ten_year_ago = last_date - pd.DateOffset(years=10)
        fifteen_year_ago = last_date - pd.DateOffset(years=15)

        summaries = []
        summary_all_time = get_summary_grade(masterdata=master, history_symbol=history_symbol, history_market=history_market)
        summary_all_time['unit_period'] = 'all_time'
        summary_all_time['summary_grade_pk'] = summary_all_time['symbol'] + '-' + summary_all_time['unit_period']
        summaries.append(summary_all_time)

        if summary_all_time['fund_age_year'].squeeze() > 5:
            summary_5_year = get_summary_grade(masterdata=master, history_symbol=history_symbol.loc[history_symbol['date'] > five_year_ago], history_market=history_market)
            summary_5_year['unit_period'] = 'recent_5_year'
            summary_5_year['summary_grade_pk'] = summary_5_year['symbol'] + '-' + summary_5_year['unit_period']
            summaries.append(summary_5_year)
        if summary_all_time['fund_age_year'].squeeze() > 10:
            summary_10_year = get_summary_grade(masterdata=master, history_symbol=history_symbol.loc[history_symbol['date'] > ten_year_ago], history_market=history_market)
            summary_10_year['unit_period'] = 'recent_10_year'
            summary_10_year['summary_grade_pk'] = summary_10_year['symbol'] + '-' + summary_10_year['unit_period']
            summaries.append(summary_10_year)
        if summary_all_time['fund_age_year'].squeeze() > 15:
            summary_15_year = get_summary_grade(masterdata=master, history_symbol=history_symbol.loc[history_symbol['date'] > fifteen_year_ago], history_market=history_market)
            summary_15_year['unit_period'] = 'recent_15_year'
            summary_15_year['summary_grade_pk'] = summary_15_year['symbol'] + '-' + summary_15_year['unit_period']
            summaries.append(summary_15_year)

        summaries = pd.concat(summaries).reset_index(drop=True)
        os.makedirs(f'./downloads/summary_grade/etf', exist_ok=True)
        summaries.to_csv(f'./downloads/summary_grade/etf/{symbol}_summary_grade.csv', index=False)

    # concat summary grades
    summaries_chunk = concat_csv_files_in_dir(get_dirpath = f'./downloads/summary_grade/etf')
    os.makedirs(f'./downloads/bigquery_tables/', exist_ok=True)
    summaries_chunk.to_csv(f'./downloads/bigquery_tables/summary_grades.csv', index=False)

    # pivot summary grades
    summaries_chunk = pd.read_csv(f'./downloads/bigquery_tables/summary_grades.csv')
    summary_grade_piv = pivot_summary_grade(summaries_chunk)
    summary_grade_piv.to_csv(f'./downloads/bigquery_tables/summary_grades_piv.csv', index=False)


if __name__ == '__main__':
    run_summary_main()
 


