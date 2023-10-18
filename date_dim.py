import pandas as pd

# 시작일부터 오늘까지의 date dimension 테이블을 생성
def get_date_dim(start_date: str = '1800-01-01') -> pd.DataFrame: # start_date must be 'yyyy-MM-dd' format
    """
    Generate a date dimension table starting from the given start date (YYYY-MM-DD).

    Parameters:
    start_date (str): The start date in YYYY-MM-DD format.

    Returns:
    pd.DataFrame: A date dimension table.
    """
    
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp('today')

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    date_dim = pd.DataFrame(date_range, columns=['date'])
    date_dim['date_pk'] = date_range.strftime('%Y%m%d').astype(int)
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['quarter'] = date_dim['date'].dt.quarter
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['month_name'] = date_dim['date'].dt.month_name()
    date_dim['iso_week'] = date_dim['date'].dt.isocalendar().week
    date_dim['day_of_year'] = date_dim['date'].dt.day_of_year
    date_dim['day_of_month'] = date_dim['date'].dt.day
    date_dim['day_of_week'] = date_dim['date'].dt.day_of_week
    date_dim['day_name'] = date_dim['date'].dt.day_name()
    date_dim['weekday_tf'] = (date_dim['date'].dt.dayofweek < 5).astype(int)
    date_dim['month_first_day_tf'] = date_dim['date'].dt.is_month_start.astype(int)
    date_dim['month_last_day_tf'] = date_dim['date'].dt.is_month_end.astype(int)
    date_dim['year_first_day_tf'] = date_dim['date'].dt.is_year_start.astype(int)
    date_dim['year_last_day_tf'] = date_dim['date'].dt.is_year_end.astype(int)
    date_dim['cnt_days_in_month'] = date_dim['date'].dt.days_in_month.astype(int)

    return date_dim

if __name__ == '__main__':
    df = get_date_dim()
    print(get_date_dim())
    df.to_csv('date_dim.csv', index=False)

