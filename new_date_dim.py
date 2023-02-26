import pandas as pd
from new_table import TableHandler
import new_table_config

class DateDim:
    def __init__(self):
        self.date_dim_table_handler = TableHandler(table_config=new_table_config.DATE_DIM)
        self.start_date = pd.Timestamp('1800-01-01')
        self.end_date = pd.Timestamp('today')

    def get_date_dim(self):
        # get data
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
        
        date_dim = pd.DataFrame(date_range, columns=['date'])
        date_dim['date_pk'] = date_range.strftime('%Y%m%d').astype(int)
        date_dim['year'] = date_dim['date'].dt.year
        date_dim['quarter'] = date_dim['date'].dt.quarter
        date_dim['month'] = date_dim['date'].dt.month
        date_dim['month_name'] = date_dim['date'].dt.month_name()
        date_dim['week_of_year'] = date_dim['date'].dt.isocalendar().week
        date_dim['day_of_year'] = date_dim['date'].dt.day_of_year
        date_dim['day'] = date_dim['date'].dt.day
        date_dim['day_name'] = date_dim['date'].dt.day_name()
        date_dim['day_of_week'] = date_dim['date'].dt.day_of_week
        date_dim['is_weekday'] = date_dim['date'].dt.dayofweek < 5
        date_dim['is_month_end'] = date_dim['date'].dt.is_month_end.astype(int)
        date_dim['is_month_start'] = date_dim['date'].dt.is_month_start.astype(int)
        date_dim['is_year_start'] = date_dim['date'].dt.is_year_start.astype(int)
        date_dim['is_year_end'] = date_dim['date'].dt.is_year_end.astype(int)
        date_dim['days_in_month'] = date_dim['date'].dt.days_in_month.astype(int)
        # date_dim['is_holiday'] = date_dim['date'].isin(holiday_list).astype(int)

        # table handling
        table_handler = self.date_dim_table_handler
        table_handler.check_columns(date_dim)
        return date_dim