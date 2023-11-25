from google.cloud import bigquery

# 순서 바꿔서 테스트해보기 -> 순서 여기서 바꿔도 상관 없음
# INT에 Null 있으면 Float로 해야함
# df.info() 에 나오는 dtype으로 맞춰야 에러 안남

class Schema:
    HISTORY = [
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("open", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("high", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("low", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("close", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("dividend", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("stock_split", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price_all_time_high", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("drawdown_current", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("drawdown_max", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume_of_share", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume_of_share_3m_avg", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume_of_dollar", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume_of_dollar_3m_avg", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("dividend_paid_or_not", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("dividend_paid_count_ttm", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("dividend_ttm", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("dividend_rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("dividend_rate_ttm", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("normal_day_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("date_id", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("transaction_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("price_change", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price_change_rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price_change_sign", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("price_7d_ago", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("weekly_price_change", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("weekly_price_change_rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price_30d_ago", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("monthly_price_change", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("monthly_price_change_rate", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("price_365d_ago", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("yearly_price_change", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("yearly_price_change_rate", bigquery.enums.SqlTypeNames.FLOAT64)
    ]

    MASTER_DATE = [
        bigquery.SchemaField("date_pk", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("quarter", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("month", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("month_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("iso_week", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("day_of_year", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("day_of_month", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("day_of_week", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("day_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("weekday_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("month_first_day_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("month_last_day_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("year_first_day_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("year_last_day_tf", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("cnt_days_in_month", bigquery.enums.SqlTypeNames.INT64)
    ]

    MASTER_SYMBOL = [
        bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("fund_family", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("summary", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("net_assets", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("expense_ratio", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("inception_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("yf_date", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("aum", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("shares_out", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("sa_1_date", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("holdings_count", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("top10_percentage", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("asset_class", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sector", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("region", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sa_2_date", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("category", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("index_tracked", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sa_3_date", bigquery.enums.SqlTypeNames.FLOAT64)
    ]

    HOLDINGS = [
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("holdings_date", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("holding_symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("holding_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("holding_percent", bigquery.enums.SqlTypeNames.FLOAT64)
    ]

    DM_GRADE = [
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("last_update", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("fund_age_day", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("fund_age_year", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("expense_ratio", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("nav", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("shares_out", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("aum", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("total_return", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("cagr", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("std_yearly_return", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("drawdown_max", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("div_ttm", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("div_yield_ttm", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("div_paid_cnt_ttm", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("mkt_corr_daily", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("mkt_corr_weekly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("mkt_corr_monthly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("mkt_corr_yearly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("volume_dollar_3m_avg", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("unit_period", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("summary_grade_pk", bigquery.enums.SqlTypeNames.STRING)        
    ]

    DM_GRADE_PIVOT = [
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("unit_period", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("var_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("value", bigquery.enums.SqlTypeNames.FLOAT64)
    ]

    DM_CORRELATION = [
        bigquery.SchemaField("unit_period", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("target_symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("start_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("end_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("corr_yearly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("corr_monthly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("corr_weekly", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("corr_daily", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("summary_corr_pk", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("rank_yearly_asc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_yearly_desc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_monthly_asc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_monthly_desc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_weekly_asc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_weekly_desc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_daily_asc", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("rank_daily_desc", bigquery.enums.SqlTypeNames.INT64)    
    ]


if __name__=="__main__":
    print(Schema().MASTER_DATE)