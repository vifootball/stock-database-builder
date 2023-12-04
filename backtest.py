import pandas as pd

def add(x,y, date):
    import numpy as np
    return np.add(x, y).tolist()

def get_backtested_portfolio(
            initial_investment, periodic_investment, 
            date, symbol_1_price, symbol_2_price,
            symbol_1_ratio, symbol_2_ratio
 ):
    
    # 초기 설정값들
    backtested_monthly_dfs = []
    monthly_df_order = 0

    recent_total_price = None
    total_investment = 0
    


    # 데이터프레임 생성
    df = pd.DataFrame({
          'date': date,
          'symbol_1_price': symbol_1_price,
          'symbol_2_price': symbol_2_price
    })


    # 월별로 데이터프레임 자르기
    monthly_dfs = []
    yymms = pd.to_datetime(df['date']).dt.strftime('%y-%m').unique()
    for yymm in yymms:
        monthly_df = df[
           pd.to_datetime(df['date']).dt.strftime('%y-%m') == yymm
        ]

        monthly_df = monthly_df.reset_index(drop=True)
        monthly_dfs.append(monthly_df)


    # 백테스트 하기
    for monthly_df in monthly_dfs[:]:
        if monthly_df_order == 0:
            # print("First month of investing")
            total_investment = initial_investment
            monthly_df['total_investment'] = total_investment

            # 해당 월의 종목별 주식 수 (고정)
            monthly_df['symbol_1_share'] = (initial_investment * symbol_1_ratio) / monthly_df['symbol_1_price'].iloc[0]
            monthly_df['symbol_2_share'] = (initial_investment * symbol_2_ratio) / monthly_df['symbol_2_price'].iloc[0]
            
        else:
            # print(f"{monthly_df_order+1}th month of investing")
            total_investment += periodic_investment
            monthly_df['total_investment'] = total_investment

            # 해당 월의 종목별 주식 수 (고정)
            monthly_df['symbol_1_share'] = ((recent_total_price + periodic_investment) * symbol_1_ratio) / monthly_df['symbol_1_price'].iloc[0]
            monthly_df['symbol_2_share'] = ((recent_total_price + periodic_investment) * symbol_2_ratio) / monthly_df['symbol_2_price'].iloc[0]

        # 해당 월의 포트폴리오 가치 (매일 변함)
        monthly_df['total_price'] = monthly_df['symbol_1_price'] * monthly_df['symbol_1_share'] + monthly_df['symbol_2_price'] * monthly_df['symbol_2_share']

        # 누적 수익률
        monthly_df['total_return'] = (monthly_df['total_price'] - monthly_df['total_investment']) / monthly_df['total_investment']
        
        # 해당 월의 종목 별 비중 (매일 변함)
        monthly_df['symbol_1_proportion'] = (monthly_df['symbol_1_share'] * monthly_df['symbol_1_price']) / monthly_df['total_price']
        monthly_df['symbol_2_proportion'] = (monthly_df['symbol_2_share'] * monthly_df['symbol_2_price']) / monthly_df['total_price']


        backtested_monthly_dfs.append(monthly_df)
        recent_total_price = monthly_df['total_price'].to_list()[-1]
        monthly_df_order += 1

    
    # 벡테스트 결과 합치기
    backtested_portfolio = pd.concat(backtested_monthly_dfs).reset_index(drop=True)
    
    return backtested_portfolio
