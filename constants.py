# df.to_dict(orient='records')

class Etfs:
    EXCLUDE = [
        'DTE'
    ]

class Indices:
    YAHOO_WORLD_INDICES = [
        # https://finance.yahoo.com/world-indices
        {'symbol': '^GSPC', 'name': 'S&P 500'},
        {'symbol': '^DJI', 'name': 'Dow Jones Industrial Average'},
        {'symbol': '^IXIC', 'name': 'NASDAQ Composite'},
        {'symbol': '^NYA', 'name': 'NYSE COMPOSITE (DJ)'},
        {'symbol': '^XAX', 'name': 'NYSE AMEX COMPOSITE INDEX'},
        {'symbol': '^BUK100P', 'name': 'Cboe UK 100'},
        {'symbol': '^RUT', 'name': 'Russell 2000'},
        {'symbol': '^VIX', 'name': 'CBOE Volatility Index'},
        {'symbol': '^FTSE', 'name': 'FTSE 100'},
        {'symbol': '^GDAXI', 'name':'DAX PERFORMANCE-INDEX'},
        {'symbol': '^FCHI', 'name':'FTSE 100'},
        {'symbol': '^STOXX50E', 'name': 'ESTX 50 PR.EUR'},
        {'symbol': '^N100', 'name': 'Euronext 100 Index'},
        {'symbol': '^BFX', 'name': 'BEL 20'},
        {'symbol': 'IMOEX.ME', 'name': 'MOEX Russia Index'},
        {'symbol': '^N225', 'name': 'Nikkei 225'},
        {'symbol': '^HSI', 'name': 'HANG SENG INDEX'},
        {'symbol': '000001.SS', 'name': 'SSE Composite Index'},
        {'symbol': '^STI', 'name': 'STI Index'},
        {'symbol': '^AXJO', 'name': 'S&P/ASX 200'},
        {'symbol': '^AORD', 'name': 'ALL ORDINARIES'},
        {'symbol': '^BSESN', 'name': 'S&P BSE SENSEX'},
        {'symbol': '^JKSE', 'name': 'IDX COMPOSITE'},
        {'symbol': '^KLSE', 'name': 'FTSE Bursa Malaysia KLCI'},
        {'symbol': '^NZ50', 'name': 'S&P/NZX 50 INDEX GROSS ( GROSS'},
        {'symbol': '^KS11', 'name': 'KOSPI Composite Index'},
        {'symbol': '^TWII', 'name': 'TSEC weighted index'},
        {'symbol': '^GSPTSE', 'name': 'S&P/TSX Composite index'},
        {'symbol': '^BVSP', 'name': 'IBOVESPA'},
        {'symbol': '^MXX', 'name': 'IPC MEXICO'},
        {'symbol': '^IPSA', 'name': 'S&P/CLX IPSA'},
        {'symbol': '^MERV', 'name': 'MERVAL'},
        {'symbol': '^TA125.TA', 'name': 'TA-125'},
        {'symbol': '^CASE30', 'name': 'EGX 30 Price Return Index'},
        {'symbol': '^JN0U.JO', 'name': 'Top 40 USD Net TRI Index'}
    ]

    YAHOO_COMMODITIES = [
        # https://finance.yahoo.com/commodities
        {'symbol': 'ES=F', 'name': 'E-Mini S&P 500'},
        {'symbol': 'YM=F', 'name': 'Mini Dow Jones Indus.-$5'},
        {'symbol': 'NQ=F', 'name': 'Nasdaq 100'},
        {'symbol': 'RTY=F', 'name': 'E-mini Russell 2000 Index Futur'},
        {'symbol': 'ZB=F', 'name': 'U.S. Treasury Bond Futures'},
        {'symbol': 'ZN=F', 'name': '10-Year T-Note Futures'},
        {'symbol': 'ZF=F', 'name': 'Five-Year US Treasury Note Futu'},
        {'symbol': 'ZT=F', 'name': '2-Year T-Note Futures'},
        {'symbol': 'GC=F', 'name': 'Gold'},
        {'symbol': 'MGC=F', 'name': 'Micro Gold Futures'},
        {'symbol': 'SI=F', 'name': 'Silver'},
        {'symbol': 'SIL=F', 'name': 'Micro Silver Futures'},
        {'symbol': 'PL=F', 'name': 'Platinum'},
        {'symbol': 'HG=F', 'name': 'Copper'},
        {'symbol': 'PA=F', 'name': 'Palladium'},
        {'symbol': 'CL=F', 'name': 'Crude Oil'},
        {'symbol': 'HO=F', 'name': 'Heating Oil'},
        {'symbol': 'NG=F', 'name': 'Natural Gas'},
        {'symbol': 'RB=F', 'name': 'RBOB Gasoline'},
        {'symbol': 'BZ=F', 'name': 'Brent Crude Oil Last Day Financ'},
        {'symbol': 'B0=F', 'name': 'Mont Belvieu LDH Propane (OPIS)'},
        {'symbol': 'ZC=F', 'name': 'Corn Futures'},
        {'symbol': 'ZO=F', 'name': 'Oat Futures'},
        {'symbol': 'KE=F', 'name': 'KC HRW Wheat Futures'},
        {'symbol': 'ZR=F', 'name': 'Rough Rice Futures'},
        {'symbol': 'ZM=F', 'name': 'S&P Composite 1500 ESG Tilted I'},
        {'symbol': 'ZL=F', 'name': 'Soybean Oil Futures'},
        {'symbol': 'ZS=F', 'name': 'Soybean Futures'},
        {'symbol': 'GF=F', 'name': 'WisdomTree International High D'},
        {'symbol': 'HE=F', 'name': 'Lean Hogs Future'},
        {'symbol': 'LE=F', 'name': 'Live Cattle Futures,'},
        {'symbol': 'CC=F', 'name': 'Cocoa'},
        {'symbol': 'KC=F', 'name': 'Coffee'},
        {'symbol': 'CT=F', 'name': 'Cotton'},
        {'symbol': 'LBS=F', 'name': 'Random Length Lumber Futures'},
        {'symbol': 'OJ=F', 'name': 'Orange Juice'},
        {'symbol': 'SB=F', 'name': 'Sugar'}
    ]

    FRED = [
        {'symbol': 'CPIAUCSL', 'name': 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average'},
        {'symbol': 'EFFR', 'name': 'Effective Federal Funds Rate'},
        {'symbol': 'T10Y2Y', 'name': '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity'},
        {'symbol': 'T10Y3M', 'name': '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity'},
        {'symbol': 'T10YIE', 'name': '10-Year Breakeven Inflation Rate'},
        {'symbol': 'USREC', 'name': 'NBER based Recession Indicators for the United States from the Period following the Peak through the Trough'},
        {'symbol': 'USRECM', 'name': 'NBER based Recession Indicators for the United States from the Peak through the Trough'},
        {'symbol': 'USRECP', 'name': 'NBER based Recession Indicators for the United States from the Peak through the Period preceding the Trough'},
        {'symbol': 'USAREC', 'name': 'OECD based Recession Indicators for the United States from the Period following the Peak through the Trough'},
        {'symbol': 'USARECM', 'name': 'OECD based Recession Indicators for the United States from the Peak through the Trough'},
        {'symbol': 'DEXKOUS', 'name': 'South Korean Won to U.S. Dollar Spot Exchange Rate'},
        {'symbol': 'INTDSRKRM193N', 'name': 'Interest Rates, Discount Rate for Republic of Korea'},
        {'symbol': 'KORCPIALLMINMEI', 'name': 'Consumer Price Index: All Items for Korea'}
    ]
