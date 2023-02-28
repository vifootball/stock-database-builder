class AssetCategories:
    EQUITY = [
        "Large Blend", "Large Value", "Technology", "Miscellaneous Region", "Large Growth",
        "Foreign Large Blend", "Diversified Emerging Mkts", "Small Blend", "Mid-Cap Blend","Health", "Natural Resources",
        "China Region", "Financial", "Foreign Large Value", "Mid-Cap Growth", "Consumer Cyclical", 
        "Mid-Cap Value", "Equity Energy", "Miscellaneous Sector", "Small Value", "Europe Stock",
        "Industrials", "Energy Limited Partnership","Small Growth", "Foreign Large Growth", "Equity Precious Metals",
        "Consumer Defensive", "Long-Short Equity", "Communications", "Utilities", "Pacific/Asia ex-Japan Stk", 
        "Preferred Stock", "India Equity","Foreign Small/Mid Blend", "Foreign Small/Mid Value", "Infrastructure",
        "Latin America Stock","World Stock", "Diversified Pacific/Asia", "Foreign Small/Mid Growth", "Bear Market",
        "Market Neutral", "Multialternative", "Long-Short Credit", "Japan Stock"
    ]
    BOND = [
        "High Yield Bond", "Corporate Bond", "Ultrashort Bond", "Short-Term Bond", "Intermediate-Term Bond",
        "Emerging Markets Bond", "Muni National Interm", "Multisector Bond", "Intermediate Government","Inflation-Protected Bond",
        "Long Government", "World Bond", "Nontraditional Bond", "Emerging-Markets Local-Currency Bond", "Short Government",
        "Bank Loan", "Muni National Long", "Muni National Short", "Long-Term Bond", "High Yield Muni",
        "Convertibles", "Muni Minnesota", "Muni New York Intermediate", "Muni California Long"
    ]
    COMMODITY = [
        "Commodities Broad Basket"
    ]
    OTHER = [
        "Trading--Leveraged Equity", "Trading--Inverse Equity","Trading--Miscellaneous",
        "Trading--Inverse Debt", "Trading--Leveraged Commodities", "Trading--Inverse Commodities", "Trading--Leveraged Debt",
        "Real Estate", "Global Real Estate",
        "World Allocation", "Tactical Allocation",
        "Volatility", "Single Currency", "Managed Futures", "Option Writing", "Multicurrency",
        r"Allocation--15% to 30% Equity",
        r"Allocation--30% to 50% Equity",
        r"Allocation--50% to 70% Equity",
        r"Allocation--70% to 85% Equity",
        r"Allocation--85%+ Equity"
    ]


class Symbols():
    YAHOO_MAIN = [
        '^GSPC', '^DJI', '^IXIC', '^NYA', '^XAX', 
        '^BUK100P', '^RUT', '^VIX', '^FTSE', '^GDAXI', 
        '^FCHI', '^STOXX50E', '^N100', '^BFX', 'IMOEX.ME', 
        '^N225', '^HSI', '000001.SS', '399001.SZ', '^STI', 
        '^AXJO', '^AORD', '^BSESN', '^JKSE', '^KLSE', 
        '^NZ50', '^KS11', '^TWII', '^GSPTSE', '^BVSP', 
        '^MXX', '^IPSA', '^MERV', '^TA125.TA', '^CASE30', 
        '^JN0U.JO', 'ES=F', 'YM=F', 'NQ=F', 'RTY=F', 'ZB=F', 
        'ZN=F', 'ZF=F', 'ZT=F', 'GC=F', 'MGC=F', 
        'SI=F', 'SIL=F', 'PL=F', 'HG=F', 'PA=F', 
        'CL=F', 'HO=F', 'NG=F', 'RB=F', 'BZ=F', 
        'B0=F', 'ZC=F', 'ZO=F', 'KE=F', 'ZR=F', 
        'ZM=F', 'ZL=F', 'ZS=F', 'GF=F', 'HE=F', 
        'LE=F', 'CC=F', 'KC=F', 'CT=F', 'LBS=F', 
        'OJ=F', 'SB=F'
    ]
    FRED = {
        'CPIAUCSL': 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average',
        'EFFR': 'Effective Federal Funds Rate',
        'T10Y2Y': '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity',
        'T10Y3M': '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity',
        'T10YIE': '10-Year Breakeven Inflation Rate',
        'USREC': 'NBER based Recession Indicators for the United States from the Period following the Peak through the Trough',
        'USRECM': 'NBER based Recession Indicators for the United States from the Peak through the Trough',
        'USRECP': 'NBER based Recession Indicators for the United States from the Peak through the Period preceding the Trough',
        'USAREC': 'OECD based Recession Indicators for the United States from the Period following the Peak through the Trough',
        'USARECM': 'OECD based Recession Indicators for the United States from the Peak through the Trough',
        'T10YIE': '10-Year Breakeven Inflation Rate',
        'DEXKOUS': 'South Korean Won to U.S. Dollar Spot Exchange Rate',
        'INTDSRKRM193N': 'Interest Rates, Discount Rate for Republic of Korea',
        'KORCPIALLMINMEI': 'Consumer Price Index: All Items for Korea'
    }
