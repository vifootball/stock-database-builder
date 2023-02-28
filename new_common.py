import numpy as np
import pandas as pd

def percentage_to_float(value):
    if pd.isna(value):
        return np.nan
    else:
        return float(value.replace('%',''))/100

def str_to_int(value):
    if pd.isna(value):
        return value
    elif isinstance(value, (int, float)):
        return int(value)
    else:
        multipliers = {'k': 1000, 'm': 1000000, 'b': 1000000000, 't': 1000000000000}
        suffix = value[-1]
        if suffix.isdigit():
            return int(value.replace(',', '').replace('.', ''))
        elif suffix.lower() in multipliers.keys():
            suffix = suffix.lower()
            return int(float(value[:-1]) * multipliers[suffix])
        else: # n/a 라고 뜨는 경우가 있음
            return np.nan
        
# print(str_to_int(112))
# print(str_to_int('11.2213123T'))