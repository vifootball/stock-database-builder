import pandas as pd
import numpy as np

# dividend


# price
def price_to_price_change(col_price):
    col_price_change = col_price.diff()
    return col_price_change

def price_change_to_price_change_sign(col_price_change):
    col_price_change_sign =  np.sign(col_price_change)
    return col_price_change_sign




