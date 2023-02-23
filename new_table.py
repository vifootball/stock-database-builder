import numpy as np
import pandas as pd

class TableHandler:
    def __init__(self, table_config):
        self.table_config = table_config

    def rename_columns(self, df):
        col_name_mapping = {col: col_config['name_adj'] for col, col_config in self.table_config.items()}
        df = df.rename(columns=col_name_mapping)
        return df

    def select_columns(self, df):
        table_config = self.table_config
        cols_to_save = [table_config[col]['name_adj'] for col in table_config if table_config[col]['select']]
        df = df[cols_to_save]
        return df

    def is_empty(self, df):
        if len(df)==0:
            return True
        else:
            return  False

    def append_na_row(self, df):
        na_row = pd.DataFrame({col: [np.nan] for col in df}) # empty row
        df = pd.concat([df, na_row], axis=0)
        return df

    # def check_columns(df, table_config): # validate?
    #     expected_columns = list(table_config.keys())
    #     unexpected_columns = set(df.columns) - set(expected_columns)
    #     missing_columns = set(expected_columns) - set(df.columns)
    #     if unexpected_columns:
    #         raise ValueError(f"Unexpected columns in source data: {unexpected_columns}")

