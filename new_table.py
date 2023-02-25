import numpy as np
import pandas as pd

class TableHandler:
    def __init__(self, table_config):
        self.table_config = table_config

    def get_src_columns(self) -> list:
        table_config = self.table_config
        src_columns = [key for key in table_config.keys()]
        return src_columns

    def rename_columns(self, df):
        table_config = self.table_config
        col_name_mapping = {col: col_config.name_adj for col, col_config in table_config.items()}
        df = df.rename(columns=col_name_mapping)
        return df

    def get_columns_to_select(self) -> list:
        table_config = self.table_config
        cols_to_select = [col_config.name_adj for col_config in table_config.values() if col_config.select]
        return cols_to_select

    def select_columns(self, df):
        cols_to_select = self.get_columns_to_select()
        df = df[cols_to_select]
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

    def check_columns(self, df): # validate?
        expected_columns = self.get_columns_to_select()
        unexpected_columns = set(df.columns) - set(expected_columns)
        missing_columns = set(expected_columns) - set(df.columns)
        if unexpected_columns:
            raise ValueError(f"Unexpected columns in data: {unexpected_columns}")
        if missing_columns:
            raise ValueError(f"Missing columns in data: {missing_columns}")

