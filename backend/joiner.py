import pandas as pd

def leftjoin_df(df1, df2, key="Date"):
    return pd.merge(df1, df2, on=key, how="left")

def innerjoin_df(df1, df2, key="Date"):
    return pd.merge(df1, df2, on=key, how="inner")

def rightjoin_df(df1, df2, key="Date"):
    return pd.merge(df1, df2, on=key, how="right")
    