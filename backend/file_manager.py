import os
import pandas as pd

def save_csv(df: pd.DataFrame, table_code):
    dir = "./tmp"
    if not os.path.exists(dir):
        os.mkdir(dir)
    path = os.path.join(dir, f"{table_code}.csv")
    df.to_csv(path, index=False)
    return path

def load_csv(path):
    return pd.read_csv(path)