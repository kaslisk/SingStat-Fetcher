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

def list_csvs():
    files = [f for f in os.listdir("./tmp") if f.endswith(".csv")]
    return {"csv files": files}

def save_joined(df: pd.DataFrame, name):
    dir = "./joined"
    if not os.path.exists(dir):
        os.mkdir(dir)
    path = os.path.join(dir, f"{name}.csv")
    df.to_csv(path, index=False)
    return path


def update():
    l = list()
    dir = "./tmp"
    for f in os.listdir(dir):
        if not f.endswith("joined.csv"):
            l.append(f[0:-4])
    return l

def delete(name):
    dir = "./tmp"
    path = os.path.join(dir, f"{name}.csv")
    os.remove(path)
    return name
