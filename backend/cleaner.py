import pandas as pd
from parsedate import parse_date

def to_df(raw_json):
    table = raw_json["Data"]["row"]
    temp = dict()
    timelst = list()

    for i in range(0, len(table[0]["columns"])):
        date = parse_date(table[0]["columns"][i]["key"])
        timelst.insert(0, date)
    temp["Date"] = timelst

    for i in range(0, len(table)):
        templst = list()
        for j in range(0, len(table[i]["columns"])):
            templst.insert(0, table[i]["columns"][j]["value"])
        temp[table[i]["rowText"]] = templst

    df = pd.DataFrame({key:pd.Series(value) for key, value in temp.items()})
    df["Date"] = pd.to_datetime(df["Date"])

    return df