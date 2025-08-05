from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from client import fetch_table_data
from cleaner import to_df
import certifi

database = "test"
uri = "mongodb+srv://kaslisk:seeyuh@cluster0.vk5ocij.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server

def store_entry(df: pd.DataFrame, id):
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    db = client[f"{database}"]
    collection = db[id]
    if id in db.list_collection_names():
        client.close()
        raise Exception("db exists. please use update")
    temp = df.to_dict()
    n = len(df)
    tmp = list()

    for i in range(0, n):
        d = dict()
        for k, v in temp.items():
            d[k] = list(v.items())[i][1]
        d["_id"] = d["Date"].strftime("%Y-%m-%d")
        tmp.append(d)
    collection.insert_many(tmp)

    client.close()
    return

def del_entry(id):
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    db = client[f"{database}"]
    if id not in db.list_collection_names():
        client.close()
        raise Exception("db does not exist.")
    else:
        db[f"{id}"].drop()
        client.close()

def update_entry(id):
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    db = client[f"{database}"]
    if id not in db.list_collection_names():
        client.close()
        raise Exception("db does not exist.")
    else:
        del_entry(id)
        store_entry(to_df(fetch_table_data(id)), id)
        client.close()

def view(output=False):
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    db = client[f"{database}"]
    out = db.list_collection_names()
    if not output:
        if len(out) == 0:
            client.close()
            return "no results"
        else:
            client.close()
            return out
    else:
        return out

def wipe():
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        db = client[f"{database}"]
        print(type(db.list_collection_names()))
        for item in db.list_collection_names():
            db[f"{item}"].drop()
        client.close()
    except Exception as e:
        print(str(e))


def join(col_a, col_b, name):
    client = MongoClient(uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
    client.admin.command('ping')
    db = client[f"{database}"]

    pipeline = [
        {
            "$lookup": {
                "from": col_b,
                "localField":"_id",
                "foreignField": "_id",
                "as": "temp"
            }
        },
        {
            "$unwind": "$temp"
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": ["$temp", "$$ROOT"]
                }
            }
        },
        {
            "$project":{
                "temp": 0
            }
        },
        {
            "$merge": {
                "into": name,
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        }
    ]

    db[col_a].aggregate(pipeline)
    client.close()
    return
 