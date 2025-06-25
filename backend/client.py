from urllib.request import Request,urlopen
import json
import pandas as pd
import requests

BASE_URL = "https://tablebuilder.singstat.gov.sg/api/"
hdr = {'User-Agent': 'Mozilla/5.0', "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8"}

def fetch_table_data(id: str, params: dict = hdr):
    url = f"{BASE_URL}table/tabledata/{id}"
    request = Request(url,headers=hdr)
    data = urlopen(request)
    return json.load(data)

def fetch_metadata(id: str):
    url = f"{BASE_URL}table/metadata/{id}"
    request = Request(url,headers=hdr)
    data = urlopen(request)
    return json.load(data)["Data"]
