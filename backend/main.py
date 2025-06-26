from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse
from client import fetch_table_data, fetch_metadata
from cleaner import to_df
from file_manager import save_csv, load_csv, list_csvs, save_joined, update, delete
from joiner import leftjoin_df, innerjoin_df, rightjoin_df
import uvicorn
from mongo import store_entry, del_entry, update_entry

app = FastAPI()

@app.get("/")
def root():
    return {"message": "wsg"}

@app.post("/tables")
def list_tables(id):
    return fetch_metadata(id)

@app.post("/query")
def query_table(id):
    try:
        raw = fetch_table_data(id)
        df = to_df(raw)
        store_entry(df, id)
        return JSONResponse(status_code=200, content = {"Success": f"Entry {id} found and stored."})
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})

@app.post("/preview")
def preview_table(id):
    try:
        raw = fetch_table_data(id)
        df = to_df(raw)
        df["Date"] = df["Date"].astype(str)
        return JSONResponse(df.head(20).to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})

@app.post("/innerjoin")
def join_csvs_inner(table1: str = Form(...), table2: str = Form(...), key: str = Form("Date"), name: str = Form(...)):
    try:
        df1 = load_csv(f"./tmp/{table1}.csv")
        df2 = load_csv(f"./tmp/{table2}.csv")
        joined = innerjoin_df(df1, df2, key)
        path = save_csv(joined, f"{name}_joined")
        return FileResponse(path, media_type="text/csv", filename=f"{name}.csv")
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})
    
@app.post("/leftjoin")
def join_csvs_left(table1: str = Form(...), table2: str = Form(...), key: str = Form("Date"), name: str = Form(...)):
    try:
        df1 = load_csv(f"./tmp/{table1}.csv")
        df2 = load_csv(f"./tmp/{table2}.csv")
        joined = leftjoin_df(df1, df2, key)
        path = save_csv(joined, f"{name}_joined")
        return FileResponse(path, media_type="text/csv", filename=f"{name}.csv")
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})
    
@app.post("/rightjoin")
def join_csvs_right(table1: str = Form(...), table2: str = Form(...), key: str = Form("Date"), name: str = Form(...)):
    try:
        df1 = load_csv(f"./tmp/{table1}.csv")
        df2 = load_csv(f"./tmp/{table2}.csv")
        joined = rightjoin_df(df1, df2, key)
        path = save_csv(joined, f"{name}_joined")
        return FileResponse(path, media_type="text/csv", filename=f"{name}.csv")
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})

@app.post("/update")
def update_csv(id):
    try:
        update_entry(id)
        return JSONResponse(status_code=200, content={"updated: ": f"{id}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error" : str(e)})

@app.post("/remove")
def remove_file(id):
    try:
        del_entry(id)
        return JSONResponse({"file deleted: ": f"{id}"})
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
