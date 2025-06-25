from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse
from client import fetch_table_data, fetch_metadata
from cleaner import to_df
from file_manager import save_csv, load_csv, list_csvs, save_joined, update, delete
from joiner import leftjoin_df, innerjoin_df, rightjoin_df
import uvicorn

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
        path = save_csv(df, id)
        return FileResponse(path, media_type="text/csv", filename=f"{id}.csv")
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

@app.post("/join")
def join_csvs(table1: str = Form(...), table2: str = Form(...), key: str = Form("Date"), name: str = Form(...)):
    try:
        df1 = load_csv(f"./tmp/{table1}.csv")
        df2 = load_csv(f"./tmp/{table2}.csv")
        joined = leftjoin_df(df1, df2, key)
        path = save_csv(joined, f"{name}_joined")
        return FileResponse(path, media_type="text/csv", filename=f"{name}.csv")
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})

@app.get("/list")
def list_files():
    try:
        return list_csvs()
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})
    
@app.post("/view")
def view_files(id):
    try:
        df = load_csv(f"./tmp/{id}.csv")
        df["Date"] = df["Date"].astype(str)
        return JSONResponse(df.head(20).to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})

@app.get("/update")
def update_csv():
    n = update()

    for id in n:
        raw = fetch_table_data(id)
        df = to_df(raw)
        path = save_csv(df, id)
    
    return JSONResponse({"number of files updated: ": len(n)})

@app.post("/remove")
def remove_file(name):
    try:
        n = delete(name)
        return JSONResponse({"file deleted: ": f"{n}.csv"})
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)