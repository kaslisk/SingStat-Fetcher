from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse
from client import fetch_table_data, fetch_metadata
from cleaner import to_df
from file_manager import save_csv, load_csv, list_csvs, save_joined, update, delete
import uvicorn
from mongo import store_entry, del_entry, update_entry, view, join

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

@app.post("/update")
def update_csv(id):
    try:
        update_entry(id)
        return JSONResponse(status_code=200, content={"updated: ": f"{id}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error" : str(e)})
    
@app.post("/join")
def join_cols(a, b, c):
    try:
        join(a, b, c)
        return JSONResponse({"joined collections": f"{a}, {b}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/remove")
def remove_file(id):
    try:
        del_entry(id)
        return JSONResponse({"file deleted: ": f"{id}"})
    except Exception as e:
        return JSONResponse(status_code=500, content = {"error": str(e)})
    
@app.get("/viewcol")
def viewcol():
    return JSONResponse(status_code=200, content={"content" : view(False)})

@app.get("/wipe")
def wipe():
    try:
        wipe()
        return JSONResponse(status_code=200, content={"collectoin wiped": "!"})
    except Exception:
        return JSONResponse(status_code=500, content={"error" : "idk"})

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
