from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse
from client import fetch_table_data, fetch_metadata
from cleaner import to_df
from file_manager import save_csv, load_csv
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
    raw = fetch_table_data(id)
    df = to_df(raw)
    path = save_csv(df, id)
    return FileResponse(path, media_type="text/csv", filename=f"{id}.csv")

@app.post("preview")
def preview_table(id):
    raw = fetch_table_data(id)
    df = to_df(raw)
    return JSONResponse(df.head(20).to_dict(orient="records"))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)