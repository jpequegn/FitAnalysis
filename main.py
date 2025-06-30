
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
from fitanalysis.loader import FitDataLoader
import os
import tempfile

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("index.html") as f:
        return f.read()

@app.post("/upload/")
async def upload_fit_file(file: UploadFile = File(...)):
    if not file.filename.endswith('.fit'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a .fit file.")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".fit") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        loader = FitDataLoader(tmp_path)
        df = loader.load()
        os.unlink(tmp_path)

        if df.empty:
            return {"timestamps": [], "power": [], "heart_rate": []}

        power_series = loader.get_power().dropna()
        hr_series = loader.get_heart_rate().dropna()

        return {
            "timestamps": df.index.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            "power": power_series.tolist(),
            "heart_rate": hr_series.tolist(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")
