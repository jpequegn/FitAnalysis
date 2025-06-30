from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
import pandas as pd
from fitanalysis.loader import FitDataLoader
import io

app = FastAPI()

@app.post("/upload/")
async def upload_fit_file(file: UploadFile = File(...)):
    if not file.filename.endswith('.fit'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a .fit file.")
    
    try:
        # Use an in-memory file
        content = await file.read()
        file_stream = io.BytesIO(content)
        
        # Since FitDataLoader expects a file path, we need to save it temporarily
        # For a more robust solution, especially with larger files or higher traffic,
        # consider saving to a temporary file or adapting FitDataLoader.
        temp_file_path = f"/tmp/{file.filename}"
        with open(temp_file_path, "wb") as f:
            f.write(content)

        loader = FitDataLoader(temp_file_path)
        df = loader.load()
        
        # Convert DataFrame to JSON
        return df.to_json(orient='split')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")

@app.get("/")
async def main():
    content = """
<body>
<form action="/upload/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)
