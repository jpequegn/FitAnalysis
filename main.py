from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse
import pandas as pd
from fitanalysis.loader import FitDataLoader
from fitanalysis.config import get_config, setup_logging
import io
import tempfile
import os

# Initialize configuration and logging
config = get_config()
setup_logging(config)

app = FastAPI()

@app.post("/upload/")
async def upload_fit_file(file: UploadFile = File(...)):
    # Check file extension against configured allowed extensions
    if not any(file.filename.endswith(ext) for ext in config.web.allowed_extensions):
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid file type. Please upload a file with one of these extensions: {', '.join(config.web.allowed_extensions)}"
        )
    
    # Check file size
    if file.size and file.size > config.web.max_file_size:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {config.web.max_file_size} bytes."
        )
    
    try:
        # Use an in-memory file
        content = await file.read()
        
        # Create a temporary file with proper cross-platform handling
        temp_dir = config.web.temp_dir
        with tempfile.NamedTemporaryFile(delete=False, suffix='.fit', dir=temp_dir) as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name

        try:
            loader = FitDataLoader(temp_file_path)
            df = loader.load()
            
            # Convert DataFrame to JSON
            return df.to_json(orient='split')
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

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
