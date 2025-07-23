from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the fixed endpoints that replace the existing ones
from app.endpoints.upload import router as upload_router
from app.endpoints.ddmrp import router as ddmrp_router

app = FastAPI(title="DDMRP API", version="1.0.0")

# CORS middleware - Allow frontend to access backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoints
@app.get("/")
def read_root():
    return {"message": "DDMRP API is running"}

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "Backend is running and ready to process files"}

# Add the fixed routes
app.include_router(upload_router, prefix="/upload", tags=["upload"])
app.include_router(ddmrp_router, prefix="/ddmrp", tags=["ddmrp"])

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os

# Add this new endpoint to your existing FastAPI app
@app.get("/ddmrp/download-csv")
async def download_csv():
    """Download the DDMRP weekly production plan CSV file"""
    csv_file_path = "data/outputs/ddmrp_weekly_production_plan.csv"

    if not os.path.exists(csv_file_path):
        raise HTTPException(status_code=404, detail="CSV file not found")

    return FileResponse(
        path=csv_file_path,
        filename="ddmrp_weekly_production_plan.csv",
        media_type="text/csv"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)