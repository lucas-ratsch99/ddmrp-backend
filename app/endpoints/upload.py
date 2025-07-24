from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Optional
import shutil
import os
import pandas as pd
import sys

# Add the parent directories to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

# Import your existing analysis main function
from app.old_main import main as run_ddmrp_analysis

router = APIRouter()

UPLOAD_DIR = "data/inputs"
OUTPUTS_DIR = "data/outputs"
PROCESSED_DIR = "data/processed"

# Ensure directories exist
for directory in [UPLOAD_DIR, OUTPUTS_DIR, PROCESSED_DIR]:
    os.makedirs(directory, exist_ok=True)


@router.post("/multiple-files")
async def upload_multiple_files(
        ddmrp_project_data: Optional[UploadFile] = File(None),
        vorschauliste: Optional[UploadFile] = File(None)
):
    """
    Upload multiple files for DDMRP analysis.
    The Artikel & Materialien file is expected to be permanently in the system.
    """
    try:
        uploaded_files = []

        # Handle DDMRP Project Data file
        if ddmrp_project_data:
            if not ddmrp_project_data.filename.endswith(('.xlsx', '.xlsm')):
                raise HTTPException(status_code=400, detail="DDMRP Project Data must be Excel file (.xlsx or .xlsm)")

            file_path = os.path.join(UPLOAD_DIR, "DDMRP Project Data.xlsx")
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(ddmrp_project_data.file, buffer)
            uploaded_files.append("DDMRP Project Data.xlsx")

        # Handle Vorschauliste file
        if vorschauliste:
            if not vorschauliste.filename.endswith(('.xlsx', '.xlsm')):
                raise HTTPException(status_code=400, detail="Vorschauliste must be Excel file (.xlsx or .xlsm)")

            # Save with a consistent name but preserve original extension
            extension = ".xlsm" if vorschauliste.filename.endswith('.xlsm') else ".xlsx"
            file_path = os.path.join(UPLOAD_DIR, f"Vorschauliste KW30 bis 08.08.2025{extension}")
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(vorschauliste.file, buffer)
            uploaded_files.append(f"Vorschauliste KW30 bis 08.08.2025{extension}")

        # MOVED OUTSIDE: Create directories and check if ALL required files are present
        for directory in [UPLOAD_DIR, OUTPUTS_DIR, PROCESSED_DIR]:
            os.makedirs(directory, exist_ok=True)

        artikel_file = os.path.join(UPLOAD_DIR, "Artikel & Materialien FGR+.XLSX")
        ddmrp_file = os.path.join(UPLOAD_DIR, "DDMRP Project Data.xlsx")
        vorschau_files = [f for f in os.listdir(UPLOAD_DIR) if f.startswith("Vorschauliste")]

        # Check if all three files exist
        if (os.path.exists(ddmrp_file) and
                len(vorschau_files) > 0 and
                os.path.exists(artikel_file)):

            # Run DDMRP analysis with complete dataset
            try:
                processed_results = run_ddmrp_analysis()
                return {
                    "status": "success",
                    "uploaded_files": uploaded_files,
                    "processed_skus": len(processed_results),
                    "message": f"Files uploaded and analyzed successfully. {len(processed_results)} SKUs processed."
                }
            except Exception as e:
                return {
                    "status": "partial_success",
                    "uploaded_files": uploaded_files,
                    "message": f"Files uploaded successfully, but analysis failed: {str(e)}"
                }
        else:
            # Not all files present yet - just return upload success
            missing = []
            if not os.path.exists(ddmrp_file): missing.append("DDMRP Project Data")
            if len(vorschau_files) == 0: missing.append("Vorschauliste")
            if not os.path.exists(artikel_file): missing.append("Artikel & Materialien")

            return {
                "status": "partial_upload",
                "uploaded_files": uploaded_files,
                "message": f"Files uploaded successfully. Waiting for: {', '.join(missing)}. Analysis will run automatically when all files are present."
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading files: {str(e)}")

@router.post("/excel")
async def upload_excel(file: UploadFile = File(...)):
    """
    Legacy single file upload endpoint for backward compatibility.
    """
    try:
        if not file.filename.endswith(('.xlsx', '.xlsm')):
            raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xlsm)")

        # Save the file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Try to run analysis if all required files are available
        try:
            processed_results = run_ddmrp_analysis()
            return {
                "status": "success",
                "filename": file.filename,
                "processed_skus": len(processed_results),
                "message": f"File uploaded and analyzed successfully. {len(processed_results)} SKUs processed."
            }
        except Exception as e:
            return {
                "status": "partial_success",
                "filename": file.filename,
                "message": f"File uploaded successfully, but analysis failed: {str(e)}"
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/analysis-status")
async def get_analysis_status():
    """
    Check if analysis data is available and return basic info.
    """
    try:
        output_path = os.path.join("data/outputs", "ddmrp_weekly_production_plan.csv")

        if os.path.exists(output_path):
            import pandas as pd
            df = pd.read_csv(output_path)
            return {
                "status": "available",
                "sku_count": len(df),
                "last_updated": os.path.getmtime(output_path)
            }
        else:
            return {
                "status": "not_available",
                "message": "No analysis data found. Please run DDMRP analysis after uploading files."
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error checking analysis status: {str(e)}"
        }