from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Optional
import shutil
import os
import pandas as pd
import sys
import logging
import traceback
import glob, os


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

INPUTS_DIR = "data/inputs"
OUTPUTS_DIR = "data/outputs"
PROCESSED_DIR = "data/processed"

# CRITICAL: Ensure directories exist at module load time
for directory in [INPUTS_DIR, OUTPUTS_DIR, PROCESSED_DIR]:
    os.makedirs(directory, exist_ok=True)


def safe_run_analysis():
    """
    Safely run DDMRP analysis with proper error handling to prevent backend crashes
    """
    try:
        # Force create directories before starting
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        os.makedirs(INPUTS_DIR, exist_ok=True)

        logger.info("🚀 Starting DDMRP analysis...")

        # Import the analysis function only when needed to avoid import errors
        from app.old_main import main as run_ddmrp_analysis

        # Run the analysis
        results = run_ddmrp_analysis()

        logger.info(f"✅ Analysis completed successfully. Processed {len(results)} SKUs.")
        return results

    except ImportError as e:
        logger.error(f"❌ Import error in analysis function: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Analysis import error: {str(e)}")

    except FileNotFoundError as e:
        logger.error(f"❌ File not found during analysis: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Required file missing: {str(e)}")

    except Exception as e:
        logger.error(f"❌ Analysis failed with error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")


@router.post("/multiple-files")
async def upload_multiple_files(
        ddmrp_project_data: Optional[UploadFile] = File(None),
        vorschauliste: Optional[UploadFile] = File(None)
):
    """
    Upload multiple files for DDMRP analysis with safe error handling.
    """
    try:
        uploaded_files = []
        logger.info(
            f"📤 Upload request received. DDMRP: {ddmrp_project_data is not None}, Vorschau: {vorschauliste is not None}")

        # Handle DDMRP Project Data file
        if ddmrp_project_data:
            if not ddmrp_project_data.filename or not ddmrp_project_data.filename.endswith(('.xlsx', '.xlsm')):
                raise HTTPException(status_code=400, detail="DDMRP Project Data must be Excel file (.xlsx or .xlsm)")

            file_path = os.path.join(INPUTS_DIR, "DDMRP Project Data.xlsx")
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(ddmrp_project_data.file, buffer)
            uploaded_files.append("DDMRP Project Data.xlsx")
            logger.info(f"✅ Saved DDMRP Project Data to {file_path}")

        # Handle Vorschauliste file
        if vorschauliste:
            if not vorschauliste.filename or not vorschauliste.filename.endswith(('.xlsx', '.xlsm')):
                raise HTTPException(status_code=400, detail="Vorschauliste must be Excel file (.xlsx or .xlsm)")

            # Use consistent filename that matches what clean_inputs.py expects
            extension = ".xlsm" if vorschauliste.filename and vorschauliste.filename.endswith('.xlsm') else ".xlsx"
            file_path = os.path.join(INPUTS_DIR, f"Vorschauliste KW30 bis 01.08.2025{extension}")
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(vorschauliste.file, buffer)
            uploaded_files.append(f"Vorschauliste KW30 bis 01.08.2025{extension}")
            logger.info(f"✅ Saved Vorschauliste to {file_path}")

        # Check if all three required files are present
        artikel_file = os.path.join(INPUTS_DIR, "Artikel & Materialien FGR+.XLSX")
        ddmrp_file = os.path.join(INPUTS_DIR, "DDMRP Project Data.xlsx")
        vorschau_files = [f for f in os.listdir(INPUTS_DIR) if f.startswith("Vorschauliste")]

        logger.info(
            f"📋 File check - Artikel: {os.path.exists(artikel_file)}, DDMRP: {os.path.exists(ddmrp_file)}, Vorschau: {len(vorschau_files) > 0}")

        # Check if all three files exist
        if (os.path.exists(ddmrp_file) and
                len(vorschau_files) > 0 and
                os.path.exists(artikel_file)):

            logger.info("🎯 All files present, starting analysis...")

            # Run DDMRP analysis with safe error handling
            try:
                processed_results = safe_run_analysis()

                return {
                    "status": "success",
                    "uploaded_files": uploaded_files,
                    "processed_skus": len(processed_results),
                    "message": f"Files uploaded and analyzed successfully. {len(processed_results)} SKUs processed."
                }

            except HTTPException:
                # Re-raise HTTP exceptions
                raise
            except Exception as e:
                logger.error(f"❌ Unexpected error during analysis: {str(e)}")
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

            logger.info(f"⏳ Partial upload complete. Missing: {missing}")

            return {
                "status": "partial_upload",
                "uploaded_files": uploaded_files,
                "message": f"Files uploaded successfully. Waiting for: {', '.join(missing)}. Analysis will run automatically when all files are present."
            }

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"❌ Upload error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error uploading files: {str(e)}")


@router.post("/trigger-analysis")
async def trigger_analysis():
    """
    Manual analysis trigger endpoint - isolated from upload to prevent crashes
    """
    try:
        logger.info("🚀 Analysis triggered manually...")

        # Check if all files exist
        artikel_exists = os.path.exists(os.path.join(INPUTS_DIR, "Artikel & Materialien FGR+.XLSX"))
        ddmrp_exists = os.path.exists(os.path.join(INPUTS_DIR, "DDMRP Project Data.xlsx"))
        vorschau_files = [f for f in os.listdir(INPUTS_DIR) if f.startswith("Vorschauliste")]

        if not (artikel_exists and ddmrp_exists and len(vorschau_files) > 0):
            missing = []
            if not artikel_exists: missing.append("Artikel & Materialien FGR+")
            if not ddmrp_exists: missing.append("DDMRP Project Data")
            if not vorschau_files: missing.append("Vorschauliste")

            return {
                "status": "error",
                "message": f"Missing required files: {', '.join(missing)}"
            }

        # Import and run analysis safely
        try:
            from old_main import main as run_analysis  # Update this import path
            results = run_analysis()
            return {
                "status": "success",
                "message": f"Analysis completed: {len(results)} SKUs processed",
                "processed_skus": len(results),
                "results": results
            }
        except ImportError as e:
            logger.error(f"Analysis import failed: {e}")
            return {
                "status": "error",
                "message": f"Analysis function not available: {e}"
            }
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return {
                "status": "error",
                "message": f"Analysis error: {e}"
            }

    except Exception as e:
        logger.error(f"❌ Analysis trigger error: {str(e)}")
        return {"status": "error", "message": str(e)}

@router.get("/analysis-status")
async def get_analysis_status():
    """
    Check if analysis data is available and return basic info.
    """
    try:
        output_path = os.path.join(OUTPUTS_DIR, "ddmrp_weekly_production_plan.csv")

        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
            file_size = os.path.getsize(output_path)
            file_modified = os.path.getmtime(output_path)

            logger.info(f"📊 Analysis status: {len(df)} SKUs, file size: {file_size} bytes")

            return {
                "status": "available",
                "sku_count": len(df),
                "last_updated": file_modified,
                "file_size": file_size
            }
        else:
            logger.info("📊 Analysis status: No data file found")
            return {
                "status": "not_available",
                "message": "No analysis data found. Please run DDMRP analysis after uploading files."
            }
    except Exception as e:
        logger.error(f"❌ Error checking analysis status: {str(e)}")
        return {
            "status": "error",
            "message": f"Error checking analysis status: {str(e)}"
        }


@router.post("/excel")
async def upload_excel(file: UploadFile = File(...)):
    """
    Legacy single file upload endpoint for backward compatibility.
    """
    try:
        if not file.filename or not file.filename.endswith(('.xlsx', '.xlsm')):
            raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xlsm)")

        # Save the file
        filename = file.filename or "uploaded_file.xlsx"
        file_path = os.path.join(INPUTS_DIR, filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        logger.info(f"✅ Legacy upload: {file.filename}")

        # Try to run analysis if all required files are available
        try:
            processed_results = safe_run_analysis()
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
        logger.error(f"❌ Legacy upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")