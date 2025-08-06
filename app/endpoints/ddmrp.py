from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import sys
import os
import pandas as pd
import json


# Add the parent directories to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

# Import the existing function that reads from processed data
from logic.ddmrp_engine import calculate_ddmrp_plan

router = APIRouter()

OUTPUTS_DIR = "data/outputs"
PROCESSED_DIR = "data/processed"


def get_latest_analysis_data():
    """
    Get the latest analysis data from either outputs or processed directory.
    """
    try:
        # Try CSV output first
        output_path = os.path.join(OUTPUTS_DIR, "ddmrp_weekly_production_plan.csv")
        if os.path.exists(output_path):
            return pd.read_csv(output_path)

        # Fall back to processed directory
        processed_path = os.path.join(PROCESSED_DIR, "latest_analysis.xlsx")
        if os.path.exists(processed_path):
            return pd.read_excel(processed_path)

        return None
    except Exception as e:
        print(f"Error loading analysis data: {e}")
        return None


def get_available_skus_list():
    """Get list of available SKUs from processed data (renamed to avoid conflict)"""
    try:
        output_path = os.path.join(OUTPUTS_DIR, "ddmrp_weekly_production_plan.csv")
        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
            return df['SKU'].unique().tolist()
        return []
    except:
        return []


@router.get("/sku/{sku_id}")
async def get_sku_data(sku_id: str) -> Dict[str, Any]:
    """
    Get DDMRP data for a specific SKU from processed data.
    This fixes the original 'list' object has no attribute 'get' error.
    """
    try:
        # Call the existing function that reads from processed data
        result = calculate_ddmrp_plan(sku_id)

        if "error" in result:
            available_skus = get_available_skus_list()
            raise HTTPException(
                status_code=404,
                detail=f"SKU {sku_id} not found. Available SKUs: {available_skus}"
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving SKU data: {str(e)}")


@router.get("/dashboard")
async def get_dashboard_data() -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns summary data for all SKUs from processed data.
    This fixes the original dashboard error by using real data instead of hardcoded SKUs.
    """
    try:
        # Get list of available SKUs from processed data
        available_skus = get_available_skus_list()

        if not available_skus:
            return {"skus": []}

        dashboard_data = []

        for sku_id in available_skus:
            try:
                # Call the existing function for each SKU
                sku_data = calculate_ddmrp_plan(str(sku_id))

                if sku_data and "error" not in sku_data:
                    # Handle different field name variations
                    net_flow = sku_data.get("net_flow", sku_data.get("Net Flow", 0))
                    red_zone = sku_data.get("red_zone", sku_data.get("Red Zone", 0))
                    yellow_zone = sku_data.get("yellow_zone", sku_data.get("Yellow Zone", 0))
                    weekly_adu = sku_data.get("weekly_adu", sku_data.get("Weekly ADU", 0))
                    current_inventory = sku_data.get("inventory_(current_week)", sku_data.get("Inventory (Current Week)", 0))
                    recommended_production = sku_data.get("recommended_production", sku_data.get("Recommended Production", 0))
                    target_week = sku_data.get("target_production_week", sku_data.get("Target Production Week", ""))

                    # Determine status based on buffer zones
                    if net_flow <= red_zone:
                        status = "red"
                    elif net_flow <= red_zone + yellow_zone:
                        status = "yellow"
                    else:
                        status = "green"

                    dashboard_item = {
                        "sku": str(sku_id),
                        "status": status,
                        # Add all CSV columns with proper field name handling
                        "current_week": sku_data.get("current_week", sku_data.get("Current Week", "")),
                        "target_production_week": sku_data.get("target_production_week",
                                                               sku_data.get("Target Production Week", "")),
                        "weekly_adu": sku_data.get("weekly_adu", sku_data.get("Weekly ADU", 0)),
                        "cov": sku_data.get("cov", sku_data.get("CoV", 0)),
                        "red_base": sku_data.get("red_base", sku_data.get("Red Base", 0)),
                        "red_safety": sku_data.get("red_safety", sku_data.get("Red Safety", 0)),
                        "red_zone": sku_data.get("red_zone", sku_data.get("Red Zone", 0)),
                        "yellow_zone": sku_data.get("yellow_zone", sku_data.get("Yellow Zone", 0)),
                        "green_zone": sku_data.get("green_zone", sku_data.get("Green Zone", 0)),
                        "top_of_red": sku_data.get("top_of_red", sku_data.get("Top of Red", 0)),
                        "top_of_yellow": sku_data.get("top_of_yellow", sku_data.get("Top of Yellow", 0)),
                        "top_of_green": sku_data.get("top_of_green", sku_data.get("Top of Green", 0)),
                        "net_flow": net_flow,
                        "recommended_production": sku_data.get("recommended_production",
                                                               sku_data.get("Recommended Production", 0)),
                        "inventory_(current_week)": sku_data.get("inventory_(current_week)",
                                                                 sku_data.get("Inventory (Current Week)", 0)),
                        "on_order": sku_data.get("on_order", sku_data.get("On Order", 0)),
                        "qualified_demand": sku_data.get("qualified_demand", sku_data.get("Qualified Demand", 0)),

                        # Keep legacy field names for backward compatibility
                        "netFlow": net_flow,
                        "weeklyADU": sku_data.get("weekly_adu", sku_data.get("Weekly ADU", 0)),
                        "currentInventory": sku_data.get("inventory_(current_week)",
                                                         sku_data.get("Inventory (Current Week)", 0)),
                        "recommendedProduction": sku_data.get("recommended_production",
                                                              sku_data.get("Recommended Production", 0)),
                        "targetWeek": sku_data.get("target_production_week", sku_data.get("Target Production Week", ""))
                    }

                    dashboard_data.append(dashboard_item)

            except Exception as e:
                print(f"Error processing SKU {sku_id}: {e}")
                continue

        return {"skus": dashboard_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving dashboard data: {str(e)}")


@router.get("/dashboard/status/{status}")
async def get_dashboard_by_status(status: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns dashboard data filtered by status (red, yellow, green)
    """
    if status not in ["red", "yellow", "green"]:
        raise HTTPException(status_code=400, detail="Status must be 'red', 'yellow', or 'green'")

    # Get all dashboard data
    all_data = await get_dashboard_data()

    # Filter by status
    filtered_data = [item for item in all_data["skus"] if item["status"] == status]

    return {"skus": filtered_data}


@router.get("/available-skus")
async def get_available_skus_endpoint() -> Dict[str, List[str]]:
    """
    Returns list of all available SKUs from processed data
    """
    try:
        skus = get_available_skus_list()
        return {"skus": [str(sku) for sku in skus]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving available SKUs: {str(e)}")


@router.get("/summary")
async def get_analysis_summary() -> Dict[str, Any]:
    """
    Get summary statistics of the current analysis
    """
    try:
        df = get_latest_analysis_data()

        if df is None:
            return {"status": "no_data", "message": "No analysis data available"}

        # Handle different column name variations
        net_flow_col = 'net_flow' if 'net_flow' in df.columns else 'Net Flow'
        red_zone_col = 'red_zone' if 'red_zone' in df.columns else 'Red Zone'
        yellow_zone_col = 'yellow_zone' if 'yellow_zone' in df.columns else 'Yellow Zone'
        recommended_production_col = 'recommended_production' if 'recommended_production' in df.columns else 'Recommended Production'

        # Calculate summary statistics
        total_skus = len(df)
        red_count = len(df[df[net_flow_col] <= df[red_zone_col]])
        yellow_count = len(
            df[(df[net_flow_col] > df[red_zone_col]) & (df[net_flow_col] <= df[red_zone_col] + df[yellow_zone_col])])
        green_count = len(df[df[net_flow_col] > df[red_zone_col] + df[yellow_zone_col]])

        total_recommended_production = df[recommended_production_col].sum()
        average_net_flow = df[net_flow_col].mean()

        return {
            "status": "available",
            "total_skus": total_skus,
            "status_counts": {
                "red": red_count,
                "yellow": yellow_count,
                "green": green_count
            },
            "total_recommended_production": float(total_recommended_production),
            "average_net_flow": float(average_net_flow),
            "last_updated": os.path.getmtime(os.path.join(PROCESSED_DIR, "latest_analysis.xlsx")) if os.path.exists(
                os.path.join(PROCESSED_DIR, "latest_analysis.xlsx")) else None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving analysis summary: {str(e)}")


@router.get("/ddmrp/sku-details/{sku_id}")
def get_sku_details(sku_id: str):
    file_path = f"data/outputs/SKU_{sku_id}.xlsx"

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail={
            "error": "SKU not found",
            "message": f"No analysis data available for SKU {sku_id}"
        })

    try:
        xls = pd.ExcelFile(file_path)
        df_master = pd.read_excel(xls, sheet_name="Master Data")

        # Basic fields
        product_description = df_master["Product Desc"].iloc[0]
        moq = df_master["MOQ"].iloc[0]
        adu = df_master["ADU"].iloc[0]
        lead_time = df_master["Lead Time"].iloc[0]
        mrp_type = df_master["MRP Type"].iloc[0]

        # Buffer zones
        red_zone = df_master["Red Zone"].iloc[0]
        yellow_zone = df_master["Yellow Zone"].iloc[0]
        green_zone = df_master["Green Zone"].iloc[0]
        top_of_red = df_master["Top of Red"].iloc[0]
        top_of_yellow = df_master["Top of Yellow"].iloc[0]
        top_of_green = df_master["Top of Green"].iloc[0]

        # Historical
        historical_data = []
        for _, row in df_master.iterrows():
            if pd.notnull(row["Week"]):
                historical_data.append({
                    "week": row["Week"],
                    "inventory": row["Inventory"] if "Inventory" in row else None,
                    "quantitySold": row["Quantity Sold"] if "Quantity Sold" in row else None,
                    "productionOrders": row["Production Orders"] if "Production Orders" in row else None
                })

        return {
            "sku_id": sku_id,
            "Product Description": product_description,
            "Description": product_description,  # Optional alias
            "MOQ": moq,
            "ADU": adu,
            "Lead Time": lead_time,
            "Lead Time (days)": lead_time,
            "MRP Type": mrp_type,
            "Red Zone": red_zone,
            "Yellow Zone": yellow_zone,
            "Green Zone": green_zone,
            "Top of Red": top_of_red,
            "Top of Yellow": top_of_yellow,
            "Top of Green": top_of_green,
            "historical": historical_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "error": "Internal Server Error",
            "message": str(e)
        })