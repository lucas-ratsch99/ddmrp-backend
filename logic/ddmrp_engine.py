import pandas as pd

def calculate_adu(sales_series, num_weeks):
    """Calculates Average Weekly Usage (ADU in weeks)"""
    return sales_series.sum() / num_weeks if num_weeks > 0 else 0

def classify_lead_time_factor(lead_time_weeks):
    if lead_time_weeks <= 2:      # Short Lead Time
        return 0.75               # 61–100%
    elif lead_time_weeks <= 4:    # Medium Lead Time
        return 0.5                # 41–60%
    else:                         # Long Lead Time
        return 0.3                # 20–40%

def classify_variability_factor(cov, all_covs, sku=None):
    """Use quantile-based classification across all SKUs to self-normalize."""
    if sku == "573602" or sku == 573602:
        return 1.5

    low_thresh = all_covs.quantile(0.33)
    high_thresh = all_covs.quantile(0.66)

    if cov <= low_thresh:
        return 0.3
    elif cov <= high_thresh:
        return 0.5
    else:
        return 0.8

def calculate_ddmrp_fields(df: pd.DataFrame,
                           moq: float,
                           lead_time_weeks: float,
                           daf: float = 1.0,
                           all_covs=None,
                           reference_week=None,
                           is_340: bool = False,
                           sku=None) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    if reference_week:
        df_hist = df[df['Week'] <= reference_week].copy()

    else:
        df_hist = df.copy()


    num_weeks = df_hist['Week'].nunique()
    weekly_adu = calculate_adu(df_hist['Quantity Sold'], num_weeks)
    weekly_std = df_hist['Quantity Sold'].std()
    cov = weekly_std / weekly_adu if weekly_adu > 0 else 0

    ltf = classify_lead_time_factor(lead_time_weeks)
    vf = classify_variability_factor(cov, all_covs, sku) if all_covs is not None else 0.5

    # Adjust ADU by DAF before calculating zones
    adjusted_adu = weekly_adu * daf

    # Buffer zones (DAF applied)
    yellow = adjusted_adu * lead_time_weeks
    # MOQ logic remains unchanged – apply DAF to MOQ comparison as well
    if is_340:
        adjusted_moq = max(moq, 4 * adjusted_adu)
    else:
        adjusted_moq = moq

    green = max(adjusted_adu * lead_time_weeks * ltf, adjusted_moq)
    red_base = adjusted_adu * lead_time_weeks * ltf
    red_safety = red_base * vf
    red_zone = red_base + red_safety

    df['Weekly ADU'] = weekly_adu  # store original ADU for reference
    df['Adjusted ADU'] = adjusted_adu  # store ADU with DAF applied
    df['ADU'] = adjusted_adu
    df['CoV'] = cov
    df['Lead Time Factor'] = ltf
    df['Variability Factor'] = vf
    df['DAF'] = daf
    df['Yellow Zone'] = yellow
    df['Green Zone'] = green
    df['Red Base'] = red_base
    df['Red Safety'] = red_safety
    df['Red Zone'] = red_zone
    # ✅ Always show debug regardless of branch

    return df


import pandas as pd
import os


def calculate_ddmrp_plan(sku_id: str):
    """
    Fixed version - returns a single dictionary for a SKU from the CSV summary file.
    """
    output_path = os.path.join("data", "outputs", "ddmrp_weekly_production_plan.csv")

    if not os.path.exists(output_path):
        return {"error": "Output plan file not found."}

    try:
        df = pd.read_csv(output_path)

        # Debug: Print available columns
        print(f"Available columns in CSV: {df.columns.tolist()}")

        # Filter by SKU ID - handle both string and numeric formats
        filtered = df[df["SKU"].astype(str) == str(sku_id)]

        if filtered.empty:
            # Try different matching approaches
            filtered = df[df["SKU"].astype(str).str.contains(str(sku_id), case=False, na=False)]

        if filtered.empty:
            available_skus = df["SKU"].unique().tolist()
            return {"error": f"No production plan found for SKU: {sku_id}. Available SKUs: {available_skus[:10]}"}

        # Take the first row (there should only be one per SKU in the summary file)
        row = filtered.iloc[0]

        # Convert to dictionary
        result = row.to_dict()

        # Convert numpy types to Python types for JSON serialization
        for key, value in result.items():
            if hasattr(value, 'item'):
                result[key] = value.item()
            elif pd.isna(value):
                result[key] = None

        return result

    except Exception as e:
        return {"error": f"Error processing SKU {sku_id}: {str(e)}"}


# Add this function to get the exact column names in the CSV
def debug_csv_structure():
    """Debug function to see the actual CSV structure"""
    output_path = os.path.join("data", "outputs", "ddmrp_weekly_production_plan.csv")

    if os.path.exists(output_path):
        df = pd.read_csv(output_path)
        print(f"CSV Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"First few rows:")
        print(df.head())
        return df
    else:
        print("CSV file not found")
        return None