import pandas as pd
import os


def load_and_clean_data(input_path):
    # Load Excel files
    df_historical = pd.read_excel(os.path.join(input_path, "DDMRP Project Data.xlsm"), sheet_name="Historical Data", skiprows=4)
    df_prod_plan = pd.read_excel(os.path.join(input_path, "DDMRP Project Data.xlsm"), sheet_name="Production Plan", skiprows=4)
    df_moq = pd.read_excel(os.path.join(input_path, "Artikel & Materialien FGR+.XLSX"), sheet_name="Artikel FGR+", skiprows=1)
    df_stock_on_hand = pd.read_excel(os.path.join(input_path, "DDMRP Project Data.xlsm"), sheet_name="Stock On Hand", skiprows=4)
    vorschau_files = [
        f for f in os.listdir(input_path)
        if f.lower().startswith("vorschauliste")
           and f.lower().endswith((".xls", ".xlsx", ".xlsm"))
    ]
    if not vorschau_files:
        raise FileNotFoundError("No Vorschauliste file found in input directory")

    df_vorschau = pd.read_excel(
        os.path.join(input_path, vorschau_files[0]),
        sheet_name="Vorschauliste"
    )

    df_vorschau.columns = df_vorschau.columns.str.strip()

    # --- Sales History ---
    df_sales = df_historical[df_historical["Key Figure"] == "GSCRM Actual Sales and Unconstrained Demand"].copy()
    df_sales = df_sales.melt(
        id_vars=["Product ID", "Product Desc", "MRP Type Indicator"],
        var_name="Week",
        value_name="Quantity Sold"
    )
    df_sales = df_sales[df_sales["Week"].str.contains("W")]  # Keep only week columns
    df_sales["MRP Type"] = df_sales["MRP Type Indicator"].map({"X0": "MTS", "X7": "MTO"})
    df_sales = df_sales[["Product ID", "Product Desc", "MRP Type", "Week", "Quantity Sold"]]

    # --- Inventory ---
    # --- Inventory history from Historical Data ---
    # Historical projected stock (multiple week columns) for each SKU
    df_proj_inv = df_historical[df_historical["Key Figure"] == "GSCRM Projected Stock (Unconstrained Demand)"].copy()
    df_proj_inv = df_proj_inv.melt(
        id_vars=["Product ID", "Product Desc", "MRP Type Indicator"],
        var_name="Week",
        value_name="Inventory"
    )
    df_proj_inv = df_proj_inv[df_proj_inv["Week"].str.contains("W")]  # only week columns
    df_proj_inv["MRP Type"] = df_proj_inv["MRP Type Indicator"].map({"X0": "MTS", "X7": "MTO"})
    df_proj_inv = df_proj_inv[["Product ID", "Product Desc", "MRP Type", "Week", "Inventory"]]
    df_proj_inv["source"] = "historical"

    # --- Stock on Hand (current week only) ---
    df_soh_inv = df_stock_on_hand[df_stock_on_hand["Key Figure"] == "Stock on Hand"].copy()
    df_soh_inv = df_soh_inv.melt(
        id_vars=["Product ID", "Product Desc", "MRP Type Indicator"],
        var_name="Week",
        value_name="Inventory"
    )
    df_soh_inv = df_soh_inv[df_soh_inv["Week"].str.contains("W")]
    df_soh_inv["MRP Type"] = df_soh_inv["MRP Type Indicator"].map({"X0": "MTS", "X7": "MTO"})
    df_soh_inv = df_soh_inv[["Product ID", "Product Desc", "MRP Type", "Week", "Inventory"]]
    df_soh_inv["source"] = "stock_on_hand"

    # Combine historical and stock‑on‑hand values; let stock‑on‑hand override if both exist
    df_inv_combined = pd.concat([df_proj_inv, df_soh_inv], ignore_index=True)
    df_inv_combined = df_inv_combined.sort_values(by="source")  # historical first, then stock_on_hand
    df_inv_combined = df_inv_combined.drop_duplicates(subset=["Product ID", "Week"], keep="last")
    df_inv_combined = df_inv_combined.drop(columns=["source"])
    df_inv = df_inv_combined[["Product ID", "Product Desc", "MRP Type", "Week", "Inventory"]]

    # --- Production Orders ---
    df_orders = df_prod_plan[df_prod_plan["Key Figure"] == "Open Production Orders (Adjusted by PLT)"].copy()
    df_orders = df_orders.melt(
        id_vars=["Product ID", "Product Desc", "MRP Type Indicator"],
        var_name="Week",
        value_name="Production Orders"
    )
    df_orders = df_orders[df_orders["Week"].str.contains("W")]
    df_orders["MRP Type"] = df_orders["MRP Type Indicator"].map({"X0": "MTS", "X7": "MTO"})
    df_orders = df_orders[["Product ID", "Product Desc", "MRP Type", "Week", "Production Orders"]]

    # --- MOQ ---
    df_moq_clean = df_moq[["Material", "Material short text", "Minimum batch size", "Rounding value", "Lead Time", "DAF"]].copy()
    df_moq_clean = df_moq_clean.rename(columns={
        "Material": "Product ID",
        "Material short text": "Product Desc",
        "Minimum batch size": "MOQ",
        "Rounding value": "Rounding Value",
        "Lead Time": "Lead Time",
        "DAF": "DAF"
    })

    # Flag 340 mm SKUs
    df_moq_clean["Is_340"] = df_moq_clean["Product Desc"].str.contains("340", case=False, na=False)

    # Convert lead time and DAF to numeric values (default to sensible values if blank).
    df_moq_clean["Lead Time"] = pd.to_numeric(df_moq_clean["Lead Time"], errors="coerce").fillna(3)
    df_moq_clean["DAF"] = pd.to_numeric(df_moq_clean["DAF"], errors="coerce").fillna(1.0)

    # --- Open Sales Orders ---
    # Filter to only FGR materials
    df_vorschau = df_vorschau[df_vorschau["Materialkurztext"].str.contains("FGR", na=False)]

    df_sales_orders = df_vorschau[[
        "Material", "Materialkurztext", "Bestelldat", "WL.Datum", "KumAuMenge", "OffnEintMg"
    ]].copy()

    df_sales_orders = df_sales_orders.rename(columns={
        "Material": "Product ID",
        "Materialkurztext": "Product Desc",
        "Bestelldat": "Order Date",
        "WL.Datum": "Due Date",
        "KumAuMenge": "Ordered Qty",
        "OffnEintMg": "Open Qty"
    })

    def clean_product_id(pid):
        # Ensure it's a string to use string operations
        pid = str(pid)
        # Remove anything after the dash (and the dash itself)
        pid = pid.split('-')[0]
        # Remove leading zeros
        pid = pid.lstrip('0')
        # Convert to int
        return int(pid) if pid else None

    df_inv["Product ID"] = df_inv["Product ID"].apply(clean_product_id)
    df_sales["Product ID"] = df_sales["Product ID"].apply(clean_product_id)
    df_orders["Product ID"] = df_orders["Product ID"].apply(clean_product_id)

    return df_sales, df_inv, df_orders, df_moq_clean, df_sales_orders