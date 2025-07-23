import os
import pandas as pd
from cleaning.clean_inputs import load_and_clean_data

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUTS_DIR = os.path.join(BASE_DIR, "data", "inputs")
OUTPUTS_DIR = os.path.join(BASE_DIR, "data", "outputs")
all_results = []


def main():
    print(f"\n📁 Base directory: {BASE_DIR}")
    print(f"📦 Looking in folder: {INPUTS_DIR}")

    print("✅ Files found in 'data/inputs':")
    for f in os.listdir(INPUTS_DIR):
        print(f)

    df_sales, df_inv, df_orders, df_moq, df_sales_orders = load_and_clean_data(input_path=INPUTS_DIR)

    # Step 1: Get SKU sets from both dataframes
    moq_skus = set(df_moq["Product ID"].dropna().unique())
    transaction_skus = set(df_sales["Product ID"].dropna().unique()) | \
                       set(df_inv["Product ID"].dropna().unique()) | \
                       set(df_orders["Product ID"].dropna().unique())

    # Step 2: Identify missing SKUs
    missing_skus = sorted(moq_skus - transaction_skus)

    # Step 3: Report
    print(f"\n🎯 Total SKUs in 'Artikel & Materialien FGR+': {len(moq_skus)}")
    print(f"📦 SKUs found in 'DDMRP Project Data': {len(transaction_skus)}")
    print(f"❌ Missing SKUs (in MOQ file but not in project data): {len(missing_skus)}")
    print("🧾 Missing SKU IDs:")
    for sku in missing_skus:
        print(f" - {sku}")

    # --- Compare SKUs between Vorschauliste and other data sources ---
    existing_skus = set(df_sales["Product ID"].dropna().unique()) | \
                    set(df_inv["Product ID"].dropna().unique()) | \
                    set(df_orders["Product ID"].dropna().unique()) | \
                    set(df_moq["Product ID"].dropna().unique())

    vorschau_skus = set(df_sales_orders["Product ID"].dropna().unique())

    matched_skus = vorschau_skus & existing_skus
    new_skus = vorschau_skus - existing_skus

    print(f"\n🔁 Matched SKUs in Vorschauliste and existing data: {len(matched_skus)}")
    print(sorted(matched_skus))

    print(f"\n➕ New SKUs found only in Vorschauliste: {len(new_skus)}")
    print(sorted(new_skus))

    unique_skus = df_sales["Product ID"].unique()
    print(f"🧮 Unique SKUs found: {len(unique_skus)}")

    from logic.ddmrp_engine import calculate_ddmrp_fields

    all_covs_data = df_sales.groupby("Product ID")["Quantity Sold"].std() / \
                    df_sales.groupby("Product ID")["Quantity Sold"].mean()


    for sku in unique_skus:
        sku_sales = df_sales[df_sales["Product ID"] == sku]
        sku_inv = df_inv[df_inv["Product ID"] == sku]
        sku_orders = df_orders[df_orders["Product ID"] == sku]
        sku_moq = df_moq[df_moq["Product ID"] == sku]
        sku_sales_orders = df_sales_orders[df_sales_orders["Product ID"] == sku]

        # Merge data
        master_df = sku_sales.copy()
        master_df = master_df.merge(sku_inv, on=["Product ID", "Product Desc", "MRP Type", "Week"], how="outer")
        master_df = master_df.merge(sku_orders, on=["Product ID", "Product Desc", "MRP Type", "Week"], how="outer")

        # Skip if not MTS
        if master_df['MRP Type'].nunique() == 1 and master_df['MRP Type'].iloc[0] != "MTS":
            continue

        # Clean week formatting for sort
        def reorder_week(week_str):
            if isinstance(week_str, str) and week_str.startswith("W"):
                parts = week_str.split()
                if len(parts) == 2:
                    return f"{parts[1]} {parts[0]}"
            return week_str

        master_df["Week"] = master_df["Week"].apply(reorder_week)
        master_df = master_df.sort_values(by="Week")

        # Add MOQ/Lead Time to each row
        moq = sku_moq["MOQ"].iloc[0] if not sku_moq.empty else 0
        lt_weeks = sku_moq["Lead Time"].iloc[0] if not sku_moq.empty else 2
        is_340 = sku_moq["Is_340"].iloc[0] if not sku_moq.empty and "Is_340" in sku_moq.columns else False
        rounding_value = sku_moq["Rounding Value"].iloc[0] if not sku_moq.empty and "Rounding Value" in sku_moq.columns else None
        master_df["MOQ"] = moq
        master_df["Lead Time"] = lt_weeks

        # Calculate DDMRP fields
        # Add Net Flow (On-Hand + On-Order - Qualified Demand)
        master_df['Inventory'] = pd.to_numeric(master_df['Inventory'], errors='coerce').fillna(0)
        master_df['Production Orders'] = pd.to_numeric(master_df['Production Orders'], errors='coerce').fillna(0)

        from logic.netflow import calculate_net_flow, calculate_qualified_demand

        # Ensure the dataframe is sorted
        master_df = master_df.sort_values("Week")

        # Get the latest week
        # Get the most recent week with available inventory data (non-null, non-zero)
        inventory_weeks = master_df[master_df["Inventory"] > 0]["Week"].dropna().unique()
        if len(inventory_weeks) == 0:
            print(f"⚠️ No inventory data found for SKU {sku}, skipping.")
            continue

        # Sort weeks and determine "current" (last week with inventory) and "next" (first future week)
        sorted_weeks = sorted(master_df["Week"].dropna().unique())
        current_week = sorted(inventory_weeks)[-1]

        master_df = calculate_ddmrp_fields(
            df=master_df,
            moq=moq,
            lead_time_weeks=lt_weeks,
            all_covs=all_covs_data,
            reference_week=current_week,
            is_340=is_340
        )

        try:
            # Add this print to check context
            print(f"\n🕓 Current inventory week for SKU {sku}: {current_week}")

            try:
                # Use lead time instead of +1
                next_week_index = sorted_weeks.index(current_week) + lt_weeks
                target_week = sorted_weeks[next_week_index]
                print(f"📦 SKU {sku}: production target week (after {lt_weeks} week(s) lead time): {target_week}")
            except IndexError:
                print(f"⚠️ Not enough future weeks after inventory week for SKU {sku}, skipping.")
                continue

        except IndexError:
            print(f"⚠️ No future week available after inventory week for SKU {sku}, skipping.")
            continue

        # Use current inventory to plan for next week
        current_row = master_df[master_df["Week"] == current_week].copy()
        next_row = master_df[master_df["Week"] == target_week].copy()

        adu = next_row["ADU"].values[0]
        qualified_demand = calculate_qualified_demand(sku_sales_orders, adu=adu, lead_time_weeks=lt_weeks)

        on_hand = current_row["Inventory"].values[0]
        # Get all open production orders from the current week onward
        future_orders = master_df.loc[master_df["Week"] >= current_week, "Production Orders"].sum()
        on_order = future_orders
        net_flow = calculate_net_flow(on_hand=on_hand, open_supply=on_order, qualified_demand=qualified_demand)

        # Compute buffer zone tops
        red_top = next_row["Red Zone"].values[0]
        yellow_top = red_top + next_row["Yellow Zone"].values[0]
        green_top = yellow_top + next_row["Green Zone"].values[0]

        # Calculate recommended production only if net flow is below Top of Yellow
        import math
        if net_flow < yellow_top:
            recommended_production = max(0, green_top - net_flow)
            if rounding_value and rounding_value > 0:
                recommended_production = math.ceil(recommended_production / rounding_value) * rounding_value
            else:
                recommended_production = int(round(recommended_production + 0.4999))
            if recommended_production < moq:
                recommended_production = 0
        else:
            recommended_production = 0

        # Output current week's snapshot
        output_data = {
            "SKU": sku,
            "Current Week": current_week,
            "Target Production Week": target_week,
            "Weekly ADU": next_row["Weekly ADU"].values[0],
            "CoV": next_row["CoV"].values[0],
            "Red Base": next_row["Red Base"].values[0],
            "Red Safety": next_row["Red Safety"].values[0],
            "Red Zone": next_row["Red Zone"].values[0],
            "Yellow Zone": next_row["Yellow Zone"].values[0],
            "Green Zone": next_row["Green Zone"].values[0],
            "Top of Red": red_top,
            "Top of Yellow": yellow_top,
            "Top of Green": green_top,
            "Net Flow": net_flow,
            "Recommended Production": recommended_production,
            "Inventory (Current Week)": on_hand,
            "On Order": on_order,
            "Qualified Demand": qualified_demand

        }
        all_results.append(output_data)

        # Save snapshot to weekly log
        snapshot_path = os.path.join(OUTPUTS_DIR, f"SKU_{sku}_ddmrp_snapshot.csv")
        pd.DataFrame([output_data]).to_csv(snapshot_path, index=False)
        print(f"📤 Snapshot saved for SKU {sku}: {snapshot_path}")

        # Save final merged SKU file
        filename = f"SKU_{sku}.xlsx"
        filepath = os.path.join(OUTPUTS_DIR, filename)

        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            master_df.to_excel(writer, sheet_name="Master Data", index=False)
            if not sku_sales_orders.empty:
                sku_sales_orders.to_excel(writer, sheet_name="Open Sales Orders", index=False)

        print(f"✅ Saved merged and calculated DDMRP file for SKU {sku}")

    # Write all production recommendations into a summary file
    summary_df = pd.DataFrame(all_results)  # This already contains the output_data for each SKU
    summary_output_path = os.path.join(OUTPUTS_DIR, "ddmrp_weekly_production_plan.csv")
    summary_df.to_csv(summary_output_path, index=False)
    print(f"\n📄 DDMRP summary for all SKUs written to:\n{summary_output_path}")


if __name__ == "__main__":
    main()