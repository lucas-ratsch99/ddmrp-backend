import pandas as pd

def calculate_qualified_demand(sales_orders_df, adu, lead_time_weeks):
    """
    DDMRP-compliant spike qualification logic.
    :param sales_orders_df: DataFrame of open sales orders
    :param adu: Weekly average daily usage
    :param lead_time_weeks: Spike horizon in weeks
    :return: Qualified demand
    """
    if sales_orders_df.empty:
        return 0

    today = pd.Timestamp.today().normalize()
    sales_orders_df = sales_orders_df.copy()
    sales_orders_df["Due Date"] = pd.to_datetime(sales_orders_df["Due Date"], errors="coerce")

    past_due = sales_orders_df[sales_orders_df["Due Date"] < today]["Open Qty"].sum()
    due_today = sales_orders_df[sales_orders_df["Due Date"] == today]["Open Qty"].sum()

    # Define spike horizon in weeks (convert to days)
    spike_horizon = today + pd.Timedelta(days=lead_time_weeks * 7)
    future_orders = sales_orders_df[
        (sales_orders_df["Due Date"] > today) & (sales_orders_df["Due Date"] <= spike_horizon)
    ]

    # Threshold based on ADU
    threshold = adu  if adu > 0 else float('inf')
    spikes = future_orders[future_orders["Open Qty"] > threshold]["Open Qty"].sum()

    qualified_demand = past_due + due_today + spikes
    return qualified_demand if pd.notnull(qualified_demand) else 0

def calculate_net_flow(on_hand, open_supply, qualified_demand):
    return on_hand + open_supply - qualified_demand
