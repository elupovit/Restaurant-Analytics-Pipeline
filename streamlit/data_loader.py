import pandas as pd

# Base path to your Gold layer
GOLD_PATH = "s3://restaurant-analytics-data/gold/"

# AWS creds are pulled automatically from env vars
STORAGE_OPTS = {
    "anon": False,
    "client_kwargs": {"region_name": "us-east-1"}
}

def _safe_cast(df, numeric_cols):
    """Ensure numeric columns are cast properly, fill NaNs with 0."""
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def load_sales_summary():
    df = pd.read_parquet(GOLD_PATH + "sales_summary/", storage_options=STORAGE_OPTS)
    df = _safe_cast(df, ["total_quantity", "total_sales"])
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    return df

def load_item_performance():
    df = pd.read_parquet(GOLD_PATH + "item_performance/", storage_options=STORAGE_OPTS)
    return _safe_cast(df, ["total_quantity_sold", "total_revenue"])

def load_customer_metrics():
    df = pd.read_parquet(GOLD_PATH + "customer_metrics/", storage_options=STORAGE_OPTS)
    return _safe_cast(df, ["total_orders", "lifetime_value", "avg_order_value"])

def load_weekly_trends():
    df = pd.read_parquet(GOLD_PATH + "weekly_trends/", storage_options=STORAGE_OPTS)
    return _safe_cast(df, ["weekly_quantity", "weekly_sales"])

def load_holiday_vs_nonholiday():
    df = pd.read_parquet(GOLD_PATH + "holiday_vs_nonholiday/", storage_options=STORAGE_OPTS)
    return _safe_cast(df, ["total_quantity", "total_sales"])
