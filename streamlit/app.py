import streamlit as st
import pandas as pd
from data_loader import (
    load_sales_summary,
    load_item_performance,
    load_customer_metrics,
    load_weekly_trends,
    load_holiday_vs_nonholiday,
)

st.set_page_config(page_title="Restaurant Analytics Dashboard", layout="wide")

st.title("📊 Restaurant Analytics Dashboard")
st.markdown("Welcome! Use the sidebar to navigate between modules.")

# --- KPI Summary on homepage ---
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

# --- Load data ---
sales_summary = load_sales_summary()
item_perf = load_item_performance()
customer_metrics = load_customer_metrics()
weekly_trends = load_weekly_trends()
holiday_perf = load_holiday_vs_nonholiday()

# --- KPIs ---
st.subheader("📌 Overall KPI Summary")

kpi_summary(sales_summary, [
    ("Total Sales", "total_sales", "sum"),
    ("Total Orders", "total_quantity", "sum"),
])

kpi_summary(customer_metrics, [
    ("Total Customers", "customer_id", "count"),
    ("Avg Lifetime Value", "lifetime_value", "mean"),
    ("Avg Orders per Customer", "total_orders", "mean"),
])

kpi_summary(item_perf, [
    ("Total Items", "item_id", "count"),
    ("Avg Revenue per Item", "total_revenue", "mean"),
])
