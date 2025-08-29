import streamlit as st
import plotly.express as px
from data_loader import load_sales_summary
import pandas as pd

st.title("⚠️ Churn Risk Indicators")

ss = load_sales_summary()

# Engineer "days_since_last_order"
latest_date = ss["order_date"].max()
customer_last = ss.groupby("brand")["order_date"].max().reset_index()
customer_last["days_since_last_order"] = (latest_date - customer_last["order_date"]).dt.days

# KPI Summary
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

st.subheader("📊 KPI Summary")
kpi_summary(customer_last, [
    ("Brands", "brand", "count"),
    ("Max Days Since Last Order", "days_since_last_order", "max"),
    ("Avg Days Since Last Order", "days_since_last_order", "mean"),
])

# Histogram
fig1 = px.histogram(
    customer_last, x="days_since_last_order", nbins=20,
    title="Distribution of Days Since Last Order"
)
st.plotly_chart(fig1, use_container_width=True)

# Boxplot
fig2 = px.box(
    customer_last, y="days_since_last_order", points="all",
    title="Boxplot of Days Since Last Order by Brand", color="brand"
)
st.plotly_chart(fig2, use_container_width=True)
