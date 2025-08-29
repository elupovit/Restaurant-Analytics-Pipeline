import streamlit as st
import plotly.express as px
from data_loader import load_item_performance
import numpy as np

st.title("💲 Pricing & Discount Effectiveness")

ip = load_item_performance()

# Create synthetic "discount_applied": flag items < median price as discounted
median_price = ip["total_revenue"] / ip["total_quantity_sold"]
ip["discount_applied"] = np.where(median_price < median_price.median(), "Yes", "No")

# KPI Summary
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

st.subheader("📊 KPI Summary")
kpi_summary(ip, [
    ("Total Items", "item_id", "count"),
    ("Avg Revenue per Item", "total_revenue", "mean"),
    ("Total Revenue", "total_revenue", "sum"),
])

# Boxplot: Revenue by Discount
fig1 = px.box(
    ip, x="discount_applied", y="total_revenue", points="all",
    title="Revenue Impact of Discounts"
)
st.plotly_chart(fig1, use_container_width=True)

# Histogram: Quantity sold under discount vs non-discount
fig2 = px.histogram(
    ip, x="total_quantity_sold", color="discount_applied", nbins=30,
    title="Distribution of Quantities Sold by Discount"
)
st.plotly_chart(fig2, use_container_width=True)
