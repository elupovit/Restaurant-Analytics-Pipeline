import streamlit as st
import plotly.express as px
from data_loader import load_sales_summary

st.title("📍 Location Performance")

ss = load_sales_summary()

# Use "brand" instead of store_name
brand_perf = ss.groupby("brand")[["total_quantity", "total_sales"]].sum().reset_index()

# KPI Summary
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

st.subheader("📊 KPI Summary")
kpi_summary(brand_perf, [
    ("Total Sales", "total_sales", "sum"),
    ("Total Quantity", "total_quantity", "sum"),
    ("Number of Brands", "brand", "count"),
])

# Bar: Sales by brand
fig1 = px.bar(
    brand_perf, x="brand", y="total_sales",
    title="Total Sales by Brand", text_auto=True
)
st.plotly_chart(fig1, use_container_width=True)

# Bar: Quantity by brand
fig2 = px.bar(
    brand_perf, x="brand", y="total_quantity",
    title="Total Quantity Sold by Brand", text_auto=True
)
st.plotly_chart(fig2, use_container_width=True)
