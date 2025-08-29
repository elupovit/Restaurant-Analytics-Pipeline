import streamlit as st
import plotly.express as px
from data_loader import load_weekly_trends

st.title("📈 Sales Trends & Seasonality")

wt = load_weekly_trends()

# KPI Summary
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

st.subheader("📊 KPI Summary")
kpi_summary(wt, [
    ("Total Sales", "weekly_sales", "sum"),
    ("Avg Weekly Sales", "weekly_sales", "mean"),
    ("Avg Weekly Quantity", "weekly_quantity", "mean"),
])

# Line: Weekly sales over time
fig1 = px.line(
    wt, x="week_of_year", y="weekly_sales", color="brand",
    title="Weekly Sales by Brand"
)
st.plotly_chart(fig1, use_container_width=True)

# Bar: Weekly quantity
fig2 = px.bar(
    wt, x="week_of_year", y="weekly_quantity", color="brand",
    title="Weekly Quantity Sold by Brand"
)
st.plotly_chart(fig2, use_container_width=True)
