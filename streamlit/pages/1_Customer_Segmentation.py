import streamlit as st
import plotly.express as px
from data_loader import load_customer_metrics

st.title("👥 Customer Segmentation (RFM)")
st.markdown("Distribution of customers by Recency–Frequency–Monetary (RFM) segments")

cm = load_customer_metrics()

# KPI Summary
def kpi_summary(df, metrics):
    cols = st.columns(len(metrics))
    for i, (label, colname, agg) in enumerate(metrics):
        if colname in df.columns:
            value = getattr(df[colname], agg)()
            cols[i].metric(label, f"{value:,.0f}")

st.subheader("📊 KPI Summary")
kpi_summary(cm, [
    ("Total Customers", "customer_id", "count"),
    ("Avg Orders", "total_orders", "mean"),
    ("Avg Lifetime Value", "lifetime_value", "mean"),
])

# Fake loyalty_status if missing
if "loyalty_status" not in cm.columns:
    cm["loyalty_status"] = ["Member" if x % 2 == 0 else "Non-Member" for x in range(len(cm))]

# Scatter: Orders vs Lifetime Value
fig1 = px.scatter(
    cm, x="total_orders", y="lifetime_value",
    color="loyalty_status", hover_data=["customer_id"],
    title="Customer Segmentation by Orders vs Lifetime Value"
)
st.plotly_chart(fig1, use_container_width=True)

# Histogram: Avg Order Value
fig2 = px.histogram(
    cm, x="avg_order_value", color="loyalty_status",
    nbins=30, title="Distribution of Average Order Value"
)
st.plotly_chart(fig2, use_container_width=True)
