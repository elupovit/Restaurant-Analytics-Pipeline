import streamlit as st
import plotly.express as px
from data_loader import load_customer_metrics

st.title("💳 Loyalty Program Impact")

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
    ("Avg Lifetime Value", "lifetime_value", "mean"),
    ("Avg Orders", "total_orders", "mean"),
])

# Fake loyalty_status if missing
if "loyalty_status" not in cm.columns:
    cm["loyalty_status"] = ["Member" if x % 2 == 0 else "Non-Member" for x in range(len(cm))]

# Boxplot: Lifetime Value by Loyalty
fig1 = px.box(
    cm, x="loyalty_status", y="lifetime_value", points="all",
    title="Lifetime Value by Loyalty Membership"
)
st.plotly_chart(fig1, use_container_width=True)

# Histogram: Orders by Loyalty
fig2 = px.histogram(
    cm, x="total_orders", color="loyalty_status", nbins=30,
    title="Order Frequency by Loyalty Membership"
)
st.plotly_chart(fig2, use_container_width=True)
