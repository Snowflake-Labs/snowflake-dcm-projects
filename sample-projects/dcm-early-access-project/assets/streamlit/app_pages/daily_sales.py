import streamlit as st

from data import load_sales

st.title("Daily sales")
daily = load_sales("daily")
st.caption("Most recent 366 sales days, including any partial current day.")
if daily.empty:
    st.info("No sales data is available yet. The sample data and dynamic tables must be populated first.")
else:
    metric = st.radio("Metric", ["DAILY_REVENUE", "DAILY_PROFITS", "DAILY_ORDERS"], horizontal=True)
    st.line_chart(daily.sort_values("SALE_DATE").set_index("SALE_DATE")[[metric]])
    st.dataframe(daily, width="stretch", hide_index=True)