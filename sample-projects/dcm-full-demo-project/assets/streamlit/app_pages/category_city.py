import streamlit as st

from data import load_sales

st.title("Sales by category and city")
sales = load_sales("category_city")
st.caption("Up to 1,000 highest-revenue category/city combinations. Charts summarize the displayed subset.")
if sales.empty:
    st.info("No category/city sales data is available yet.")
else:
    categories = sorted(sales["ITEM_CATEGORY"].dropna().unique().tolist())
    cities = sorted(sales["CUSTOMER_CITY"].dropna().unique().tolist())
    selected_categories = st.multiselect("Categories", categories, default=categories)
    selected_cities = st.multiselect("Cities", cities, default=cities)
    filtered = sales[
        sales["ITEM_CATEGORY"].isin(selected_categories)
        & sales["CUSTOMER_CITY"].isin(selected_cities)
    ]
    if filtered.empty:
        st.info("No rows match the selected categories and cities.")
    else:
        totals = filtered.groupby("ITEM_CATEGORY")["TOTAL_REVENUE"].sum(min_count=1)
        st.bar_chart(totals)
        st.dataframe(filtered, width="stretch", hide_index=True)