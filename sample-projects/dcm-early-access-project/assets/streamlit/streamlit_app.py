"""Multipage sales dashboard deployed as one DCM asset."""

import streamlit as st

st.set_page_config(layout="wide", page_title="DCM sales dashboard")
page = st.navigation([
    st.Page("app_pages/overview.py", title="Overview", default=True),
    st.Page("app_pages/daily_sales.py", title="Daily sales"),
    st.Page("app_pages/category_city.py", title="Category and city"),
])
page.run()