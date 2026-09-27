"""Bounded, cached reads shared by the dashboard pages."""

import streamlit as st

QUERIES = {
    "daily": (
        "V_DASHBOARD_DAILY_SALES",
        "select SALE_DATE, DAILY_ORDERS, DAILY_REVENUE, DAILY_PROFITS "
        "from identifier(%s) order by SALE_DATE desc limit 366",
    ),
    "category_city": (
        "V_DASHBOARD_SALES_BY_CATEGORY_CITY",
        "select ITEM_CATEGORY, CUSTOMER_CITY, TOTAL_REVENUE "
        "from identifier(%s) order by TOTAL_REVENUE desc nulls last, "
        "ITEM_CATEGORY, CUSTOMER_CITY limit 1000",
    ),
}


def load_sales(dataset):
    view, query = QUERIES[dataset]
    try:
        connection = st.connection("snowflake")
        context = connection.query("select current_database() as DB", ttl=0)
        database = context.iloc[0]["DB"]
        if not database:
            st.error("This app requires the database context of its deployed DCM project.")
            st.stop()
        database_identifier = '"' + database.replace('"', '""') + '"'
        return connection.query(
            query,
            params=[f"{database_identifier}.SERVE.{view}"],
            ttl=60,
        )
    except Exception:
        st.error("Sales data could not be read. Check deployment, role access, and warehouse availability.")
        st.stop()