"""Orders dashboard for the DCM Projects quickstart.

This file is deployed by the DCM project, not by hand. The manifest declares the
folder it lives in as the `dashboard` asset, and serve.sql points a
DEFINE STREAMLIT statement at it with `FROM 'asset://dashboard/'`.

It reads the semantic view rather than the dynamic table, so the metric
definitions shown here are the same ones an AI agent would use. Nothing is
hard-coded to an environment: the database name is read from the session, so the
identical file serves DEV, STAGE and PROD.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Orders Dashboard", layout="wide")
st.title("Orders Dashboard")
st.caption("Reading the ORDER_ANALYTICS semantic view deployed by this DCM project.")

session = get_active_session()

# The app object lives in <database>.SERVE, so the current database is the one
# this deployment created. Asset files are not Jinja-rendered, so resolving the
# name at runtime is what keeps one file working across every target.
database = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

revenue_by_category = session.sql(f"""
    SELECT *
    FROM SEMANTIC_VIEW(
        {database}.SERVE.ORDER_ANALYTICS
        DIMENSIONS order_lines.ITEM_CATEGORY
        METRICS
            order_lines.TOTAL_REVENUE,
            order_lines.TOTAL_PROFIT,
            order_lines.ORDER_COUNT
    )
    ORDER BY TOTAL_REVENUE DESC
""").to_pandas()

if revenue_by_category.empty:
    st.info(
        "No rows yet. Run scripts/02_post_deploy.sql to seed the landing tables "
        "and refresh the dynamic table."
    )
    st.stop()

left, middle, right = st.columns(3)
left.metric("Revenue", f"${revenue_by_category['TOTAL_REVENUE'].sum():,.2f}")
middle.metric("Profit", f"${revenue_by_category['TOTAL_PROFIT'].sum():,.2f}")
right.metric("Orders", f"{revenue_by_category['ORDER_COUNT'].sum():,}")

st.subheader("Revenue by menu category")
st.bar_chart(revenue_by_category, x="ITEM_CATEGORY", y="TOTAL_REVENUE")

st.subheader("Detail")
st.dataframe(revenue_by_category, use_container_width=True, hide_index=True)

