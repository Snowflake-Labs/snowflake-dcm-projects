"""Customer segments dashboard for the Marketing team.

Reads CUSTOMER_SEGMENTS, the Iceberg table the Marketing Spark job fills. The
database comes from the app's own context, so the same file runs in DEV and
PROD.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
segments = session.table(f"{db}.SEGMENTS.CUSTOMER_SEGMENTS").to_pandas()

st.title("Customer segments")

if segments.empty:
    st.info("No segments yet. Run the Marketing pipeline to fill CUSTOMER_SEGMENTS.")
    st.stop()

st.caption(f"Last segmented at {segments['SEGMENTED_AT'].max():%Y-%m-%d %H:%M}")

tiers = sorted(segments["ACTIVITY_TIER"].unique())
picked = st.multiselect("Activity tier", tiers, default=tiers)
shown = segments[segments["ACTIVITY_TIER"].isin(picked)]

left, right = st.columns(2)
left.metric("Customers", len(shown))
right.metric("Trading value", f"${shown['TOTAL_VALUE'].sum():,.2f}")

st.subheader("Customers per channel")
st.bar_chart(shown.groupby("PREFERRED_CHANNEL").size())

st.subheader("Customers")
st.dataframe(
    shown[["CUSTOMER_NAME", "ACTIVITY_TIER", "TRADE_COUNT", "TOTAL_VALUE", "PREFERRED_CHANNEL", "CONTACT_EMAIL"]]
    .sort_values("TOTAL_VALUE", ascending=False),
    hide_index=True,
)
