import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Daily revenue by region")
st.caption("Built on SERVE.DAILY_REVENUE, a DCM Dynamic Table over the dbt model FCT_ORDERS.")

df = session.sql(
	"select ORDER_DATE, REGION, REVENUE from SERVE.DAILY_REVENUE order by ORDER_DATE"
).to_pandas()

if df.empty:
	st.info("No data yet. Load the sample files and run the pipeline.")
else:
	st.bar_chart(df, x="ORDER_DATE", y="REVENUE", color="REGION")
	st.dataframe(df, use_container_width=True)
