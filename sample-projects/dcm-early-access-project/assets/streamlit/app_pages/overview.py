import streamlit as st

st.title("DCM sales dashboard")
st.write("This app and its underlying sales views are managed by the same DCM project.")
st.markdown(
    "* **Daily sales:** Revenue, profit, and order trends for up to 366 sales days.\n"
    "* **Category and city:** Filter the highest-revenue category/city combinations."
)
st.info("Sales pages require sample data and completed dynamic-table refreshes. Reads are cached for 60 seconds.")