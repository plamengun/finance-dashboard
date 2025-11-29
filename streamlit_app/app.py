import streamlit as st
import pandas as pd

st.title("Local Parquet Dashboard")

# Read directly from file in the container
df = pd.read_parquet("dashboard_data.parquet", engine="pyarrow")

st.subheader("Data Preview")
st.dataframe(df)

# Example chart (customize column names to your schema)
if "date" in df.columns and "total_events" in df.columns:
    df["date"] = pd.to_datetime(df["date"])
    st.subheader("Events Over Time")
    st.line_chart(df.set_index("date")["total_events"])