import os
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Realtime Inventory Dashboard", layout="wide")
st.title("Realtime Inventory Watch Dashboard")

TOP_PRODUCTS_PATH = Path("/app/output/top_products.csv")
ALERTS_DIR = Path("/app/output/alerts")


def load_top_products() -> pd.DataFrame:
    if not TOP_PRODUCTS_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(TOP_PRODUCTS_PATH)


def load_alerts() -> pd.DataFrame:
    if not ALERTS_DIR.exists():
        return pd.DataFrame()

    parquet_files = sorted(
        [f for f in ALERTS_DIR.iterdir() if f.suffix == ".parquet"],
        key=os.path.getmtime,
        reverse=True,
    )
    if not parquet_files:
        return pd.DataFrame()

    frames = [pd.read_parquet(pf) for pf in parquet_files]
    return pd.concat(frames, ignore_index=True)


st.sidebar.header("Controls")
refresh = st.sidebar.button("Refresh data")
if refresh:
    st.rerun()

top_products_df = load_top_products()
alerts_df = load_alerts()

col1, col2, col3 = st.columns(3)
col1.metric("Top products rows", len(top_products_df))
col2.metric("Alerts rows", len(alerts_df))
col3.metric(
    "Unique products in alerts",
    int(alerts_df["product_id"].nunique()) if not alerts_df.empty else 0,
)

st.subheader("Top Products (Airflow Daily Report)")
if top_products_df.empty:
    st.info("No top products report found yet. Run the Airflow DAG first.")
else:
    st.bar_chart(top_products_df.set_index("product_id")["view_count"])
    st.dataframe(top_products_df, use_container_width=True)

st.subheader("Recent Alerts (Spark Streaming)")
if alerts_df.empty:
    st.info("No alert parquet files found yet. Start producer + spark stream.")
else:
    if "window" in alerts_df.columns:
        # Spark window often comes as a struct/object; keep table readable.
        alerts_df = alerts_df.copy()
        alerts_df["window"] = alerts_df["window"].astype(str)

    st.dataframe(alerts_df.tail(50), use_container_width=True)

