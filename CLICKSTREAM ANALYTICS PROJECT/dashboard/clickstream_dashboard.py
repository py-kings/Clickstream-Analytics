
import streamlit as st
import pandas as pd
import boto3
import io


AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
BUCKET_NAME = "project-bda-data-123"
REGION = "us-east-1"



st.set_page_config(page_title="RetailRocket Clickstream Dashboard", layout="wide")


@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION,
    )


def list_parquet_keys(s3, prefix):
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]


def read_parquet_folder(s3, prefix):
    """Reads every .parquet part-file under a folder prefix and unions them into one DataFrame."""
    keys = list_parquet_keys(s3, prefix)
    if not keys:
        return pd.DataFrame()
    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        frames.append(pd.read_parquet(buf))
    return pd.concat(frames, ignore_index=True)



@st.cache_data(ttl=300)
def load_data():
    s3 = get_s3_client()
    daily_funnel = read_parquet_folder(s3, "processed/daily_funnel/")
    sessions = read_parquet_folder(s3, "processed/sessions/")
    top_items = read_parquet_folder(s3, "processed/top_items/")
    return daily_funnel, sessions, top_items



st.title("📊 RetailRocket Clickstream — Live Dashboard")
st.caption("Reads directly from S3 (Parquet) — auto-refreshes every 5 minutes, or click below for the latest data.")

if st.button("🔄 Refresh data now"):
    st.cache_data.clear()

with st.spinner("Loading data from S3..."):
    daily_funnel, sessions, top_items = load_data()

if daily_funnel.empty:
    st.error("No data found in S3. Check your bucket name, keys, and that processed/ files exist.")
    st.stop()

# ---- Prep ----
daily_funnel = daily_funnel.sort_values("event_date")
total_viewers = int(daily_funnel["viewers"].sum())
total_carters = int(daily_funnel["carters"].sum())
total_buyers = int(daily_funnel["buyers"].sum())
overall_conv = total_buyers / total_viewers if total_viewers else 0

# ---- KPI row ----
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Viewers", f"{total_viewers:,}")
col2.metric("Total Cart Adds", f"{total_carters:,}")
col3.metric("Total Purchases", f"{total_buyers:,}")
col4.metric("Overall Conversion Rate", f"{overall_conv:.2%}")

st.divider()

# ---- Funnel chart ----
st.subheader("Conversion Funnel")
funnel_totals = pd.DataFrame({
    "Stage": ["Viewers", "Cart Adds", "Purchases"],
    "Count": [total_viewers, total_carters, total_buyers],
})
st.bar_chart(funnel_totals.set_index("Stage"))

# ---- Conversion trend over time ----
st.subheader("Daily Conversion Rate Trend")
daily_funnel["conversion_rate"] = daily_funnel["buyers"] / daily_funnel["viewers"]
st.line_chart(daily_funnel.set_index("event_date")["conversion_rate"])

# ---- Top items ----
if not top_items.empty:
    st.subheader("Top 20 Items by Purchases")
    top20 = top_items.sort_values("buyers", ascending=False).head(20)
    st.bar_chart(top20.set_index("itemid")["buyers"])

# ---- Session behavior ----
if not sessions.empty:
    st.subheader("Session Behavior: Converted vs Not Converted")
    behavior = sessions.groupby("converted")["event_count"].mean().rename(
        index={0: "Not Converted", 1: "Converted"}
    )
    st.bar_chart(behavior)

st.caption(f"Data source: s3://{BUCKET_NAME}/processed/  |  Cache refreshes every 5 minutes")
