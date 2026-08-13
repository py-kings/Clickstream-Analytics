
import io
import os
import boto3
import pandas as pd
from prophet import Prophet


BUCKET_NAME = os.environ.get("CLICKSTREAM_BUCKET", "project-bda-data-123")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
FORECAST_DAYS = int(os.environ.get("FORECAST_DAYS", "14"))
OUTPUT_KEY = "predicted/viewers_forecast/viewers_forecast.parquet"
# ============================================================


def get_s3_client():
    
    return boto3.client("s3", region_name=REGION)


def read_parquet_folder(s3, prefix):
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        raise FileNotFoundError(f"No parquet files found under s3://{BUCKET_NAME}/{prefix}")
    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(frames, ignore_index=True)


def run_prophet_forecast(source_df, value_col, label):
    
    prophet_df = source_df[["event_date", value_col]].rename(
        columns={"event_date": "ds", value_col: "y"}
    )

    model = Prophet(
        weekly_seasonality=True,   # captures the weekly zigzag pattern
        daily_seasonality=False,
        yearly_seasonality=False,  # not enough history to learn a yearly cycle reliably
    )
    model.fit(prophet_df)

    future = model.make_future_dataframe(periods=FORECAST_DAYS)
    forecast = model.predict(future)

    
    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(FORECAST_DAYS)
    result = result.rename(columns={
        "ds": "date",
        "yhat": f"predicted_{label}",
        "yhat_lower": f"{label}_lower_bound",
        "yhat_upper": f"{label}_upper_bound",
    })
    return result


def main():
    s3 = get_s3_client()

    print("Reading daily_funnel from S3...")
    df = read_parquet_folder(s3, "processed/daily_funnel/")
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values("event_date").reset_index(drop=True)

    print(f"Loaded {len(df)} days, from {df['event_date'].min().date()} to {df['event_date'].max().date()}")

    print("\nTraining Prophet model for viewers...")
    viewers_forecast = run_prophet_forecast(df, "viewers", "viewers")

    print(f"\nProphet forecast for the next {FORECAST_DAYS} days:\n")
    print(viewers_forecast.to_string(index=False))

    print(f"\nUploading forecast to s3://{BUCKET_NAME}/{OUTPUT_KEY} ...")
    buffer = io.BytesIO()
    viewers_forecast.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY, Body=buffer.getvalue())
    print("Upload complete.")


if __name__ == "__main__":
    main()
