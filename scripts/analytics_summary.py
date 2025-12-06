"""
Aggregate curated S3 parquet into monthly and seasonal summaries.
Outputs a CSV to S3: tlc/analytics/summary.csv
Usage:
  python scripts/analytics_summary.py --cab-types yellow green fhv fhvhv
"""
import argparse
import boto3
import pandas as pd
from io import BytesIO
import re

def month_to_season(m):
    return {12:"Winter",1:"Winter",2:"Winter",3:"Spring",4:"Spring",5:"Spring",6:"Summer",7:"Summer",8:"Summer",9:"Fall",10:"Fall",11:"Fall"}[m]

def list_curated(bucket, cab):
    s3 = boto3.client("s3")
    prefix = f"tlc/curated/{cab}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys

def read_parquet_s3(s3, bucket, key):
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    return pd.read_parquet(BytesIO(data))

def aggregate(cab_types):
    bucket = "nyc-yellowcab-data-as-2025"
    s3 = boto3.client("s3")
    records = []
    pattern = re.compile(r"year=(\d{4})/month=(\d{2})/")
    for cab in cab_types:
        keys = list_curated(bucket, cab)
        for key in keys:
            m = pattern.search(key)
            if not m:
                continue
            year, month = int(m.group(1)), int(m.group(2))
            df = read_parquet_s3(s3, bucket, key)
            row_count = len(df)
            sum_dist = df['trip_distance'].sum() if 'trip_distance' in df.columns else None
            sum_fare = df['fare_amount'].sum() if 'fare_amount' in df.columns else None
            records.append({
                'cab_type': cab,
                'year': year,
                'month': month,
                'season': month_to_season(month),
                'rows': row_count,
                'sum_trip_distance': sum_dist,
                'sum_fare_amount': sum_fare,
            })
    result = pd.DataFrame(records)
    monthly = result.groupby(['cab_type','year','month','season'], as_index=False).sum(numeric_only=True)
    seasonal = result.groupby(['cab_type','year','season'], as_index=False).sum(numeric_only=True)
    yearly = result.groupby(['cab_type','year'], as_index=False).sum(numeric_only=True)
    return result, monthly, seasonal, yearly

def write_csv_to_s3(df, bucket, key):
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def main(cab_types):
    raw, monthly, seasonal, yearly = aggregate(cab_types)
    bucket = "nyc-yellowcab-data-as-2025"
    write_csv_to_s3(raw, bucket, "tlc/analytics/raw_records.csv")
    write_csv_to_s3(monthly, bucket, "tlc/analytics/monthly.csv")
    write_csv_to_s3(seasonal, bucket, "tlc/analytics/seasonal.csv")
    write_csv_to_s3(yearly, bucket, "tlc/analytics/yearly.csv")
    print("Wrote analytics CSVs under s3://nyc-yellowcab-data-as-2025/tlc/analytics/")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cab-types", nargs="+", required=True)
    args = parser.parse_args()
    main(args.cab_types)
