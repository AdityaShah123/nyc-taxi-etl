"""
Simple ETL: read parquet from S3, filter, write parquet back to S3.
Uses pandas + pyarrow + boto3. No Spark JVM issues.
Usage:
  python spark_jobs/etl_simple.py --cab-type yellow --year 2019 --month 01
"""
import argparse
import os
import sys
import pandas as pd
import pyarrow.parquet as pq
import boto3
from io import BytesIO

def log(msg):
    print(f"[ETL] {msg}", file=sys.stderr)
    sys.stderr.flush()

def etl(cab_type, year, month):
    """Read raw parquet from S3, filter, write curated parquet to S3.
    Note: fhv and fhvhv files both live under the fhv/ directory in this bucket.
    """
    try:
        log(f"START {cab_type} {year}-{month:02d}")
        
        # Setup S3
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        
        if not access_key or not secret_key:
            log("ERROR: AWS credentials not found in environment")
            return False
        
        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        bucket = "nyc-yellowcab-data-as-2025"
        base_dir = "fhv" if cab_type in ("fhv", "fhvhv") else cab_type
        input_key = f"tlc/raw/{base_dir}/{year}/{cab_type}_tripdata_{year}-{month:02d}.parquet"
        output_key = f"tlc/curated/{cab_type}/year={year}/month={month:02d}/data.parquet"
        
        log(f"Reading: s3://{bucket}/{input_key}")
        
        # Download parquet from S3
        try:
            response = s3.get_object(Bucket=bucket, Key=input_key)
        except s3.exceptions.NoSuchKey:
            log(f"ERROR: Key not found: s3://{bucket}/{input_key}")
            # List available files in the directory to help the user
            prefix = f"tlc/raw/{base_dir}/{year}/"
            listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
            if not listing:
                log(f"No files found under prefix: s3://{bucket}/{prefix}")
            else:
                log("Available files under prefix:")
                for obj in listing:
                    log(f" - {obj['Key']}")
            return False
        parquet_data = response['Body'].read()
        
        # Read into pandas
        df = pd.read_parquet(BytesIO(parquet_data))
        log(f"Read {len(df)} rows")
        
        # Filter with column guards (fhv/fhvhv often lack fare_amount)
        conditions = []
        if 'fare_amount' in df.columns:
            conditions.append(df['fare_amount'] > 0)
        if 'trip_distance' in df.columns:
            conditions.append(df['trip_distance'] > 0)
        if conditions:
            from functools import reduce
            import operator
            mask = reduce(operator.and_, conditions)
            df = df[mask]
        log(f"Filtered to {len(df)} rows")
        
        # Write to BytesIO buffer
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        
        # Upload to S3
        log(f"Writing: s3://{bucket}/{output_key}")
        s3.put_object(Bucket=bucket, Key=output_key, Body=buffer.getvalue())
        
        log(f"SUCCESS {cab_type} {year}-{month:02d}")
        return True
        
    except Exception as e:
        log(f"ERROR: {str(e)}")
        import traceback
        log(traceback.format_exc())
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple pandas-based ETL")
    parser.add_argument("--cab-type", required=True, help="yellow, green, fhv, fhvhv")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    
    args = parser.parse_args()
    success = etl(args.cab_type, args.year, args.month)
    sys.exit(0 if success else 1)
