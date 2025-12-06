"""
ETL Job: Read raw yellow taxi parquet from S3, clean, write curated parquet to S3.

Usage:
  spark-submit \
    --packages "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:s3:2.20.26" \
    spark_jobs/etl_yellow_s3.py \
    --year 2019 --month 01
"""
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, hour, 
    date_format, when, unix_timestamp, 
    datediff, lit, coalesce
)

def create_spark():
    """Initialize Spark with S3 credentials."""
    spark = SparkSession.builder \
        .appName("ETL-Yellow-S3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-2")) \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark

def clean_yellow_data(df):
    """
    Clean and transform yellow taxi data.
    Removes invalid records, creates derived features.
    """
    # Parse timestamps
    df = df.withColumn("pickup_dt", to_timestamp(col("tpep_pickup_datetime")))
    df = df.withColumn("dropoff_dt", to_timestamp(col("tpep_dropoff_datetime")))
    
    # Filter invalid records
    df = df.filter(
        (col("pickup_dt").isNotNull()) &
        (col("dropoff_dt").isNotNull()) &
        (col("dropoff_dt") > col("pickup_dt")) &
        (col("trip_distance") > 0) &
        (col("fare_amount") >= 0)
    )
    
    # Compute trip duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (unix_timestamp("dropoff_dt") - unix_timestamp("pickup_dt")) / 60.0
    )
    
    # Filter outliers
    df = df.filter(
        (col("trip_duration_min") > 0.5) &
        (col("trip_duration_min") < 1440) &  # 1 day
        (col("trip_distance") < 500)
    )
    
    # Derived features
    df = df.withColumn(
        "fare_per_mile",
        when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance")).otherwise(None)
    )
    df = df.withColumn(
        "avg_speed_mph",
        when(col("trip_duration_min") > 0, col("trip_distance") / (col("trip_duration_min") / 60.0)).otherwise(None)
    )
    
    # Time features
    df = df.withColumn("pickup_year", year("pickup_dt"))
    df = df.withColumn("pickup_month", month("pickup_dt"))
    df = df.withColumn("pickup_hour", hour("pickup_dt"))
    df = df.withColumn("pickup_date", date_format("pickup_dt", "yyyy-MM-dd"))
    
    return df

def write_curated(df, output_base, year, month):
    """Write cleaned data to S3 curated location."""
    output_path = f"{output_base}/yellow/year={year}/month={month:02d}/"
    
    print(f"Writing curated output to: {output_path}")
    df.coalesce(10).write.mode("overwrite").parquet(output_path)
    print(f"Successfully wrote {df.count()} records to {output_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, help="Year to process (e.g., 2019)")
    parser.add_argument("--month", required=True, help="Month to process (01-12)")
    parser.add_argument("--bucket", default="nyc-yellowcab-data-as-2025", help="S3 bucket name")
    args = parser.parse_args()
    
    year = args.year
    month = args.month.zfill(2)  # Pad to 2 digits
    bucket = args.bucket
    
    # S3 paths
    input_base = f"s3a://{bucket}/tlc/raw"
    output_base = f"s3a://{bucket}/tlc/curated"
    
    print(f"\n{'='*60}")
    print(f"ETL Job: Yellow Taxi {year}-{month}")
    print(f"Input: {input_base}/yellow/{year}/{month}/*")
    print(f"Output: {output_base}/yellow/year={year}/month={month:02d}/")
    print(f"{'='*60}\n")
    
    spark = create_spark()
    
    try:
        # Read raw data
        input_path = f"{input_base}/yellow/{year}/yellow_tripdata_{year}-{month}.parquet"
        print(f"Reading: {input_path}")
        df = spark.read.parquet(input_path)
        print(f"Read {df.count()} raw records")
        print(f"Schema: {df.printSchema()}")
        
        # Clean and transform
        print("\nCleaning data...")
        df_clean = clean_yellow_data(df)
        print(f"After cleaning: {df_clean.count()} records")
        
        # Write curated
        write_curated(df_clean, output_base, year, month)
        
        print(f"\nETL completed successfully!")
    
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
