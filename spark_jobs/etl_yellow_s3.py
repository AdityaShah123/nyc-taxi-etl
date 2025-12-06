import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def log(msg):
    """Print to stderr so Spark doesn't suppress it."""
    print(msg, file=sys.stderr)
    sys.stderr.flush()

def create_spark(app_name):
    """Create Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

def configure_s3(spark):
    """Set S3A credentials from environment."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    
    if access_key and secret_key:
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        hconf.set("fs.s3a.access.key", access_key)
        hconf.set("fs.s3a.secret.key", secret_key)
        hconf.set("fs.s3a.endpoint.region", region)
        log(f"[CONFIG] S3A credentials set. Region: {region}")
    else:
        log("[WARNING] No AWS credentials found in environment.")

def etl(input_base, output_base, cab_type, year, month):
    """Read raw parquet, filter, write curated parquet to S3."""
    try:
        log(f"\n[START] {cab_type.upper()} {year}-{month:02d}")
        
        spark = create_spark(f"ETL-{cab_type.upper()}-{year}-{month:02d}")
        configure_s3(spark)
        
        input_path = f"{input_base}/{cab_type}/{year}/{cab_type}_tripdata_{year}-{month:02d}.parquet"
        output_path = f"{output_base}/{cab_type}/year={year}/month={month:02d}/"
        
        log(f"[INPUT] {input_path}")
        df = spark.read.parquet(input_path)
        rows_before = df.count()
        log(f"[READ] {rows_before} rows")
        
        df = df.filter((col("fare_amount") > 0) & (col("trip_distance") > 0))
        rows_after = df.count()
        log(f"[FILTER] {rows_after} rows")
        
        log(f"[OUTPUT] {output_path}")
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        spark.stop()
        log(f"[SUCCESS] Done\n")
        return True
        
    except Exception as e:
        log(f"[ERROR] {str(e)}")
        import traceback
        log(traceback.format_exc())
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--cab-type", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    
    args = parser.parse_args()
    success = etl(args.input_base, args.output_base, args.cab_type, args.year, args.month)
    sys.exit(0 if success else 1)