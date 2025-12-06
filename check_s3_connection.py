from pyspark.sql import SparkSession
import os

def check_s3():
    # Initialize Spark with S3 credentials from environment
    spark = SparkSession.builder \
        .appName("CheckS3Connection") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-2")) \
        .getOrCreate()

    bucket = "nyc-yellowcab-data-as-2025"
    s3_path = f"s3a://{bucket}/tlc/raw/"

    print(f"--- AWS Credentials Check ---")
    print(f"AWS_ACCESS_KEY_ID: {'SET' if os.getenv('AWS_ACCESS_KEY_ID') else 'NOT SET'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'SET' if os.getenv('AWS_SECRET_ACCESS_KEY') else 'NOT SET'}")
    print(f"AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'NOT SET')}")
    print()
    
    print(f"--- Attempting to read yellow taxi from S3 ---")
    
    try:
        df = spark.read.format("parquet").load(s3_path + "yellow/2025/01/*")
        print(f"SUCCESS! S3 Connection established.")
        print(f"Row count: {df.count()}")
        print(f"Schema: {df.printSchema()}")
        df.show(3)
    except Exception as e:
        print(f"ERROR: Could not read from S3.")
        print(f"Exception: {type(e).__name__}")
        print(f"Details: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_s3()