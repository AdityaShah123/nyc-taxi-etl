"""
Quick test: Can boto3 read from S3?
If this works, AWS creds are OK. If not, creds/permissions issue.
"""
import boto3
import os

def test_s3_boto():
    print("--- Testing boto3 S3 connection ---")
    print(f"AWS_ACCESS_KEY_ID: {'SET' if os.getenv('AWS_ACCESS_KEY_ID') else 'NOT SET'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'SET' if os.getenv('AWS_SECRET_ACCESS_KEY') else 'NOT SET'}")
    print()
    
    try:
        s3 = boto3.client('s3', region_name='us-east-2')
        bucket = 'nyc-yellowcab-data-as-2025'
        
        # List objects in yellow/2025/01/
        print(f"Listing s3://{bucket}/tlc/raw/yellow/2025/01/")
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='tlc/raw/yellow/2025/01/',
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"SUCCESS! Found {len(response['Contents'])} files")
            for obj in response['Contents'][:3]:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("ERROR: No files found in that prefix")
    
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test_s3_boto()
