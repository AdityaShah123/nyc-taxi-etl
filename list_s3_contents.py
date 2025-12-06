"""List all prefixes in S3 bucket to understand structure."""
import boto3

s3 = boto3.client('s3', region_name='us-east-2')
bucket = 'nyc-yellowcab-data-as-2025'

print(f"Listing all objects in {bucket}:")
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, MaxKeys=20)

count = 0
for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            print(f"  {obj['Key']} ({obj['Size']} bytes)")
            count += 1
            if count >= 20:
                break
    if count >= 20:
        break

if count == 0:
    print("  (No objects found)")
