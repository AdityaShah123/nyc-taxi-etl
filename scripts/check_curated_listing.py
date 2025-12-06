"""
scripts/check_curated_listing.py
List objects under the curated yellow prefix using boto3 and print counts + samples.
Usage:
  python scripts/check_curated_listing.py --bucket nyc-yellowcab-data-as-2025 --prefix tlc/curated/yellow/
"""
import argparse
import boto3


def list_prefix(bucket, prefix, max_items=20):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    total = 0
    samples = []
    for page in pages:
        contents = page.get('Contents', [])
        total += len(contents)
        for obj in contents:
            if len(samples) < max_items:
                samples.append(obj['Key'])
    return total, samples


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--max-sample', type=int, default=20)
    args = parser.parse_args()

    total, samples = list_prefix(args.bucket, args.prefix, args.max_sample)
    print(f"Found {total} objects under s3://{args.bucket}/{args.prefix}")
    if samples:
        print('Sample keys:')
        for k in samples:
            print(' -', k)
    else:
        print('No objects found for that prefix.')
