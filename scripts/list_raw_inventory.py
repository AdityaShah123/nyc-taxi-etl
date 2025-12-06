"""
List available raw keys per cab type and extract year/month combos.
Usage:
  python scripts/list_raw_inventory.py --cab-types yellow green fhv fhvhv
"""
import argparse
import re
import boto3
from collections import defaultdict

def list_keys(bucket, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def main(cab_types):
    bucket = "nyc-yellowcab-data-as-2025"
    pattern = re.compile(r"(\d{4})-(\d{2})")
    results = defaultdict(set)
    for cab in cab_types:
        base_dir = "fhv" if cab in ("fhv", "fhvhv") else cab
        prefix = f"tlc/raw/{base_dir}/"
        for key in list_keys(bucket, prefix):
            if f"/{cab}_tripdata_" not in key:
                continue
            m = pattern.search(key)
            if m:
                year, month = m.group(1), m.group(2)
                results[cab].add((year, month))
    for cab in cab_types:
        combos = sorted(results.get(cab, []))
        print(f"{cab}: {len(combos)} months")
        for year, month in combos:
            print(f"  {year}-{month}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cab-types", nargs="+", required=True, help="e.g. yellow green fhv fhvhv")
    args = parser.parse_args()
    main(args.cab_types)
