"""
Batch ETL over all available months/years for the specified cab types.
Uses pandas-based ETL from spark_jobs.etl_simple.

Example:
  python scripts/batch_etl_simple.py --cab-types yellow green fhv fhvhv
  python scripts/batch_etl_simple.py --cab-types fhvhv --start-year 2019 --end-year 2020
"""
import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import boto3

# Ensure repo root on sys.path so spark_jobs is importable when run as a script
sys.path.append(str(Path(__file__).resolve().parents[1]))
from spark_jobs.etl_simple import etl, log

def discover_months(bucket, cab_types, start_year=None, end_year=None):
    s3 = boto3.client("s3")
    pattern = re.compile(r"(\d{4})-(\d{2})")
    results = defaultdict(set)
    for cab in cab_types:
        base_dir = "fhv" if cab in ("fhv", "fhvhv") else cab
        prefix = f"tlc/raw/{base_dir}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if f"/{cab}_tripdata_" not in key:
                    continue
                m = pattern.search(key)
                if not m:
                    continue
                year, month = int(m.group(1)), int(m.group(2))
                if start_year and year < start_year:
                    continue
                if end_year and year > end_year:
                    continue
                results[cab].add((year, month))
    return {cab: sorted(vals) for cab, vals in results.items()}

def main(cab_types, start_year=None, end_year=None):
    bucket = "nyc-yellowcab-data-as-2025"
    plan = discover_months(bucket, cab_types, start_year, end_year)
    total = sum(len(v) for v in plan.values())
    log(f"Discovered {total} month(s) across {len(plan)} cab type(s)")
    for cab, combos in plan.items():
        for year, month in combos:
            log(f"RUN {cab} {year}-{month:02d}")
            ok = etl(cab, year, month)
            if not ok:
                log(f"FAILED {cab} {year}-{month:02d}")
    log("Batch ETL complete")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cab-types", nargs="+", required=True, help="yellow green fhv fhvhv")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    args = parser.parse_args()
    main(args.cab_types, args.start_year, args.end_year)
