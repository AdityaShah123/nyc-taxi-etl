# scripts/manual_test_upload.py
import sys
from pathlib import Path
from urllib.parse import urlparse
import requests
import boto3
import botocore

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet"
BUCKET = "nyc-yellowcab-data-as-2025"
PREFIX = "tlc/raw/fhv/2019"   # target prefix in your bucket
DEST_DIR = Path("data/raw/manual_test")
DEST_DIR.mkdir(parents=True, exist_ok=True)

def download(url, out_dir: Path):
    fn = Path(urlparse(url).path).name
    out = out_dir / fn
    print("Downloading ->", out)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=4_194_304):
                if chunk:
                    f.write(chunk)
    print("Downloaded size (bytes):", out.stat().st_size)
    return out

def upload(local_path: Path, bucket: str, prefix: str):
    key = f"{prefix.rstrip('/')}/{local_path.name}"
    print(f"Uploading {local_path} -> s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    try:
        s3.upload_file(str(local_path), bucket, key)
        print("Upload succeeded")
    except botocore.exceptions.ClientError as e:
        print("Upload failed (ClientError):", e)
        raise
    except Exception as e:
        print("Upload failed (other):", e)
        raise
    return f"s3://{bucket}/{key}"

def verify(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    try:
        r = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)
        print("S3 list KeyCount:", r.get("KeyCount", 0))
        for o in r.get("Contents", [])[:20]:
            print(" -", o["Key"], o["Size"])
    except botocore.exceptions.ClientError as e:
        print("S3 list failed (ClientError):", e)
        raise

if __name__ == "__main__":
    try:
        local = download(URL, DEST_DIR)
    except Exception as e:
        print("Download failed:", e)
        sys.exit(2)
    try:
        s3uri = upload(local, BUCKET, PREFIX)
        print("Uploaded to:", s3uri)
    except Exception as e:
        print("Upload error:", e)
        sys.exit(3)
    try:
        verify(BUCKET, PREFIX)
    except Exception as e:
        print("Verify error:", e)
        sys.exit(4)
    print("Manual test completed successfully.")
