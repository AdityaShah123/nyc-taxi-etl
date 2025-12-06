# scripts/retry_failed.py
import json, time, datetime, logging
from pathlib import Path
from urllib.parse import urlparse
import requests
import boto3

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
MANIFEST = Path("data/raw/manifest.json")
TMP_DIR = Path("data/raw/tmp_download")
S3_PREFIX = "tlc/raw"

def load_manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))

def save_manifest(m):
    MANIFEST.write_text(json.dumps(m, indent=2), encoding="utf-8")

def download_url(url, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name
    target = out_dir / filename
    # if a partial file exists, remove it
    if target.exists():
        target.unlink()
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with open(target, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=4_194_304):
            if chunk:
                fh.write(chunk)
    return target

def upload_to_s3(local_path, bucket, ttype, year):
    s3 = boto3.client("s3")
    key = f"{S3_PREFIX}/{ttype}/{year}/{local_path.name}"
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"

def retry_entry(entry, s3_bucket, max_attempts=3):
    url = entry["url"]
    ym = entry["year_month"]
    year = ym.split("-")[0]
    ttype = entry["type"]
    errors = []
    for attempt in range(1, max_attempts+1):
        try:
            logging.info("Attempt %d/%d for %s", attempt, max_attempts, url)
            # download to tmp
            local = download_url(url, TMP_DIR)
            entry["downloaded"] = True
            entry["local_path"] = str(local)
            entry["size_bytes"] = local.stat().st_size
            # upload
            s3uri = upload_to_s3(local, s3_bucket, ttype, year)
            entry["uploaded"] = True
            entry["s3_uri"] = s3uri
            entry["errors"] = []
            entry["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
            # remove tmp file
            try:
                local.unlink(missing_ok=True)
            except Exception:
                pass
            return entry
        except Exception as e:
            logging.warning("Error on attempt %d: %s", attempt, e)
            errors.append(str(e))
            time.sleep(2 ** attempt)
    # failed all attempts
    entry.setdefault("errors", [])
    entry["errors"].extend(["retry_failed:"+e for e in errors])
    entry["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    return entry

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True, help="S3 bucket to upload to")
    p.add_argument("--max-attempts", type=int, default=3)
    args = p.parse_args()

    m = load_manifest()
    changed = False
    for i, e in enumerate(m.get("files", [])):
        # select entries that failed (not uploaded) and belong to our desired upload range
        if not e.get("uploaded", False) and (e.get("errors") or True):
            # only retry those types/months relevant to our upload range
            # we'll attempt for all failed entries
            logging.info("Retrying entry %d: %s", i, e.get("url"))
            updated = retry_entry(e, args.bucket, max_attempts=args.max_attempts)
            m["files"][i] = updated
            save_manifest(m)  # flush immediately
            changed = True
    if not changed:
        logging.info("No failed entries to retry.")
    else:
        logging.info("Retries complete, manifest updated.")

if __name__ == "__main__":
    main()
