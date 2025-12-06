#!/usr/bin/env python3
"""
scripts/download_tlc_range_s3.py  (improved)

- Only append manifest entries after a file is processed (success or failed).
- Add retry/backoff for downloads and uploads.
- Manifest entries include status metadata: downloaded, uploaded, errors, size_bytes, s3_uri.
- Periodically flush manifest to disk after each processed file.
"""
from __future__ import annotations
import argparse
import datetime
import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("tlc-range-s3")

TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
MANIFEST_FILE = Path("data/raw/manifest.json")


def find_parquet_links(page_url: str = TLC_PAGE) -> list[str]:
    r = requests.get(page_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".parquet"):
            links.append(urljoin(page_url, href))
    links = sorted(set(links), key=lambda u: urlparse(u).path)
    logger.info("Found %d parquet links", len(links))
    return links


def infer_year_month_and_type(url: str):
    fn = Path(urlparse(url).path).name.lower()
    import re
    m = re.search(r"(20\d{2})[-_]?([01]\d)", fn)
    if not m:
        return None, None, None
    yy, mm = m.group(1), m.group(2)
    ttype = "other"
    if "yellow" in fn:
        ttype = "yellow"
    elif "green" in fn:
        ttype = "green"
    elif "hvf" in fn:
        ttype = "hvfvhv"
    elif "fhv" in fn:
        ttype = "fhv"
    return yy, mm, ttype


def build_month_set(start_ym: str, end_ym: str) -> set[str]:
    sy, sm = [int(x) for x in start_ym.split("-")]
    ey, em = [int(x) for x in end_ym.split("-")]
    cur = datetime.date(sy, sm, 1)
    end = datetime.date(ey, em, 1)
    out = set()
    while cur <= end:
        out.add(f"{cur.year}-{cur.month:02d}")
        # move to next month
        cur = (cur.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return out


def retry_loop(fn, attempts=3, base_delay=1.0, *args, **kwargs):
    """Simple retry with exponential backoff."""
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            backoff = base_delay * (2 ** (i - 1))
            logger.warning("Attempt %d/%d failed: %s. Backing off %.1fs", i, attempts, e, backoff)
            time.sleep(backoff)
    raise last_exc


def download_url_once(url: str, out_dir: Path, chunk_size: int = 4_194_304) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name
    out_path = out_dir / filename
    logger.info("Downloading: %s", url)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    return out_path


def download_url(url: str, out_dir: Path) -> Path:
    # Skip if exists
    filename = Path(urlparse(url).path).name
    out_path = out_dir / filename
    if out_path.exists():
        logger.info("SKIP (exists): %s", out_path)
        return out_path
    return retry_loop(download_url_once, attempts=3, base_delay=1.0, url=url, out_dir=out_dir)


def upload_file_once(local_path: Path, bucket: str, prefix: str) -> str:
    import boto3
    s3 = boto3.client("s3")
    key = f"{prefix.rstrip('/')}/{local_path.name}".lstrip("/")
    logger.info("Uploading %s -> s3://%s/%s", local_path, bucket, key)
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def upload_file_s3(local_path: Path, bucket: str, prefix: str) -> str:
    return retry_loop(upload_file_once, attempts=3, base_delay=1.0, local_path=local_path, bucket=bucket, prefix=prefix)


def load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        try:
            return json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Could not parse existing manifest, starting fresh.")
            return {"files": []}
    return {"files": []}


def save_manifest(man: dict):
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(json.dumps(man, indent=2), encoding="utf-8")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--s3-bucket", required=True, help="S3 bucket to upload to")
    p.add_argument("--s3-prefix", default="tlc/raw", help="S3 key prefix")
    p.add_argument("--dest", default="data/raw", help="Local base path")
    p.add_argument("--upload-start", default="2019-01", help="Upload start yyyy-mm")
    p.add_argument("--upload-end", default="2025-09", help="Upload end yyyy-mm")
    p.add_argument("--local-years", nargs="+", default=["2025"], help="Years to keep locally (e.g. 2025)")
    p.add_argument("--limit", type=int, default=0, help="Process only N files (for testing). 0 = all")
    p.add_argument("--dry-run", action="store_true", help="List actions but do not download/upload")
    p.add_argument("--cleanup-tmp", action="store_true", help="After upload, delete tmp_download files (if any)")
    args = p.parse_args()

    dest_root = Path(args.dest)
    links = find_parquet_links()
    upload_set = build_month_set(args.upload_start, args.upload_end)
    local_years = set(args.local_years)

    logger.info("Upload range months: %d", len(upload_set))
    logger.info("Local download years: %s", ",".join(sorted(local_years)))

    manifest = load_manifest()
    processed = 0

    for url in links:
        yy, mm, ttype = infer_year_month_and_type(url)
        if yy is None:
            continue
        ym = f"{yy}-{mm}"
        want_local = yy in local_years
        want_upload = ym in upload_set

        if not (want_local or want_upload):
            # skip entirely
            continue

        if args.dry_run:
            logger.info("DRY: %s local=%s upload=%s", url, want_local, want_upload)
            continue

        if args.limit and processed >= args.limit:
            logger.info("Reached --limit (%d) stopping", args.limit)
            break

        entry = {
            "url": url,
            "year_month": ym,
            "type": ttype,
            "downloaded": False,
            "uploaded": False,
            "errors": [],
            "size_bytes": None,
            "s3_uri": None,
            "timestamp": None,
        }

        try:
            # local download if desired
            local_path = None
            if want_local:
                out_dir = dest_root / ttype / yy
                try:
                    lp = download_url(url, out_dir)
                    entry["downloaded"] = True
                    entry["local_path"] = str(lp)
                    entry["size_bytes"] = lp.stat().st_size
                except Exception as e:
                    entry["errors"].append(f"download_error:{e}")
                    logger.exception("Download error for %s", url)
                    # continue to attempt upload if wanted (we'll try tmp download)
            # upload if desired
            if want_upload:
                # if we already have a local path (from local download), use it.
                if entry.get("local_path"):
                    lp = Path(entry["local_path"])
                    used_tmp = False
                else:
                    # download to tmp for upload
                    tmp_dir = dest_root / "tmp_download"
                    lp = download_url(url, tmp_dir)
                    used_tmp = True
                try:
                    s3uri = upload_file_s3(lp, args.s3_bucket, f"{args.s3_prefix}/{ttype}/{yy}")
                    entry["uploaded"] = True
                    entry["s3_uri"] = s3uri
                except Exception as e:
                    entry["errors"].append(f"upload_error:{e}")
                    logger.exception("Upload error for %s", lp)
                # cleanup tmp-only download
                if args.cleanup_tmp and used_tmp:
                    try:
                        lp.unlink(missing_ok=True)
                        logger.info("Deleted tmp file %s", lp)
                    except Exception:
                        logger.warning("Could not delete tmp %s", lp)
            entry["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
        except Exception as e:
            entry["errors"].append(f"unexpected:{e}")
            logger.exception("Unexpected error for %s", url)

        # append entry only after attempted processing
        manifest["files"].append(entry)
        processed += 1
        save_manifest(manifest)  # flush after every processed file

    save_manifest(manifest)
    logger.info("Wrote manifest -> %s", MANIFEST_FILE)
    logger.info("Done.")


if __name__ == "__main__":
    main()