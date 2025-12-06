# scripts/list_targets.py
import argparse
from urllib.parse import urlparse
from pathlib import Path
import datetime
import requests
from bs4 import BeautifulSoup

TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

def find_parquet_links(page_url=TLC_PAGE):
    r = requests.get(page_url, timeout=30)
    r.raise_for_status()
    from urllib.parse import urljoin
    soup = BeautifulSoup(r.text, "html.parser")
    links = [urljoin(page_url, a["href"]) for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".parquet")]
    return sorted(set(links), key=lambda u: urlparse(u).path)

def infer_year_month_and_type(url: str):
    fn = Path(urlparse(url).path).name.lower()
    import re
    m = re.search(r"(20\d{2})[-_]?([01]\d)", fn)
    if not m:
        return None, None, None
    yy, mm = m.group(1), m.group(2)
    if "yellow" in fn:
        ttype = "yellow"
    elif "green" in fn:
        ttype = "green"
    elif "hvf" in fn:
        ttype = "hvfvhv"
    elif "fhv" in fn:
        ttype = "fhv"
    else:
        ttype = "other"
    return yy, mm, ttype

def build_month_set(start_ym: str, end_ym: str) -> set:
    sy, sm = [int(x) for x in start_ym.split("-")]
    ey, em = [int(x) for x in end_ym.split("-")]
    cur = datetime.date(sy, sm, 1)
    end = datetime.date(ey, em, 1)
    out = set()
    while cur <= end:
        out.add(f"{cur.year}-{cur.month:02d}")
        cur = (cur.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload-start", default="2019-01")
    parser.add_argument("--upload-end", default="2025-09")
    parser.add_argument("--local-years", nargs="+", default=["2025"])
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    links = find_parquet_links()
    upload_set = build_month_set(args.upload_start, args.upload_end)
    local_years = set(args.local_years)

    out = []
    for url in links:
        yy, mm, ttype = infer_year_month_and_type(url)
        if yy is None:
            continue
        ym = f"{yy}-{mm}"
        want_local = yy in local_years
        want_upload = ym in upload_set
        if want_local or want_upload:
            out.append({"url": url, "year_month": ym, "type": ttype, "local": want_local, "upload": want_upload})
        if len(out) >= args.limit:
            break

    for i, e in enumerate(out, 1):
        print(f"{i:02d}. {e['url']}  local={e['local']} upload={e['upload']}")

if __name__ == "__main__":
    main()
