# Creating four fully-updated Jupyter notebooks for yellow, green, fhv, fhvhv analytics.
# Notebooks include:
# - Robust repo-root discovery (works when opened from /notebooks or repo root)
# - Automatic schema detection (pickup/dropoff/distance/fare/PU/DO)
# - Aggregation: avg fare/mile (when available), trip counts by hour/day/month, busiest PU/DO
# - Fallback computations for datasets missing fare/distance (hourly/monthly counts)
# - Trip duration histogram (sample-based)
# - Optional zone lookup: loads data/misc/taxi_zone_lookup.csv if present; attempts download if missing
# - Clear instructions and save reminder so outputs embed in notebook.
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
from pathlib import Path
BASE = Path("/data")
BASE.mkdir(exist_ok=True)

TYPES = ["yellow","green","fhv","fhvhv"]

COMMON_HEADER = """# Parameters (edit if necessary)
from pathlib import Path
# compute repo root dynamically so notebook works whether opened from repo root or /notebooks/
NOTEBOOK_PATH = Path.cwd().resolve()
# If this notebook sits in /notebooks/, repo root is parent, otherwise adjust as needed
if (NOTEBOOK_PATH.name == "notebooks"):
    REPO_ROOT = NOTEBOOK_PATH.parent
else:
    REPO_ROOT = NOTEBOOK_PATH
print("Notebook path:", NOTEBOOK_PATH)
print("Repo root assumed:", REPO_ROOT)

DATA_TYPE = "{dtype}"     # yellow, green, fhv, fhvhv
DATA_BASE = str(REPO_ROOT / "data" / "raw")    # top-level raw data folder (repo_root/data/raw)
YEARS = [2025]            # years to include, e.g. [2019,2020]
OUTPUT_BASE = str(REPO_ROOT / "data" / "local_output" / "analytics")  # where analytics parquet will be written
MISC_BASE = str(REPO_ROOT / "data" / "misc")  # for taxi_zone_lookup.csv
Path(MISC_BASE).mkdir(parents=True, exist_ok=True)

print("DATA_BASE:", DATA_BASE)
print("Looking for files under:", Path(DATA_BASE) / DATA_TYPE)
"""

COMMON_IMPORTS = """# Imports
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')
%matplotlib inline
"""

DETECT_AND_COMPUTE = """
# --- file discovery ---
p = Path(DATA_BASE) / DATA_TYPE
files = []
for y in YEARS:
    q = p / str(y)
    if q.exists():
        files += sorted([str(f) for f in q.glob("*.parquet")])
# also match files with year in filename
files += sorted([str(f) for f in (p.rglob("*.parquet")) if any(str(y) in f.name for y in YEARS)])
files = list(dict.fromkeys(files))
print("Found", len(files), "files for", DATA_TYPE, "at", p.resolve())
files[:10]

# --- schema detection helper ---
def detect_columns_from_schema(schema_names):
    schema = schema_names
    def find_any(cands):
        for c in cands:
            for name in schema:
                if c.lower() == name.lower():
                    return name
        for c in cands:
            for name in schema:
                if c.lower() in name.lower():
                    return name
        return None

    pickup = find_any(["tpep_pickup_datetime","lpep_pickup_datetime","pickup_datetime","request_datetime","pickup_dt"])
    dropoff = find_any(["tpep_dropoff_datetime","lpep_dropoff_datetime","dropoff_datetime","dropOff_datetime","dropoff_dt"])
    distance = find_any(["trip_distance","trip_miles","trip_distance_miles"])
    fare = find_any(["fare_amount","total_amount","base_passenger_fare","total_fare"])
    pu = find_any(["PULocationID","PUlocationID","PUlocationid"])
    do = find_any(["DOLocationID","DOlocationID","DOlocationid"])
    return {"pickup":pickup,"dropoff":dropoff,"distance":distance,"fare":fare,"pu":pu,"do":do}

# If no files found, create empty placeholders and stop gracefully
if not files:
    print("No files found for", DATA_TYPE, " — edit DATA_BASE / DATA_TYPE / YEARS at top")
    df_hour = pd.DataFrame(columns=["pickup_hour","avg_fare_per_mile","trip_count"])
    df_dow = pd.DataFrame(columns=["pickup_dow","trip_count"])
    df_pu = pd.DataFrame(columns=["PULocationID","trip_count"])
    df_do = pd.DataFrame(columns=["DOLocationID","trip_count"])
    df_month = pd.DataFrame(columns=["month","avg_fare_per_mile","trip_count"])
else:
    # detect schema from first file
    sample_schema = pq.read_schema(files[0]).names
    detected = detect_columns_from_schema(sample_schema)
    print("Detected sample schema mapping:", detected)
    # aggregator containers
    agg_fare_hour = dict()  # hour -> {"sum":..,"count":..}
    agg_dow = dict()
    agg_pu = dict()
    agg_do = dict()
    agg_month = dict()

    # process each file (memory-conscious: read only useful columns)
    for i,f in enumerate(files,1):
        print(f"[{i}/{len(files)}] reading", f)
        schema = pq.read_schema(f).names
        cols_to_read = []
        for c in [detected["pickup"], detected["dropoff"], detected["distance"], detected["fare"], detected["pu"], detected["do"]]:
            if c and c in schema:
                cols_to_read.append(c)
        # read table
        try:
            table = pq.read_table(f, columns=cols_to_read if cols_to_read else None)
            df = table.to_pandas()
        except Exception as e:
            print("read error", e)
            df = pd.DataFrame()

        if df.empty:
            continue

        # normalize timestamps & features
        if detected["pickup"] and detected["pickup"] in df.columns:
            df[detected["pickup"]] = pd.to_datetime(df[detected["pickup"]], errors="coerce")
            df["pickup_hour"] = df[detected["pickup"]].dt.hour
            df["pickup_dow"] = df[detected["pickup"]].dt.day_name().str[:3]
            df["pickup_month"] = df[detected["pickup"]].dt.to_period("M").astype(str)

        if detected["dropoff"] and detected["dropoff"] in df.columns:
            df[detected["dropoff"]] = pd.to_datetime(df[detected["dropoff"]], errors="coerce")

        if detected["distance"] and detected["distance"] in df.columns:
            df[detected["distance"]] = pd.to_numeric(df[detected["distance"]], errors="coerce")

        if detected["fare"] and detected["fare"] in df.columns:
            df["fare_amount_calc"] = pd.to_numeric(df[detected["fare"]], errors="coerce")

        # compute fare_per_mile when possible
        if (detected["distance"] and detected["distance"] in df.columns) and ("fare_amount_calc" in df.columns):
            df = df[(df[detected["distance"]] > 0) & (~df["fare_amount_calc"].isna())]
            df["fare_per_mile"] = df["fare_amount_calc"] / df[detected["distance"]]
        else:
            df["fare_per_mile"] = None

        # PU/DO standardization
        if detected["pu"] and detected["pu"] in df.columns:
            df["PULocationID_std"] = pd.to_numeric(df[detected["pu"]], errors="coerce").astype("Int64")
        if detected["do"] and detected["do"] in df.columns:
            df["DOLocationID_std"] = pd.to_numeric(df[detected["do"]], errors="coerce").astype("Int64")

        # aggregate fare/hour
        if "pickup_hour" in df.columns and "fare_per_mile" in df.columns:
            g = df.groupby("pickup_hour")["fare_per_mile"].agg(["sum","count"]).reset_index()
            for _,r in g.iterrows():
                h = int(r["pickup_hour"])
                if h not in agg_fare_hour:
                    agg_fare_hour[h] = {"sum":0.0,"count":0}
                agg_fare_hour[h]["sum"] += float(r["sum"] if not pd.isna(r["sum"]) else 0.0)
                agg_fare_hour[h]["count"] += int(r["count"])
        # dow
        if "pickup_dow" in df.columns:
            g2 = df.groupby("pickup_dow").size().reset_index(name="trip_count")
            for _,r in g2.iterrows():
                agg_dow[r["pickup_dow"]] = agg_dow.get(r["pickup_dow"],0) + int(r["trip_count"])
        # pu/do
        if "PULocationID_std" in df.columns:
            g3 = df.groupby("PULocationID_std").size().reset_index(name="trip_count")
            for _,r in g3.iterrows():
                if pd.isna(r["PULocationID_std"]): continue
                k = int(r["PULocationID_std"])
                agg_pu[k] = agg_pu.get(k,0) + int(r["trip_count"])
        if "DOLocationID_std" in df.columns:
            g4 = df.groupby("DOLocationID_std").size().reset_index(name="trip_count")
            for _,r in g4.iterrows():
                if pd.isna(r["DOLocationID_std"]): continue
                k = int(r["DOLocationID_std"])
                agg_do[k] = agg_do.get(k,0) + int(r["trip_count"])
        # monthly fare aggregation
        if "pickup_month" in df.columns and "fare_per_mile" in df.columns:
            gm = df.groupby("pickup_month")["fare_per_mile"].agg(["sum","count"]).reset_index()
            for _,r in gm.iterrows():
                m = r["pickup_month"]
                if m not in agg_month:
                    agg_month[m] = {"sum":0.0,"count":0}
                agg_month[m]["sum"] += float(r["sum"] if not pd.isna(r["sum"]) else 0.0)
                agg_month[m]["count"] += int(r["count"])

    # finalize dataframes
    df_hour = pd.DataFrame([{"pickup_hour":h, "avg_fare_per_mile": (vals["sum"]/vals["count"] if vals["count"]>0 else None), "trip_count": vals["count"]} for h,vals in sorted(agg_fare_hour.items())])
    # ensure hours 0-23 present (even if empty)
    if df_hour.empty:
        df_hour = pd.DataFrame([{"pickup_hour":h,"avg_fare_per_mile":None,"trip_count":0} for h in range(24)])
    else:
        # fill missing hours with zero counts
        existing = set(df_hour["pickup_hour"].tolist())
        for h in range(24):
            if h not in existing:
                df_hour = pd.concat([df_hour, pd.DataFrame([{"pickup_hour":h,"avg_fare_per_mile":None,"trip_count":0}])], ignore_index=True)
        df_hour = df_hour.sort_values("pickup_hour").reset_index(drop=True)

    df_dow = pd.DataFrame([{"pickup_dow":k,"trip_count":v} for k,v in agg_dow.items()]).sort_values("pickup_dow")
    df_pu = pd.DataFrame([{"PULocationID":k,"trip_count":v} for k,v in sorted(agg_pu.items(), key=lambda x:-x[1])])
    df_do = pd.DataFrame([{"DOLocationID":k,"trip_count":v} for k,v in sorted(agg_do.items(), key=lambda x:-x[1])])
    if agg_month:
        df_month = pd.DataFrame([{"month":m,"avg_fare_per_mile": (vals["sum"]/vals["count"] if vals["count"]>0 else None),"trip_count": vals["count"]} for m,vals in sorted(agg_month.items())])
    else:
        df_month = pd.DataFrame()
"""

PLOT_CELL = """
# Robust plotting & finalization (handles missing fare/distance)
import matplotlib.pyplot as plt
import pandas as pd
%matplotlib inline

# Hourly: prefer df_hour if it has trip_count > 0, else compute df_hour_counts via sample read
if 'df_hour' in globals() and not df_hour.empty and df_hour['trip_count'].sum() > 0:
    print("== Hourly avg fare (first rows) ==")
    display(df_hour.head(24))
    # show trip counts by hour
    plt.figure(figsize=(10,4))
    plt.bar(df_hour['pickup_hour'].astype(int), df_hour['trip_count'])
    plt.title(f"Trip counts by hour - {DATA_TYPE}")
    plt.xlabel("Hour")
    plt.ylabel("Trip count")
    plt.xticks(range(0,24))
    plt.grid(axis='y', alpha=0.3)
    plt.show()

    # only plot avg_fare_per_mile when meaningful
    if df_hour['avg_fare_per_mile'].notna().sum() > 0:
        plt.figure(figsize=(10,4))
        plt.plot(df_hour['pickup_hour'], df_hour['avg_fare_per_mile'], marker='o')
        plt.title(f"Avg fare per mile by hour - {DATA_TYPE}")
        plt.xlabel("Hour")
        plt.ylabel("Avg fare per mile ($)")
        plt.xticks(range(0,24))
        plt.grid(True)
        plt.show()
    else:
        print("No fare/distance info available to compute avg fare per mile for this dataset.")
else:
    # compute df_hour_counts quickly from raw files if available
    try:
        import pyarrow.parquet as pq, pandas as pd
        from collections import Counter
        hour_counter = Counter()
        for f in files:
            try:
                schema = pq.read_schema(f).names
                pickup = next((c for c in schema if 'pickup' in c.lower()), None)
                if not pickup:
                    continue
                tbl = pq.read_table(f, columns=[pickup]).to_pandas()
                tbl[pickup] = pd.to_datetime(tbl[pickup], errors='coerce')
                hrs = tbl[pickup].dt.hour.dropna().astype(int)
                hour_counter.update(hrs.tolist())
            except Exception:
                continue
        df_hour = pd.DataFrame([{'pickup_hour':h,'trip_count':hour_counter.get(h,0)} for h in range(24)])
        print("== Hourly trip counts (computed) ==")
        display(df_hour)
        plt.figure(figsize=(10,4))
        plt.bar(df_hour['pickup_hour'], df_hour['trip_count'])
        plt.title(f"Trip counts by hour - {DATA_TYPE}")
        plt.xlabel("Hour")
        plt.ylabel("Trip count")
        plt.show()
    except Exception as e:
        print("Could not compute hourly counts:", e)

# Trips by day of week
print("\\n== Trips by day of week ==")
if 'df_dow' in globals() and not df_dow.empty and df_dow['trip_count'].sum() > 0:
    display(df_dow)
    order = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    try:
        df_dow2 = df_dow.set_index('pickup_dow').reindex(order).fillna(0).reset_index()
    except Exception:
        df_dow2 = df_dow
    plt.figure(figsize=(8,4))
    plt.bar(df_dow2['pickup_dow'], df_dow2['trip_count'])
    plt.title(f"Trip count by day of week - {DATA_TYPE}")
    plt.show()
else:
    print("No trip-by-day data available.")

# Top pickup/dropoff
print("\\n== Top 10 pickup zones ==")
if 'df_pu' in globals() and not df_pu.empty:
    display(df_pu.head(10))
    plt.figure(figsize=(10,4))
    plt.bar(df_pu.head(10)['PULocationID'].astype(str), df_pu.head(10)['trip_count'])
    plt.title(f"Top 10 busiest pickup zones - {DATA_TYPE}")
    plt.show()
else:
    print("No pickup zone data available.")

print("\\n== Top 10 dropoff zones ==")
if 'df_do' in globals() and not df_do.empty:
    display(df_do.head(10))
    plt.figure(figsize=(10,4))
    plt.bar(df_do.head(10)['DOLocationID'].astype(str), df_do.head(10)['trip_count'])
    plt.title(f"Top 10 busiest dropoff zones - {DATA_TYPE}")
    plt.show()
else:
    print("No dropoff zone data available.")

# Trip duration distribution (sample)
print("\\n== Trip duration distribution (sample) ==")
duration_drawn = False
try:
    import pyarrow.parquet as pq, pandas as pd
    sample = files[0] if files else None
    if sample:
        tbl = pq.read_table(sample)
        df_sample = tbl.to_pandas()
        # find pickup/drop columns
        pickup = next((c for c in df_sample.columns if 'pickup' in c.lower()), None)
        drop = next((c for c in df_sample.columns if 'drop' in c.lower()), None)
        if pickup and drop and pickup in df_sample.columns and drop in df_sample.columns:
            df_sample[pickup] = pd.to_datetime(df_sample[pickup], errors='coerce')
            df_sample[drop] = pd.to_datetime(df_sample[drop], errors='coerce')
            df_sample['trip_duration_min'] = (df_sample[drop] - df_sample[pickup]).dt.total_seconds()/60.0
            dur = df_sample['trip_duration_min'].dropna()
            dur = dur[(dur>0)&(dur<1000)]
            if len(dur)>0:
                plt.figure(figsize=(8,4))
                plt.hist(dur, bins=60)
                plt.title(f"Trip duration distribution (sample) - {DATA_TYPE}")
                plt.xlim(0,200)
                plt.show()
                duration_drawn = True
except Exception as e:
    print("Could not compute durations:", e)

if not duration_drawn:
    print("Trip duration distribution unavailable (no pickup/drop timestamps).")

# Monthly aggregates fallback: trip counts if fare missing
print("\\n== Monthly aggregates ==")
if 'df_month' in globals() and not df_month.empty and df_month['trip_count'].sum()>0:
    display(df_month)
    if 'avg_fare_per_mile' in df_month.columns and df_month['avg_fare_per_mile'].notna().sum()>0:
        plt.figure(figsize=(10,4))
        plt.plot(df_month['month'], df_month['avg_fare_per_mile'], marker='o')
        plt.xticks(rotation=45)
        plt.title(f"Monthly avg fare per mile - {DATA_TYPE}")
        plt.show()
    else:
        dfm = df_month.sort_values('month')
        plt.figure(figsize=(10,4))
        plt.plot(dfm['month'], dfm['trip_count'], marker='o')
        plt.xticks(rotation=45)
        plt.title(f"Monthly trip counts - {DATA_TYPE}")
        plt.show()
else:
    # compute monthly counts quickly
    try:
        import pyarrow.parquet as pq, pandas as pd
        from collections import Counter
        month_counter = Counter()
        for f in files:
            try:
                schema = pq.read_schema(f).names
                pickup = next((c for c in schema if 'pickup' in c.lower()), None)
                if not pickup:
                    continue
                tbl = pq.read_table(f, columns=[pickup]).to_pandas()
                tbl[pickup] = pd.to_datetime(tbl[pickup], errors='coerce')
                months = tbl[pickup].dt.to_period('M').dropna().astype(str)
                month_counter.update(months.tolist())
            except Exception:
                continue
        if month_counter:
            df_month = pd.DataFrame(sorted([{'month':m,'trip_count':c} for m,c in month_counter.items()], key=lambda x:x['month']))
            display(df_month)
            plt.figure(figsize=(10,4))
            plt.plot(df_month['month'], df_month['trip_count'], marker='o')
            plt.xticks(rotation=45)
            plt.title(f"Monthly trip counts - {DATA_TYPE}")
            plt.show()
        else:
            print("No monthly counts found.")
    except Exception as e:
        print("Could not compute monthly counts:", e)
"""

ZONE_CELL = """
# Optional: map PULocationID/DOLocationID to human-readable zone names using TLC lookup CSV
# Put taxi_zone_lookup.csv under data/misc/ (automatically created) as 'taxi_zone_lookup.csv'
# If not present, this cell will attempt to download it from the official NYC TLC URL (internet required).
import os
from pathlib import Path
tz_path = Path(MISC_BASE) / "taxi_zone_lookup.csv"
if not tz_path.exists():
    print("taxi_zone_lookup.csv not found at", tz_path)
    print("Attempting to download from NYC TLC (requires internet)...")
    try:
        import requests
        url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        tz_path.write_bytes(r.content)
        print("Downloaded taxi_zone_lookup.csv to", tz_path)
    except Exception as e:
        print("Download failed:", e)
# if file present, load it
if tz_path.exists():
    tz = pd.read_csv(tz_path)
    # normalize column names
    tz_cols = [c.lower() for c in tz.columns]
    # expected columns include LocationID, Borough, Zone
    if 'locationid' in tz_cols:
        # find proper names
        loc = tz.columns[tz_cols.index('locationid')]
        zonecol = None
        borcol = None
        for i,c in enumerate(tz_cols):
            if 'zone' in c and zonecol is None:
                zonecol = tz.columns[i]
            if 'borough' in c and borcol is None:
                borcol = tz.columns[i]
        tz = tz.rename(columns={loc: 'LocationID'})
        if zonecol: tz = tz.rename(columns={zonecol: 'Zone'})
        if borcol: tz = tz.rename(columns={borcol: 'Borough'})
        tz['LocationID'] = pd.to_numeric(tz['LocationID'], errors='coerce').astype('Int64')
        print("Loaded taxi_zone_lookup with", len(tz), "rows")
    else:
        print("taxi_zone_lookup.csv doesn't contain LocationID column; skipping mapping")
else:
    print("No taxi_zone_lookup.csv available; top-10 charts will show numeric IDs")
"""

README_CELL = """
## Notes & Usage
- Run kernel -> Restart & Run All to execute the notebook. After it finishes, **File -> Save** to embed plots/images into the .ipynb file.
- The notebook auto-detects schema differences between yellow/green/fhv/fhvhv datasets and will compute fare-based metrics only when fare+distance are present. Otherwise it falls back to trip counts and durations.
- To map zone IDs to names, place `taxi_zone_lookup.csv` into `data/misc/` or allow the notebook to attempt downloading it (internet required).
- If you want a Spark-based notebook for EMR, ask "create pyspark notebooks" and I'll prepare them.
"""

# generate notebooks
created = []
for dtype in TYPES:
    nb = new_notebook()
    cells = []
    cells.append(new_markdown_cell(f"# {dtype.capitalize()} Taxi — Full Analytics Notebook\n\nAuto-generated: robust detection, fallback analytics, zone mapping. Run all cells and save the notebook to embed plots."))
    cells.append(new_code_cell(COMMON_HEADER.format(dtype=dtype)))
    cells.append(new_code_cell(COMMON_IMPORTS))
    cells.append(new_code_cell(DETECT_AND_COMPUTE))
    cells.append(new_code_cell(PLOT_CELL))
    cells.append(new_code_cell(ZONE_CELL))
    cells.append(new_markdown_cell(README_CELL))
    nb['cells'] = cells
    path = BASE / f"analytics_{dtype}_full.ipynb"
    with open(path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    created.append(path)

for p in created:
    print("Wrote", p)

{"notebooks":[str(p) for p in created]}

