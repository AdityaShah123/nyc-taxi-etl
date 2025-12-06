# scripts/analytics_pandas.py
"""
Analytics (pandas + pyarrow) with automatic column detection.
Processes Parquet files under an input base (recursively) for the given years,
computes:
 - avg_fare_per_mile_by_hour
 - trips_by_dow
 - busiest_pickup
 - busiest_dropoff

Writes parquet outputs to output_base.
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
import argparse
from collections import defaultdict
import math

# Heuristics for column detection
PICKUP_CANDIDATES = ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime", "request_datetime", "pickup_dt", "pickup_date", "pickup_time"]
DROPOFF_CANDIDATES = ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime", "dropOff_datetime", "dropoff_dt"]
DISTANCE_CANDIDATES = ["trip_distance", "trip_miles", "trip_distance_miles", "trip_distance_km"]
FARE_CANDIDATES = ["fare_amount", "total_amount", "total_fare", "fare"]
# fallback components to build fare if missing
FARE_COMPONENTS = [["base_passenger_fare","tips","tolls"], ["base_passenger_fare","tips"], ["fare_amount","tip_amount","tolls_amount"]]
PU_CANDIDATES = ["PULocationID","PUlocationID","PUlocationid","PUlocation_id","pu_location_id"]
DO_CANDIDATES = ["DOLocationID","DOlocationID","DOlocationid","DOlocation_id","do_location_id"]

def detect_columns(schema_names):
    s = [n.lower() for n in schema_names]
    def find(cands):
        for c in cands:
            if c in schema_names:
                return c
        # try case-insensitive contains
        for c in cands:
            for name in schema_names:
                if c.lower() == name.lower():
                    return name
        for c in cands:
            for name in schema_names:
                if c.lower() in name.lower():
                    return name
        return None

    pickup = find(PICKUP_CANDIDATES)
    dropoff = find(DROPOFF_CANDIDATES)
    distance = find(DISTANCE_CANDIDATES)
    fare = find(FARE_CANDIDATES)
    # try to find fare by components if fare not found
    fare_components_found = None
    if not fare:
        for comp_list in FARE_COMPONENTS:
            found = [c for c in comp_list if any(name.lower() == c.lower() or c.lower() in name.lower() for name in schema_names)]
            if len(found) > 0:
                fare_components_found = found
                break

    pu = find(PU_CANDIDATES)
    do = find(DO_CANDIDATES)

    return {
        "pickup": pickup,
        "dropoff": dropoff,
        "distance": distance,
        "fare": fare,
        "fare_components": fare_components_found,
        "pu": pu,
        "do": do
    }

def read_table_to_df(pqfile, cols_to_read=None):
    try:
        if cols_to_read:
            tbl = pq.read_table(pqfile, columns=cols_to_read)
        else:
            tbl = pq.read_table(pqfile)
        return tbl.to_pandas()
    except Exception as e:
        # fallback: read full file
        print(f"Read error for {pqfile} with cols {cols_to_read}: {e}. Trying full read.")
        tbl = pq.read_table(pqfile)
        return tbl.to_pandas()

def process_file(path, detected):
    # build list of columns to read (only those present)
    schema = pq.read_schema(path)
    names = schema.names
    cols = []
    for key in [detected["pickup"], detected["dropoff"], detected["distance"], detected["fare"], detected["pu"], detected["do"]]:
        if key and key in names:
            cols.append(key)
    # also include potential fare components
    if detected.get("fare_components"):
        for c in detected["fare_components"]:
            for name in names:
                if c.lower() in name.lower():
                    cols.append(name)
    # dedupe
    cols = list(dict.fromkeys(c for c in cols if c is not None))
    if not cols:
        # read minimal fallback columns to at least get time
        try:
            df = pq.read_table(path).to_pandas()
            return df
        except Exception as e:
            print("Failed to read file fully:", path, e)
            return pd.DataFrame()

    df = read_table_to_df(path, cols_to_read=cols)
    # normalize names to working columns
    # detect actual pickup/dropoff col
    if detected["pickup"] and detected["pickup"] not in df.columns:
        # find alternate by substring
        for c in df.columns:
            if "pickup" in c.lower():
                detected["pickup"] = c
                break
    if detected["dropoff"] and detected["dropoff"] not in df.columns:
        for c in df.columns:
            if "drop" in c.lower():
                detected["dropoff"] = c
                break
    if detected["distance"] and detected["distance"] not in df.columns:
        for c in df.columns:
            if "mile" in c.lower() or "distance" in c.lower():
                detected["distance"] = c
                break
    # cast timestamps
    if detected["pickup"] and detected["pickup"] in df.columns:
        df[detected["pickup"]] = pd.to_datetime(df[detected["pickup"]], errors="coerce")
    if detected["dropoff"] and detected["dropoff"] in df.columns:
        df[detected["dropoff"]] = pd.to_datetime(df[detected["dropoff"]], errors="coerce")
    # numeric casts
    if detected["distance"] and detected["distance"] in df.columns:
        df[detected["distance"]] = pd.to_numeric(df[detected["distance"]], errors="coerce")
    # compute fare_amount if present, else combine components
    if detected["fare"] and detected["fare"] in df.columns:
        df["fare_amount_calc"] = pd.to_numeric(df[detected["fare"]], errors="coerce")
    elif detected.get("fare_components"):
        # try to sum components present
        comp_cols = []
        for comp in detected["fare_components"]:
            for name in df.columns:
                if comp.lower() in name.lower():
                    comp_cols.append(name)
        if comp_cols:
            df["fare_amount_calc"] = df[comp_cols].apply(lambda row: sum([float(x) if (not pd.isna(x)) else 0.0 for x in row]), axis=1)
        else:
            df["fare_amount_calc"] = None
    else:
        df["fare_amount_calc"] = None

    # derive features
    # pickup hour / dow
    if detected["pickup"] and detected["pickup"] in df.columns:
        df["pickup_hour"] = df[detected["pickup"]].dt.hour
        df["pickup_dow"] = df[detected["pickup"]].dt.day_name().str[:3]
        df["pickup_month"] = df[detected["pickup"]].dt.to_period("M").astype(str)
    # compute fare_per_mile if possible
    if (detected["distance"] and detected["distance"] in df.columns) and ("fare_amount_calc" in df.columns):
        df = df[(df[detected["distance"]] > 0) & (~df["fare_amount_calc"].isna())]
        df["fare_per_mile"] = df["fare_amount_calc"] / df[detected["distance"]]
    else:
        df["fare_per_mile"] = None

    # standardize PU/DO columns if present
    if detected["pu"] and detected["pu"] in df.columns:
        df["PULocationID_std"] = df[detected["pu"]].astype('Int64')
    else:
        # try case-insensitive match
        for c in df.columns:
            if "pu" in c.lower() and "location" in c.lower():
                df["PULocationID_std"] = pd.to_numeric(df[c], errors="coerce").astype('Int64')
                break

    if detected["do"] and detected["do"] in df.columns:
        df["DOLocationID_std"] = df[detected["do"]].astype('Int64')
    else:
        for c in df.columns:
            if "do" in c.lower() and "location" in c.lower():
                df["DOLocationID_std"] = pd.to_numeric(df[c], errors="coerce").astype('Int64')
                break

    return df

def accumulate_aggregates(df, agg):
    if df.empty:
        return
    # fare per hour
    if "pickup_hour" in df.columns and "fare_per_mile" in df.columns:
        g = df.groupby("pickup_hour")["fare_per_mile"].agg(["sum","count"]).reset_index()
        for _, r in g.iterrows():
            h = int(r["pickup_hour"])
            agg["fare_hour"].setdefault(h, {"sum":0.0,"count":0})
            agg["fare_hour"][h]["sum"] += float(r["sum"] if not pd.isna(r["sum"]) else 0.0)
            agg["fare_hour"][h]["count"] += int(r["count"])
    # dow
    if "pickup_dow" in df.columns:
        g2 = df.groupby("pickup_dow").size().reset_index(name="trip_count")
        for _, r in g2.iterrows():
            agg["dow"].setdefault(r["pickup_dow"],0)
            agg["dow"][r["pickup_dow"]] += int(r["trip_count"])
    # pu/do
    if "PULocationID_std" in df.columns:
        g3 = df.groupby("PULocationID_std").size().reset_index(name="trip_count")
        for _, r in g3.iterrows():
            if pd.isna(r["PULocationID_std"]): continue
            agg["pu"].setdefault(int(r["PULocationID_std"]),0)
            agg["pu"][int(r["PULocationID_std"])] += int(r["trip_count"])
    if "DOLocationID_std" in df.columns:
        g4 = df.groupby("DOLocationID_std").size().reset_index(name="trip_count")
        for _, r in g4.iterrows():
            if pd.isna(r["DOLocationID_std"]): continue
            agg["do"].setdefault(int(r["DOLocationID_std"]),0)
            agg["do"][int(r["DOLocationID_std"])] += int(r["trip_count"])
    # monthly fare
    if "pickup_month" in df.columns and "fare_per_mile" in df.columns:
        gm = df.groupby("pickup_month")["fare_per_mile"].agg(["sum","count"]).reset_index()
        for _, r in gm.iterrows():
            m = r["pickup_month"]
            agg["month"].setdefault(m, {"sum":0.0,"count":0})
            agg["month"][m]["sum"] += float(r["sum"] if not pd.isna(r["sum"]) else 0.0)
            agg["month"][m]["count"] += int(r["count"])

def finalize_and_write(agg, out_base: Path):
    out_base.mkdir(parents=True, exist_ok=True)
    # hour
    rows=[]
    for h,v in sorted(agg["fare_hour"].items()):
        avg = (v["sum"]/v["count"]) if v["count"]>0 else None
        rows.append({"pickup_hour":int(h),"avg_fare_per_mile":avg,"trip_count":v["count"]})
    pd.DataFrame(rows).sort_values("pickup_hour").to_parquet(out_base / "avg_fare_per_mile_by_hour.parquet", index=False)
    # dow
    pd.DataFrame([{"pickup_dow":k,"trip_count":v} for k,v in agg["dow"].items()]).sort_values("pickup_dow").to_parquet(out_base / "trips_by_dow.parquet", index=False)
    # pu
    pd.DataFrame([{"PULocationID":k,"trip_count":v} for k,v in sorted(agg["pu"].items(), key=lambda x:-x[1])]).to_parquet(out_base / "busiest_pickup.parquet", index=False)
    # do
    pd.DataFrame([{"DOLocationID":k,"trip_count":v} for k,v in sorted(agg["do"].items(), key=lambda x:-x[1])]).to_parquet(out_base / "busiest_dropoff.parquet", index=False)
    # month
    if agg["month"]:
        rows = [{"month":k, "avg_fare_per_mile": (v["sum"]/v["count"] if v["count"]>0 else None), "trip_count":v["count"]} for k,v in sorted(agg["month"].items())]
        pd.DataFrame(rows).to_parquet(out_base / "monthly_fare_trend.parquet", index=False)

def main(input_base, output_base, years):
    p = Path(input_base)
    # find files with years in name or nested in year folders
    files = []
    for y in years:
        q = p / str(y)
        if q.exists():
            files += sorted([str(f) for f in q.glob("*.parquet")])
    files += sorted([str(f) for f in p.rglob("*.parquet") if any(str(y) in f.name for y in years)])
    files = list(dict.fromkeys(files))
    if not files:
        raise SystemExit(f"No parquet files found under {input_base} for years {years}")
    print(f"Found {len(files)} parquet files — processing sequentially.")
    # detect columns from first file schema
    schema = pq.read_schema(files[0]).names
    detected = detect_columns(schema)
    print("Detected columns:", detected)
    agg = {"fare_hour":{}, "dow":{}, "pu":{}, "do":{}, "month":{}}
    for i,f in enumerate(files,1):
        print(f"[{i}/{len(files)}] processing {f}")
        try:
            df = process_file(f, detected.copy())
            accumulate_aggregates(df, agg)
        except Exception as e:
            print("Error processing", f, e)
    finalize_and_write(agg, Path(output_base))
    print("Wrote analytics to", output_base)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-base", required=True, help="top-level path containing parquet files for a taxi type")
    parser.add_argument("--output-base", required=True, help="where analytics parquet will be written")
    parser.add_argument("--years", nargs="+", type=int, default=[2025], help="years to include")
    args = parser.parse_args()
    main(args.input_base, args.output_base, args.years)
