# scripts/check_analytics_pandas.py
import pandas as pd
from pathlib import Path
p = Path("data/local_output/analytics/yellow")

for fname in ["avg_fare_per_mile_by_hour.parquet", "trips_by_dow.parquet", "busiest_pickup.parquet", "busiest_dropoff.parquet"]:
    path = p / fname
    if path.exists():
        print(f"\n=== {fname} ===")
        df = pd.read_parquet(path)
        print(df.head(10))
    else:
        print(f"Missing: {path}")
