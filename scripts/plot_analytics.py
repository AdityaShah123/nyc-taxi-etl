# scripts/plot_analytics.py
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

base = Path("data/local_output/analytics/yellow")

# 1: Avg fare per mile by hour
df = pd.read_parquet(base / "avg_fare_per_mile_by_hour.parquet")
df = df.sort_values("pickup_hour")
plt.figure(figsize=(8,4))
plt.plot(df["pickup_hour"], df["avg_fare_per_mile"], marker="o")
plt.title("Avg fare per mile by hour")
plt.xlabel("Hour of day")
plt.ylabel("Avg fare per mile ($)")
plt.grid(True)
plt.xticks(range(0,24))
plt.tight_layout()
plt.show()

# 2: Trip count by day of week
df2 = pd.read_parquet(base / "trips_by_dow.parquet")
df2 = df2.set_index("pickup_dow").reindex(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]).fillna(0)
plt.figure(figsize=(6,4))
df2["trip_count"].plot(kind="bar")
plt.title("Trip count by day of week")
plt.xlabel("")
plt.tight_layout()
plt.show()

# 3: Busiest pickup (top 10)
df3 = pd.read_parquet(base / "busiest_pickup.parquet").head(10)
plt.figure(figsize=(8,4))
plt.bar(df3["PULocationID"].astype(str), df3["trip_count"])
plt.title("Top 10 busiest pickup zones (PULocationID)")
plt.xlabel("PULocationID")
plt.tight_layout()
plt.show()

# 4: Trip duration distribution: requires original Parquet (demo using sample file)
# Not produced in pandas aggregates above; we will skip heavy histogram here or compute separately if needed.

# 5: Seasonal trend (monthly aggregates) - not computed above; propose to compute with a second run aggregating by month if you want.
