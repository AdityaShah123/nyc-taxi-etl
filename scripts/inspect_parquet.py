# scripts/inspect_parquet.py
from pyspark.sql import SparkSession
from pathlib import Path
import sys

spark = SparkSession.builder.master("local[*]").appName("inspect-parquet").getOrCreate()

p = Path("data/raw/yellow/2025")
if not p.exists():
    print("Path not found:", p)
    sys.exit(1)

# read all parquet files in the folder
df = spark.read.parquet(str(p))
print("=== Columns ===")
print(df.columns)
print("\n=== Schema ===")
df.printSchema()
print("\n=== 5 sample rows ===")
df.show(5, truncate=False)

print("\n=== Count rows (approx) ===")
print(df.count())

spark.stop()
