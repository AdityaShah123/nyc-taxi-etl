# scripts/inspect_parquet_pyarrow.py
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from pathlib import Path
p = Path("data/raw/yellow/2025")
files = list(p.glob("*.parquet"))
if not files:
    # try nested files
    files = list(p.rglob("*.parquet"))
if not files:
    raise SystemExit("No parquet files found under " + str(p))

print("Using parquet file:", files[0])
tbl = pq.read_table(str(files[0]))
print("\n=== Columns ===")
print(tbl.schema.names)
print("\n=== Schema (arrow) ===")
print(tbl.schema)
print("\n=== First 5 rows (pandas) ===")
print(tbl.to_pandas().head(5))
print("\nRows in this file:", tbl.num_rows)
