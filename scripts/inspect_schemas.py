"""Inspect schemas of all cab types to understand column differences."""
import pandas as pd
from pathlib import Path

data_root = Path('../data/raw')

cab_types = {
    'yellow': data_root / 'yellow/2025/yellow_tripdata_2025-01.parquet',
    'green': data_root / 'green/2025/green_tripdata_2025-01.parquet',
    'fhv': data_root / 'fhv/2025/fhv_tripdata_2025-01.parquet',
    'fhvhv': data_root / 'fhvhv/2025/fhvhv_tripdata_2025-01.parquet'
}

print("=" * 80)
print("NYC TAXI DATA SCHEMA INSPECTION - January 2025")
print("=" * 80)

for cab_type, path in cab_types.items():
    if not path.exists():
        print(f"\n❌ {cab_type.upper()}: FILE NOT FOUND")
        continue
    
    print(f"\n{'=' * 80}")
    print(f"📊 {cab_type.upper()} TAXI")
    print(f"{'=' * 80}")
    
    df = pd.read_parquet(path)
    print(f"Shape: {df.shape}")
    print(f"\nColumns ({len(df.columns)}):")
    print(df.dtypes.to_string())
    print(f"\nSample (first 2 rows):")
    print(df.head(2).to_string())
    print(f"\nNull counts:")
    print(df.isnull().sum()[df.isnull().sum() > 0].to_string())

print("\n" + "=" * 80)
print("COMPARISON SUMMARY")
print("=" * 80)

# Load all and compare columns
schemas = {}
for cab_type, path in cab_types.items():
    if path.exists():
        df = pd.read_parquet(path)
        schemas[cab_type] = set(df.columns)

print("\nCommon columns across ALL types:")
if schemas:
    common = set.intersection(*schemas.values())
    print(sorted(common))

print("\nYellow-only columns:")
if 'yellow' in schemas:
    yellow_only = schemas['yellow'] - set.union(*[v for k, v in schemas.items() if k != 'yellow'])
    print(sorted(yellow_only))

print("\nGreen-only columns:")
if 'green' in schemas:
    green_only = schemas['green'] - set.union(*[v for k, v in schemas.items() if k != 'green'])
    print(sorted(green_only))

print("\nFHV-only columns:")
if 'fhv' in schemas:
    fhv_only = schemas['fhv'] - set.union(*[v for k, v in schemas.items() if k != 'fhv'])
    print(sorted(fhv_only))

print("\nFHVHV-only columns:")
if 'fhvhv' in schemas:
    fhvhv_only = schemas['fhvhv'] - set.union(*[v for k, v in schemas.items() if k != 'fhvhv'])
    print(sorted(fhvhv_only))
