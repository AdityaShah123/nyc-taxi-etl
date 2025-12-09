# NYC Taxi ETL

Production-minded ETL and analytics pipeline for NYC TLC taxi data. Runs locally or against S3 with PySpark 4.0.1. This README is a concise operator guide; the detailed reference is in `PROJECT_FULL_DOCUMENTATION.md`.

## What's in here
- Spark jobs: `spark_jobs/` (`etl_yellow_s3.py`, `analytics_yellow_s3.py`, `utils.py`)
- Utilities: `scripts/` (download/upload, retries, analytics with pandas/pyarrow)
- Notebooks: `notebooks/` (exploration and advanced visualizations)
- Data layout: `data/raw/`, `data/curated/`, `data/local_output/analytics/`
- Spark distribution: `spark-4.0.1-bin-hadoop3/`
- Python env: `sparkprojenv/` (preferred) and `requirements.txt`
- Full reference: `PROJECT_FULL_DOCUMENTATION.md`

## Prerequisites
- Java 17 (set `JAVA_HOME`)
- Python 3.8+ with `pip`
- Spark 4.0.1 at project root (matches `pyspark==4.0.1`)
- AWS credentials for S3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`)

## Quick start (local)
```powershell
python -m venv sparkprojenv
.\sparkprojenv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Running tasks
- Smoke test Spark: `python smoke_test.py`
- Download TLC range and send to S3 (example): `python scripts/download_tlc_range_s3.py --s3-bucket <bucket> --s3-prefix tlc/raw --upload-start 2025-01 --upload-end 2025-12`
- Run Spark ETL (S3): `spark-submit spark_jobs/etl_yellow_s3.py --input-base s3a://<bucket>/tlc/raw --output-base s3a://<bucket>/tlc/curated --year 2025 --month 01`
- Local analytics (pandas/pyarrow): `python scripts/analytics_pandas.py --input-base data/raw --output-base data/local_output/analytics`
- Visual notebooks: open `notebooks/advanced_visualizations.ipynb` with the `sparkprojenv` kernel

## Data locations
- Raw: `data/raw/` (or `s3a://<bucket>/tlc/raw/`)
- Curated: `data/curated/` (or `s3a://<bucket>/tlc/curated/`)
- Outputs: `data/local_output/analytics/`

## Environment variables (PowerShell sample)
```powershell
$env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME = "$PWD\\spark-4.0.1-bin-hadoop3"
$env:AWS_ACCESS_KEY_ID = "<key>"
$env:AWS_SECRET_ACCESS_KEY = "<secret>"
$env:AWS_DEFAULT_REGION = "us-east-1"
```

## Troubleshooting
- Ensure `sparkprojenv` is active before running Python scripts.
- Keep Spark binary and `pyspark` versions aligned (4.0.1).
- For S3 access issues, verify credentials and bucket policy.
- See `PROJECT_FULL_DOCUMENTATION.md` for full troubleshooting and architecture notes.
