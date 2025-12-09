# Architecture Overview

This project ingests NYC TLC taxi parquet data, stages it locally or on S3, transforms it with Spark, and produces curated datasets and analytics artifacts. Key elements:

- **Sources:** NYC TLC monthly parquet files (yellow, green, FHV, FHVHV) retrieved via `scripts/download_tlc_range_s3.py`.
- **Staging:** Local `data/raw/` or `s3a://<bucket>/tlc/raw/` with a `manifest.json` for tracking.
- **Processing:** PySpark jobs in `spark_jobs/` (notably `etl_yellow_s3.py`) reading from raw and writing cleaned partitions.
- **Outputs:** `data/curated/` or `s3a://<bucket>/tlc/curated/`, plus analytics charts under `data/local_output/analytics/`.
- **Environment:** Spark 4.0.1 with `hadoop-aws` connector; Python venv `sparkprojenv` hosts dependencies.

## Data Flow
1. Download monthly parquet files (optionally upload directly to S3).
2. Track downloaded files in `data/raw/manifest.json`.
3. Run Spark ETL to clean, partition, and write curated parquet.
4. Generate analytics (pandas/pyarrow scripts or notebooks) from curated data.
5. Publish outputs locally or to S3 for downstream reporting.

## Operational Notes
- Keep `pyspark` and the bundled Spark binary on the same version (4.0.1).
- Provide AWS credentials and `JAVA_HOME`/`SPARK_HOME` environment variables before running Spark jobs.
- `jars/hadoop-aws-3.4.1.jar` must be on the Spark classpath for S3 access.
- Use `PROJECT_FULL_DOCUMENTATION.md` for detailed setup, IAM expectations, and troubleshooting.
