# NYC Taxi ETL — Full Project Documentation

Generated: December 5, 2025
Path: `PROJECT_FULL_DOCUMENTATION.md` (project root)

---

**Purpose:** Single-file reference containing everything needed to run, develop, and maintain the NYC Taxi ETL pipeline locally and with S3.

Contents:
- System requirements and runtimes
- Python and Java dependencies (versions)
- Spark binary and PySpark info
- Local file layout and important paths
- Cloud (S3) layout and required IAM/credentials
- Environment variables to set
- Quick start (step-by-step) for a new developer
- Common commands and examples
- Troubleshooting checklist and notes

---

## 1) System & Runtime

- Operating System: Windows (PowerShell 5.1) is primary here; Linux/macOS supported.
- Java: OpenJDK / Temurin 17.0.17 installed at:
  - `C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot`
  - Verify: `java -version`
- Python: 3.8+ recommended; project venv at `sparkprojenv/`.
- Disk: allocate 100GB+ for raw and processed data.

---

## 2) Key Project Artifacts & Locations (local)

Project root: `c:\All Data\Studies\Msc Rutgers\Semester 3\Cloud computing\Project\nyc-taxi-etl`

Important folders:
- `data/raw/` — downloaded raw Parquet files and `manifest.json`
- `data/curated/` — cleaned/curated parquet outputs
- `data/local_output/analytics/` — local analytics outputs
- `spark_jobs/` — Spark ETL scripts (`etl_yellow_s3.py`, `analytics_yellow_s3.py`, `utils.py`)
- `scripts/` — utilities (download, upload, analytics scripts)
- `notebooks/` — Jupyter notebooks for exploration
- `jars/` — `hadoop-aws-3.4.1.jar` (S3 connector)
- `sparkprojenv/` — Python virtual environment
- `spark-4.0.1-bin-hadoop3/` — Spark binary distribution
- `requirements.txt` — Python dependencies
- `smoke_test.py` — quick Spark smoke test

---

## 3) Python Dependencies (from `requirements.txt`)

Primary packages used (with versions present in repo):
- `pyspark==4.0.1`
- `py4j==0.10.9.9`
- `pyarrow==22.0.0`
- `pandas==2.3.3`
- `numpy==2.3.5`
- `boto3==1.40.74`, `botocore==1.40.74`, `s3transfer==0.14.0`
- `beautifulsoup4==4.14.2`, `requests==2.32.5`, `urllib3==2.5.0`
- Visualization: `matplotlib==3.10.7`, `seaborn==0.13.2`, `pillow==12.0.0`
- Utilities: `python-dateutil==2.9.0.post0`, `pytz==2025.2`, `tzdata==2025.2`, `certifi==2025.11.12`, plus other helpers listed in `requirements.txt`.

Note: Keep `pyspark` version aligned with the Spark binary (4.0.1 ↔ 4.0.1).

---

## 4) Java / Spark / Hadoop

- Spark binary used: `spark-4.0.1-bin-hadoop3/` located at the project root.
- Use Spark 4.0.1 to match `pyspark==4.0.1`.
- Hadoop libraries are bundled inside the Spark distribution (Hadoop 3.x flavor used in the distribution).
- `jars/hadoop-aws-3.4.1.jar` provides S3 connector support — make sure Spark is aware of it (via `--jars` or `spark.driver.extraClassPath`).

---

## 5) Cloud (AWS S3) — layout and access

S3 layout expected by scripts and jobs (change `--s3-prefix` via arguments if needed):

s3://<bucket>/
- `tlc/raw/` — raw monthly Parquet files organized by taxi type (yellow/green/fhv/fhvhv)
- `tlc/curated/` — processed ETL outputs (partitioned by year/month)

Where scripts expect S3 data:
- `download_tlc_range_s3.py` uploads files to an S3 bucket provided by `--s3-bucket`.
- Spark jobs use `s3a://<bucket>/tlc/raw` and `s3a://<bucket>/tlc/curated` when reading/writing in production.

IAM permissions required on the AWS principal used (minimal S3):
- `s3:ListBucket` on bucket
- `s3:GetObject` on `arn:aws:s3:::<bucket>/*`
- `s3:PutObject` on `arn:aws:s3:::<bucket>/*`
- `s3:DeleteObject` (optional) on `arn:aws:s3:::<bucket>/*`

Credentials can be provided via:
- Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN`)
- `~/.aws/credentials` profile
- IAM role when running on EC2/EMR

---

## 6) Environment Variables (essential)

Set these before running any S3 or Spark commands.

Required:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:AWS_ACCESS_KEY_ID = "<your-access-key>"
$env:AWS_SECRET_ACCESS_KEY = "<your-secret>"
$env:AWS_DEFAULT_REGION = "us-east-1"
```

Recommended:
```powershell
$env:SPARK_HOME = "<project-root>\spark-4.0.1-bin-hadoop3"
$env:PYTHONPATH = "<project-root>\spark_jobs;$env:PYTHONPATH"
# Optional tuning
$env:SPARK_DRIVER_MEMORY = "4g"
$env:SPARK_EXECUTOR_MEMORY = "4g"
```

On Linux/macOS, set equivalents in `~/.bashrc` or `~/.zshrc`.

---

## 7) Quick Start — full step-by-step (new developer)

1) Clone repo and cd to project root
```powershell
git clone <repo-url>
cd nyc-taxi-etl
```

2) Install Java 17 (Adoptium recommended), verify:
```powershell
java -version
```

3) Create and activate Python venv, then install requirements:
```powershell
python -m venv sparkprojenv
.\sparkprojenv\Scripts\Activate.ps1
pip install -r requirements.txt
```

4) Download Spark 4.0.1 binary (Hadoop 3 flavor) and extract in project root:
```powershell
$SPARK_URL = "https://archive.apache.org/dist/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz"
Invoke-WebRequest -Uri $SPARK_URL -OutFile "spark-4.0.1-bin-hadoop3.tgz"
tar -xzf spark-4.0.1-bin-hadoop3.tgz
Remove-Item spark-4.0.1-bin-hadoop3.tgz
```

5) Set environment variables (PowerShell example):
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:SPARK_HOME = (Join-Path (Get-Location) "spark-4.0.1-bin-hadoop3")
$env:PATH = "$env:SPARK_HOME\bin;$env:JAVA_HOME\bin;$env:PATH"
# Set AWS creds for S3 access
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_DEFAULT_REGION = "us-east-1"
```

6) Smoke test Spark (local):
```powershell
python smoke_test.py
```
Expect it to print `Spark version:` and JVM java.version.

7) (Optional) Verify Spark CLI works:
```powershell
& ".\spark-4.0.1-bin-hadoop3\bin\spark-submit.cmd" --version
```

---

## 8) Typical Commands

- Download TLC data and upload to S3 (example):
```bash
python scripts/download_tlc_range_s3.py \
  --s3-bucket my-bucket \
  --s3-prefix tlc/raw \
  --upload-start 2025-01 \
  --upload-end 2025-12
```

- Run ETL (Spark):
```bash
spark-submit spark_jobs/etl_yellow_s3.py \
  --input-base s3a://my-bucket/tlc/raw \
  --output-base s3a://my-bucket/tlc/curated \
  --year 2025 --month 01
```

- Run local analytics (Pandas + PyArrow):
```bash
python scripts/analytics_pandas.py --input-base data/raw --output-base data/local_output/analytics
```

---

## 9) Data Flow (concise)

1. Scrape/download Parquet files from NYC TLC site (`scripts/download_tlc_range_s3.py`)
2. Store on local disk under `data/raw/` and track progress via `data/raw/manifest.json`
3. Upload to S3 (`s3://<bucket>/tlc/raw/`) using `boto3` in scripts (or `upload_to_s3.sh`)
4. Process data with Spark jobs (read from S3 `s3a://` or local `data/raw/`) in `spark_jobs/`
5. Write cleaned/curated output to `s3://<bucket>/tlc/curated/` or local `data/curated/`
6. Run analytics and visualization (scripts and notebooks)

---

## 10) Troubleshooting (common issues & fixes)

- UnsupportedClassVersionError (compiled by newer Java): ensure `JAVA_HOME` points to Java 17+.
- Conda / entry-point import errors (e.g., `AliasGenerator` from `pydantic`): use the venv `sparkprojenv` instead of conda.
- S3 AccessDenied: verify AWS credentials and IAM policy.
- PySpark import errors: activate venv and ensure `pyspark==4.0.1` is installed.
- Parquet read failures: ensure `pyarrow` is installed and up-to-date (`pip install --upgrade pyarrow`).

---

## 11) Minimal IAM policy (example)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

---

## 12) Checklist for starting from scratch
- [ ] Install Java 17+
- [ ] Create Python venv and install `requirements.txt`
- [ ] Download Spark 4.0.1 binary and extract at project root
- [ ] Ensure `jars/hadoop-aws-3.4.1.jar` is present or add to Spark classpath
- [ ] Set environment variables (Java, Spark, AWS creds)
- [ ] Run `python smoke_test.py` and confirm success

---

## 13) Where to look for code that matters
- Downloading and uploading: `scripts/download_tlc_range_s3.py`, `scripts/retry_failed.py`
- ETL jobs: `spark_jobs/etl_yellow_s3.py`, `spark_jobs/analytics_yellow_s3.py`
- ETL helpers: `spark_jobs/utils.py`
- Local analytics: `scripts/analytics_pandas.py`
- Spark test: `smoke_test.py` and `check_hadoop.py`

---

If you want, I can now:
- Commit this file to git for you and push (I can run `git add`/`commit`/`push`), or
- Export a shorter one-page `README` version, or
- Open the file in the editor for you to review and tweak.

Which would you like me to do next? (or reply "nothing" to finish)