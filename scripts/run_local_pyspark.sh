#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SPARK_HOME=${SPARK_HOME:-"$ROOT/spark-4.0.1-bin-hadoop3"}
APP="$ROOT/spark_jobs/etl_yellow_s3.py"
INPUT_BASE=${INPUT_BASE:-"$ROOT/data/raw"}
OUTPUT_BASE=${OUTPUT_BASE:-"$ROOT/data/curated"}
YEAR=${YEAR:-2025}
MONTH=${MONTH:-01}

"$SPARK_HOME/bin/spark-submit" \
	--master local[*] \
	--jars "$ROOT/jars/hadoop-aws-3.4.1.jar" \
	"$APP" \
	--input-base "$INPUT_BASE" \
	--output-base "$OUTPUT_BASE" \
	--year "$YEAR" \
	--month "$MONTH"
