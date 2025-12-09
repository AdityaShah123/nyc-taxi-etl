#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SRC=${SRC:-"$ROOT/data/raw"}
BUCKET=${BUCKET:-""}
PREFIX=${PREFIX:-"tlc/raw"}

if [ -z "$BUCKET" ]; then
	echo "BUCKET is required (target S3 bucket)" >&2
	exit 1
fi

aws s3 sync "$SRC" "s3://$BUCKET/$PREFIX" --exclude "*.tmp"
