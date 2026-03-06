#!/usr/bin/env bash

set -u

cd "$(dirname "$0")"
mkdir -p logs

LOCK_FILE="logs/run_attachment_pipeline.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] Attachment pipeline is already running. Exiting."
  exit 0
fi

PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN"
  exit 1
fi

DB_PATH="press_unified.db"
TS="$(date "+%Y%m%d_%H%M%S")"
LOG_FILE="logs/attachment_pipeline_${TS}.log"

echo "[INFO] Attachment pipeline start"
echo "[INFO] Log file: $LOG_FILE"

"$PYTHON_BIN" attachment_pipeline.py \
  --db-path "$DB_PATH" \
  --download-dir "attachment_store" \
  --batch-size 120 \
  --max-retry 3 \
  > "$LOG_FILE" 2>&1

EXIT_CODE=$?
cat "$LOG_FILE"
echo
exit "$EXIT_CODE"
