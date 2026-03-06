#!/usr/bin/env bash

# Unified press ingestion runner for Linux/macOS.
# Usage: ./run_daily_ingest.sh [API_KEY]

set -u

cd "$(dirname "$0")"
mkdir -p logs

LOCK_FILE="logs/run_daily_ingest.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] Another ingest run is already in progress. Exiting."
  exit 0
fi

SERVICE_KEY=""
if [ -n "${1:-}" ]; then
  SERVICE_KEY="$1"
elif [ -f "service_key.txt" ]; then
  SERVICE_KEY="$(tr -d '\r\n' < service_key.txt)"
fi

if [ -z "$SERVICE_KEY" ]; then
  echo "[ERROR] service_key.txt is missing or API key is empty."
  exit 1
fi

WINDOW_DAYS=5
if [ -f "ingest_config.yaml" ]; then
  CONFIG_DAYS="$(grep -E '^[[:space:]]*date_window_days:' ingest_config.yaml | grep -o -E '[0-9]+' | head -1)"
  if [ -n "${CONFIG_DAYS:-}" ]; then
    WINDOW_DAYS="$CONFIG_DAYS"
  fi
fi

END_DATE="$(date "+%Y%m%d")"
if date --version >/dev/null 2>&1; then
  START_DATE="$(date -d "${WINDOW_DAYS} days ago" "+%Y%m%d")"
else
  START_DATE="$(date -v-"${WINDOW_DAYS}"d "+%Y%m%d")"
fi

PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN"
  exit 1
fi

DB_PATH="press_unified.db"
TS="$(date "+%Y%m%d_%H%M%S")"
LOG_FILE="logs/ingest_${TS}.log"

echo "[INFO] Ingest start: ${START_DATE} ~ ${END_DATE}"
echo "[INFO] Log file: ${LOG_FILE}"

"$PYTHON_BIN" unified_press_ingest.py \
  --service-key "$SERVICE_KEY" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --db-path "$DB_PATH" \
  --config "ingest_config.yaml" \
  > "$LOG_FILE" 2>&1

EXIT_CODE=$?

cat "$LOG_FILE"
echo

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "[INFO] Ingest completed successfully."
  RUN_ATTACHMENT_PIPELINE="${RUN_ATTACHMENT_PIPELINE:-1}"
  if [ "$RUN_ATTACHMENT_PIPELINE" = "1" ] && [ -f "attachment_pipeline.py" ]; then
    ATTACH_LOG_FILE="logs/attachment_pipeline_${TS}.log"
    echo "[INFO] Starting attachment pipeline..."
    "$PYTHON_BIN" attachment_pipeline.py \
      --db-path "$DB_PATH" \
      --download-dir "attachment_store" \
      --batch-size 120 \
      --max-retry 3 \
      > "$ATTACH_LOG_FILE" 2>&1
    ATTACH_EXIT=$?
    cat "$ATTACH_LOG_FILE"
    if [ "$ATTACH_EXIT" -ne 0 ]; then
      echo "[WARN] Attachment pipeline failed. Exit code: $ATTACH_EXIT"
    fi
  fi
else
  echo "[ERROR] Ingest failed. Exit code: $EXIT_CODE"
fi

exit "$EXIT_CODE"
