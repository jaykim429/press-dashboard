#!/usr/bin/env bash

# Unified press ingestion runner for Linux/macOS.
# Usage: ./run_daily_ingest.sh [API_KEY]

set -u

# Keep date window and "today" semantics consistent with Korea operations.
export TZ="Asia/Seoul"

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
MAIN_LOG_FILE="logs/ingest_${TS}.log"
COMMON_ARGS=(
  --service-key "$SERVICE_KEY"
  --start-date "$START_DATE"
  --end-date "$END_DATE"
  --db-path "$DB_PATH"
  --config "ingest_config.yaml"
)
EXTRA_ARGS=()
if [ "${FAIL_ON_COLLECTOR_ERROR:-1}" = "1" ]; then
  EXTRA_ARGS+=(--fail-on-collector-error)
fi
EXTRA_ARGS+=(--collector-retry-attempts "${COLLECTOR_RETRY_ATTEMPTS:-2}")
EXTRA_ARGS+=(--collector-retry-backoff-sec "${COLLECTOR_RETRY_BACKOFF_SEC:-2.0}")

echo "[INFO] Ingest start: ${START_DATE} ~ ${END_DATE}"
echo "[INFO] Main log file: ${MAIN_LOG_FILE}"

run_collector_step() {
  local collector_key="$1"
  local step_log="logs/ingest_${collector_key}_${TS}.log"
  echo "[INFO] Running collector: ${collector_key}"
  PYTHONUNBUFFERED=1 "$PYTHON_BIN" unified_press_ingest.py \
    "${COMMON_ARGS[@]}" \
    "${EXTRA_ARGS[@]}" \
    --only-collector "$collector_key" \
    --skip-analytics \
    --preview-json "ingest_preview_${collector_key}.json" \
    > "$step_log" 2>&1
  local step_code=$?
  cat "$step_log" | tee -a "$MAIN_LOG_FILE"
  echo
  echo >> "$MAIN_LOG_FILE"
  if [ "$step_code" -ne 0 ]; then
    echo "[WARN] Collector ${collector_key} failed. Exit code: ${step_code}"
    return 1
  fi
  return 0
}

RUN_SPLIT_COLLECTORS="${RUN_SPLIT_COLLECTORS:-1}"
EXIT_CODE=0

if [ "$RUN_SPLIT_COLLECTORS" = "1" ]; then
  if [ -n "${INGEST_COLLECTORS:-}" ]; then
    read -r -a COLLECTORS <<< "$INGEST_COLLECTORS"
  else
    COLLECTORS=(
      api
      fss
      fss-admin
      ksd
      ksd-rule
      fsc
      fsc-admin
      fsc-reply
      bok
      kfb
      fsec
      krx-recent
      krx-notice
      kofia-recent
      kofia-notice
    )
  fi

  echo "[INFO] Split collector mode enabled."
  : > "$MAIN_LOG_FILE"
  for collector in "${COLLECTORS[@]}"; do
    if ! run_collector_step "$collector"; then
      EXIT_CODE=1
    fi
  done

  ANALYTICS_LOG="logs/ingest_analytics_${TS}.log"
  echo "[INFO] Running final analytics pre-computation..."
  PYTHONUNBUFFERED=1 "$PYTHON_BIN" unified_press_ingest.py \
    "${COMMON_ARGS[@]}" \
    "${EXTRA_ARGS[@]}" \
    --analytics-only \
    --preview-json "ingest_preview_daily.json" \
    > "$ANALYTICS_LOG" 2>&1
  ANALYTICS_EXIT=$?
  cat "$ANALYTICS_LOG" | tee -a "$MAIN_LOG_FILE"
  echo
  if [ "$ANALYTICS_EXIT" -ne 0 ]; then
    echo "[WARN] Analytics pre-computation failed. Exit code: $ANALYTICS_EXIT"
    EXIT_CODE=1
  fi
else
  PYTHONUNBUFFERED=1 "$PYTHON_BIN" unified_press_ingest.py \
    "${COMMON_ARGS[@]}" \
    "${EXTRA_ARGS[@]}" \
    --preview-json "ingest_preview_daily.json" \
    > "$MAIN_LOG_FILE" 2>&1
  EXIT_CODE=$?
  cat "$MAIN_LOG_FILE"
  echo
fi

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "[INFO] Ingest completed successfully."
  RUN_ATTACHMENT_PIPELINE="${RUN_ATTACHMENT_PIPELINE:-1}"
  if [ "$RUN_ATTACHMENT_PIPELINE" = "1" ] && [ -f "attachment_pipeline.py" ]; then
    ATTACH_LOG_FILE="logs/attachment_pipeline_${TS}.log"
    echo "[INFO] Starting attachment pipeline..."
    PYTHONUNBUFFERED=1 "$PYTHON_BIN" attachment_pipeline.py \
      --db-path "$DB_PATH" \
      --download-dir "attachment_store" \
      --batch-size 120 \
      --max-retry 3 \
      ${ATTACH_CORE_ONLY:+--core-only} \
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
