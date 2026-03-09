#!/usr/bin/env bash

set -u

cd "$(dirname "$0")"
mkdir -p logs

WINDOW_DAYS=3
if [ -f "ingest_config.yaml" ]; then
  CONFIG_DAYS="$(grep -E '^[[:space:]]*date_window_days:' ingest_config.yaml | grep -o -E '[0-9]+' | head -1)"
  if [ -n "${CONFIG_DAYS:-}" ]; then
    WINDOW_DAYS="$CONFIG_DAYS"
  fi
fi

if date --version >/dev/null 2>&1; then
  FROM_DATE="$(date -d "${WINDOW_DAYS} days ago" "+%Y-%m-%d")"
  TO_DATE="$(date "+%Y-%m-%d")"
else
  FROM_DATE="$(date -v-"${WINDOW_DAYS}"d "+%Y-%m-%d")"
  TO_DATE="$(date "+%Y-%m-%d")"
fi

echo "[INFO] Full pipeline start: ${FROM_DATE} ~ ${TO_DATE}"

./run_daily_ingest.sh "$@"
INGEST_EXIT=$?
if [ "$INGEST_EXIT" -ne 0 ]; then
  echo "[ERROR] Ingest step failed."
  exit "$INGEST_EXIT"
fi

PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN"
  exit 1
fi

"$PYTHON_BIN" report_builder.py \
  --db-path "press_unified.db" \
  --from-date "$FROM_DATE" \
  --to-date "$TO_DATE" \
  --topic "${REPORT_TOPIC:-core regulation/admin-guidance impact analysis}" \
  --max-sources "${REPORT_MAX_SOURCES:-80}" \
  --max-chars-per-source "${REPORT_MAX_CHARS:-3000}" \
  ${REPORT_CORE_ONLY:+--core-only} \
  ${REPORT_ALL_CHANNELS:+--all-channels} \
  --output-json "logs/report_payload_${FROM_DATE}_${TO_DATE}.json"

./run_llm_report_pipeline.sh
LLM_EXIT=$?
if [ "$LLM_EXIT" -ne 0 ]; then
  echo "[WARN] LLM step failed. You can rerun ./run_llm_report_pipeline.sh"
fi

echo "[INFO] Full pipeline completed."
exit 0

