#!/usr/bin/env bash

set -u

cd "$(dirname "$0")"
mkdir -p logs

LOCK_FILE="logs/run_llm_report_pipeline.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] LLM report pipeline is already running. Exiting."
  exit 0
fi

PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN"
  exit 1
fi

DB_PATH="press_unified.db"
TS="$(date "+%Y%m%d_%H%M%S")"
LOG_FILE="logs/llm_report_pipeline_${TS}.log"

PROVIDER="${LLM_PROVIDER:-openai}"
MODEL="${LLM_MODEL:-google/gemma-4-26B-A4B-it}"
if [ "$PROVIDER" = "google" ]; then
  MODEL="${LLM_MODEL:-gemini-2.0-flash}"
  API_BASE="${LLM_API_BASE:-${GOOGLE_API_BASE:-https://generativelanguage.googleapis.com/v1beta}}"
else
  API_BASE="${LLM_API_BASE:-${OPENAI_API_BASE:-http://222.110.207.7:8000/v1}}"
fi
MAX_OUTPUTS="${LLM_MAX_OUTPUTS:-5}"
TEMP="${LLM_TEMPERATURE:-0.2}"

echo "[INFO] LLM report pipeline start"
echo "[INFO] provider=${PROVIDER}, model=${MODEL}"
echo "[INFO] log=${LOG_FILE}"

API_KEY="${LLM_API_KEY:-}"
if [ -z "$API_KEY" ]; then
  if [ "$PROVIDER" = "google" ]; then
    API_KEY="${GOOGLE_API_KEY:-}"
  else
    API_KEY="${OPENAI_API_KEY:-}"
  fi
fi

if [ -z "$API_KEY" ] && { [ "$PROVIDER" = "google" ] || [ "$API_BASE" = "https://api.openai.com/v1" ]; }; then
  echo "[WARN] API key is empty for provider=${PROVIDER}. Running dry-run mode."
  "$PYTHON_BIN" llm_report_pipeline.py \
    --db-path "$DB_PATH" \
    --provider "$PROVIDER" \
    --model "$MODEL" \
    --api-base "$API_BASE" \
    --max-outputs "$MAX_OUTPUTS" \
    --temperature "$TEMP" \
    --dry-run \
    > "$LOG_FILE" 2>&1
else
  "$PYTHON_BIN" llm_report_pipeline.py \
    --db-path "$DB_PATH" \
    --provider "$PROVIDER" \
    --model "$MODEL" \
    --api-base "$API_BASE" \
    --api-key "$API_KEY" \
    --max-outputs "$MAX_OUTPUTS" \
    --temperature "$TEMP" \
    > "$LOG_FILE" 2>&1
fi

EXIT_CODE=$?
cat "$LOG_FILE"
echo
exit "$EXIT_CODE"
