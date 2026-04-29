#!/usr/bin/env bash

set -u

cd "$(dirname "$0")"
mkdir -p logs tmp/internal_rule_cache

LOCK_FILE="logs/run_internal_rule_impact_pipeline.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] Internal rule impact pipeline is already running. Exiting."
  exit 0
fi

PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python"
fi

DB_PATH="${DB_PATH:-press_unified.db}"
RULE_DIR="${INTERNAL_RULE_DIR:-3. 내규목록}"
DAYS="${INTERNAL_RULE_IMPACT_DAYS:-30}"
FROM_DATE="${INTERNAL_RULE_IMPACT_FROM_DATE:-}"
TO_DATE="${INTERNAL_RULE_IMPACT_TO_DATE:-}"
MAX_GUIDANCE="${INTERNAL_RULE_IMPACT_MAX_GUIDANCE:-20}"
MAX_RULES="${INTERNAL_RULE_IMPACT_MAX_RULES:-30}"
TS="$(date "+%Y%m%d_%H%M%S")"
PAYLOAD_JSON="tmp/internal_rule_impact_payload_${TS}.json"
LOG_FILE="logs/internal_rule_impact_pipeline_${TS}.log"

echo "[INFO] Internal rule impact pipeline start"
echo "[INFO] rule_dir=${RULE_DIR}"
echo "[INFO] log=${LOG_FILE}"

ARGS=(
  internal_rule_impact_builder.py
  --db-path "$DB_PATH"
  --rule-dir "$RULE_DIR"
  --days "$DAYS"
  --max-guidance "$MAX_GUIDANCE"
  --max-rules "$MAX_RULES"
  --output-json "$PAYLOAD_JSON"
)

if [ -n "$FROM_DATE" ]; then
  ARGS+=(--from-date "$FROM_DATE")
fi
if [ -n "$TO_DATE" ]; then
  ARGS+=(--to-date "$TO_DATE")
fi

"$PYTHON_BIN" "${ARGS[@]}" > "$LOG_FILE" 2>&1
BUILD_EXIT=$?
cat "$LOG_FILE"
echo
if [ "$BUILD_EXIT" -ne 0 ]; then
  exit "$BUILD_EXIT"
fi

echo "[INFO] LLM report generation start"
LLM_PROMPT_PROFILE=internal_rule_impact LLM_MAX_OUTPUTS=1 ./run_llm_report_pipeline.sh
