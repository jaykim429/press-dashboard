#!/usr/bin/env bash

# Single-instance dashboard runner for Linux.
# Usage:
#   ./run_dashboard.sh            # start if not running
#   ./run_dashboard.sh --restart  # restart

set -u

cd "$(dirname "$0")"
mkdir -p logs

HOST="0.0.0.0"
PORT="80"
DB_PATH="press_unified.db"
PYTHON_BIN=".venv/bin/python"
LOG_FILE="logs/dashboard.log"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "[ERROR] Python executable not found: $PYTHON_BIN"
  exit 1
fi

if [ ! -f "$DB_PATH" ]; then
  echo "[ERROR] DB file not found: $DB_PATH"
  exit 1
fi

if [ "${1:-}" = "--restart" ]; then
  pkill -f "local_dashboard.py" || true
  sleep 1
fi

if pgrep -f "local_dashboard.py --db-path $DB_PATH --host $HOST --port $PORT" >/dev/null 2>&1; then
  echo "[INFO] Dashboard is already running on ${HOST}:${PORT}"
  pgrep -af "local_dashboard.py --db-path $DB_PATH --host $HOST --port $PORT"
  exit 0
fi

nohup "$PYTHON_BIN" local_dashboard.py \
  --db-path "$DB_PATH" \
  --host "$HOST" \
  --port "$PORT" \
  > "$LOG_FILE" 2>&1 &

sleep 1
PID="$(pgrep -f "local_dashboard.py --db-path $DB_PATH --host $HOST --port $PORT" | head -1)"

if [ -n "${PID:-}" ]; then
  echo "[INFO] Dashboard started (PID: $PID) on http://${HOST}:${PORT}"
  
  # DB 캐시 웜업 (백그라운드로 무거운 쿼리 실행시켜 SQLite 페이지 캐싱 유도)
  echo "[INFO] Initiating cache warm-up..."
  (
    sleep 2
    # Stats (가장 무거운 쿼리)
    curl -s "http://127.0.0.1:${PORT}/api/stats?top_n=8" > /dev/null
    # Articles 첫 페이지
    curl -s "http://127.0.0.1:${PORT}/api/articles?page=1&page_size=10" > /dev/null
    # Filters
    curl -s "http://127.0.0.1:${PORT}/api/filters" > /dev/null
    
    echo "[INFO] Cache warm-up completed." >> "$LOG_FILE"
  ) &
  
  exit 0
fi

echo "[ERROR] Dashboard failed to start. Check $LOG_FILE"
exit 1
