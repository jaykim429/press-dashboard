#!/bin/bash
# =====================================================================
# 통합 보도자료 일일 수집 - Linux/macOS 용 스크립트
# 옵션: ./run_daily_ingest.sh [API_KEY]
# =====================================================================

cd "$(dirname "$0")"

# 1. API 키 로딩
SERVICE_KEY=""
if [ -n "$1" ]; then
    SERVICE_KEY="$1"
elif [ -f "service_key.txt" ]; then
    SERVICE_KEY=$(cat service_key.txt | tr -d '\r\n')
fi

if [ -z "$SERVICE_KEY" ]; then
    echo "[ERROR] service_key.txt 파일이 없거나 API 키가 입력되지 않았습니다."
    exit 1
fi

echo "[INFO] API 키 로드 완료"

# 2. 날짜 계산 (ingest_config.yaml의 date_window_days 확인, 기본 5일)
WINDOW_DAYS=5
if [ -f "ingest_config.yaml" ]; then
    # 단순 grep 파싱 (의존성 최소화)
    CONFIG_DAYS=$(grep -E '^[[:space:]]*date_window_days:' ingest_config.yaml | grep -o -E '[0-9]+' | head -1)
    if [ -n "$CONFIG_DAYS" ]; then
        WINDOW_DAYS=$CONFIG_DAYS
    fi
fi

# macOS와 Linux의 date 명령어 차이 호환 처리
END_DATE=$(date "+%Y%m%d")
if date --version >/dev/null 2>&1; then
    # Linux (GNU date)
    START_DATE=$(date -d "${WINDOW_DAYS} days ago" "+%Y%m%d")
else
    # macOS/BSD date
    START_DATE=$(date -v-${WINDOW_DAYS}d "+%Y%m%d")
fi

echo "[INFO] 시작 일자: $START_DATE"
echo "[INFO] 종료 일자: $END_DATE"

# 3. 환경 변수 설정 및 디렉토리 준비
export PYTHONIOENCODING=utf-8
DB_PATH="press_unified.db"
mkdir -p logs
LOG_FILE="logs/ingest_$(date "+%Y%m%d").log"

echo "[INFO] 수집을 시작합니다. 로그 파일: $LOG_FILE"

# 4. 파이썬 스크립트 실행
.venv/bin/python unified_press_ingest.py \
    --service-key "$SERVICE_KEY" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --db-path "$DB_PATH" \
    --config "ingest_config.yaml" > "$LOG_FILE" 2>&1

EXIT_CODE=$?

cat "$LOG_FILE"
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo "[INFO] (종료코드: 0) 데이터 수집을 성공적으로 완료하였습니다."
else
    echo "[ERROR] (종료코드: $EXIT_CODE) 데이터 수집 중 오류가 발생하였습니다."
fi
