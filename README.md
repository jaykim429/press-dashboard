# 통합 보도자료 대시보드

금융규제 보도자료·규정 변경·행정지도 등을 자동 수집하고, 웹 대시보드에서 검색·열람·이메일 발송할 수 있는 시스템입니다.

---

## 아키텍처

```
GCP 서버 (34.30.218.173)
├── unified_press_ingest.py  ← 크론/수동 실행으로 DB 수집
├── local_dashboard.py       ← HTTP 서버 (포트 80)
├── press_unified.db         ← SQLite DB
└── systemd press-dashboard.service
```

---

## 주요 기능

| 기능 | 설명 |
|------|------|
| **보도자료 목록** | 기관·유형·날짜 필터, 페이징, 전문 검색 |
| **검색어 자동완성** | FTS5 기반 실시간 자동완성 |
| **상세 보기** | 원문 링크, 첨부파일 다운로드 |
| **알림 모달** | 오늘자 신규 자료 팝업 확인 |
| **관리자 페이지** | 오늘자 요약표 생성 및 이메일 발송 |
| **수신자 관리** | 이메일 수신자 추가/삭제 |

---

## 수집 기관 (ingest_config.yaml)

- 금융위원회 (FSC) — 보도자료, 행정지도, 규정 변경, 법령해석, 비조치의견서
- 금융감독원 (FSS) — 보도자료, 보도설명자료, 행정지도
- 한국은행 (BOK) — 보도자료
- 예탁결제원 (KSD) — 보도자료, 규정 변경
- 거래소 (KRX) — 최신 규정 변경
- 금융투자협회 (KOFIA) — 최신 규정 변경
- 금융연구원 (KFB), 금융감독원 전자공시 (FSEC) — 기타

---

## 디렉토리 구조

```
.
├── local_dashboard.py         # 웹 서버 (HTTP API + 정적 파일)
├── unified_press_ingest.py    # 데이터 수집 엔진
├── attachment_pipeline.py     # 첨부파일 다운로드/처리
├── document_text_extractor.py # 문서 텍스트 추출 (PDF/HWP)
├── hwp_text_extractor.py      # HWP 전용 추출
├── llm_report_pipeline.py     # LLM 보고서 생성 파이프라인
├── report_builder.py          # 보고서 빌더
├── ingest_config.yaml         # 수집 기관/채널 설정
├── requirements.txt           # Python 의존성
├── service_key.txt            # 공공데이터 API 키 (git 제외)
├── dashboard.html             # 대시보드 UI
├── admin.html                 # 관리자 페이지 UI
├── article.html               # 기사 상세 페이지 UI
├── login.html                 # 로그인 페이지
├── run_daily_ingest.sh        # 수집 실행 스크립트
├── run_dashboard.sh           # 대시보드 실행 스크립트
├── run_attachment_pipeline.sh # 첨부파일 파이프라인 실행
├── run_full_pipeline.sh       # 전체 파이프라인 실행
├── run_llm_report_pipeline.sh # LLM 보고서 파이프라인 실행
├── deploy/                    # 서버 배포 설정
└── logs/                      # 실행 로그
```

---

## 서버 설정 (GCP 최초 배포)

```bash
# 1. 저장소 클론
git clone https://github.com/jaykim429/press-dashboard.git
cd press-dashboard

# 2. 가상환경 생성 및 의존성 설치
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 3. API 키 설정
echo "YOUR_API_KEY" > service_key.txt

# 4. 첫 수집 실행
chmod +x run_daily_ingest.sh
./run_daily_ingest.sh

# 5. systemd 서비스 등록 (deploy/ 참고)
sudo systemctl enable press-dashboard
sudo systemctl start press-dashboard
```

---

## 일상 운영

### 대시보드 접속
```
http://34.30.218.173
ID: test123 / PW: test123
```

### 데이터 수집 (수동)
```bash
cd ~/press-dashboard
nohup ./run_daily_ingest.sh > logs/manual_$(date +%Y%m%d_%H%M).log 2>&1 &
tail -f logs/manual_*.log
```

### 서비스 재시작
```bash
sudo systemctl restart press-dashboard
sudo journalctl -u press-dashboard -n 50 --no-pager
```

### 코드 업데이트 배포
```bash
cd ~/press-dashboard
git pull
sudo systemctl restart press-dashboard
```

---

## 관리자 페이지 (`/admin`)

- 대시보드 우측 상단 **[관]** 버튼 클릭
- **오늘자 요약표**: 날짜별 기관·유형·링크 테이블 자동 생성
- **이메일 발송**: 요약표 전체 또는 개별 기사를 수신자에게 메일 발송
- **수신자 관리**: 이메일 주소 추가/삭제
- 발신 계정: `testprocess429@gmail.com` (App Password 인증)

---

## 개발 환경 (로컬 테스트)

```bash
# 로컬에서 서버 실행 (DB 파일 필요)
.venv/bin/python local_dashboard.py --db-path press_unified.db --host 127.0.0.1 --port 8080
```
