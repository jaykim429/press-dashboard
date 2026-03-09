# LLM Pipeline Quickstart

This guide assumes the repository root is `~/press-dashboard` (Linux server).

## 1) One-time setup

```bash
cd ~/press-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Required keys

- `service_key.txt`: Data.go.kr API key (already used by ingest)
- One of:
  - `OPENAI_API_KEY` for OpenAI
  - `GOOGLE_API_KEY` for Gemini

```bash
cd ~/press-dashboard
export LLM_PROVIDER=openai
export OPENAI_API_KEY="<YOUR_OPENAI_API_KEY>"
```

Gemini example:

```bash
cd ~/press-dashboard
export LLM_PROVIDER=google
export GOOGLE_API_KEY="<YOUR_GOOGLE_API_KEY>"
export LLM_MODEL=gemini-2.0-flash
```

## 3) Run full pipeline (ingest -> attachment extraction -> report payload -> LLM markdown)

```bash
cd ~/press-dashboard
export LLM_PROVIDER=google
export GOOGLE_API_KEY="<YOUR_GOOGLE_API_KEY>"
export LLM_MODEL=gemini-1.5-flash
./run_full_pipeline.sh
```

## 4) Optional knobs

```bash
# Only core channels (recommended): default behavior
export ATTACH_CORE_ONLY=1

# LLM options
export LLM_PROVIDER=openai
export LLM_MODEL=gpt-4o-mini
export OPENAI_API_BASE=https://api.openai.com/v1
export GOOGLE_API_BASE=https://generativelanguage.googleapis.com/v1beta
export LLM_MAX_OUTPUTS=5
export LLM_TEMPERATURE=0.2
export LLM_PROMPT_PROFILE=insurance_impact
export LLM_COMPANY_NAME="MetLife Korea"

# Report builder options
export REPORT_TOPIC="핵심 규정/행정지도 영향도 분석"
export REPORT_MAX_SOURCES=80
export REPORT_MAX_CHARS=3000
# export REPORT_ALL_CHANNELS=1  # uncomment to include non-core channels
```

## 5) Verify DB writes

```bash
cd ~/press-dashboard
sqlite3 press_unified.db "
SELECT id, llm_status, llm_provider, llm_model, length(report_markdown) AS md_len, llm_completed_at
FROM report_outputs
ORDER BY id DESC
LIMIT 5;"
```

Expected: newest rows should show `llm_status='completed'` and `md_len > 0`.

## 6) Run only LLM step again

```bash
cd ~/press-dashboard
export LLM_PROVIDER=google
export GOOGLE_API_KEY="<YOUR_GOOGLE_API_KEY>"
export LLM_MODEL=gemini-2.0-flash
./run_llm_report_pipeline.sh
```

## 7) Dry-run mode (no API cost)

```bash
cd ~/press-dashboard
unset GOOGLE_API_KEY
unset OPENAI_API_KEY
./run_llm_report_pipeline.sh
```

This writes placeholder markdown and keeps DB schema/migration path validated.
