# Production Deployment Guide

## 1. What changed

- Class-based attachment pipeline: `attachment_pipeline.py`
  - `AttachmentPipelineConfig`
  - `AttachmentRepository`
  - `AttachmentTextExtractor`
  - `AttachmentPipelineApp`
- Class-based report builder: `report_builder.py`
  - `ReportBuildConfig`
  - `ReportRepository`
  - `ReportPayloadBuilder`
  - `ReportBuilderApp`
- Stable service deployment with systemd:
  - `deploy/systemd/press-dashboard.service`
  - `deploy/systemd/press-ingest.service`
  - `deploy/systemd/press-ingest.timer`
  - `deploy/setup_production.sh`

## 2. Server bootstrap

```bash
cd ~/press-dashboard
chmod +x deploy/setup_production.sh
./deploy/setup_production.sh ~/press-dashboard $(whoami) $(id -gn)
```

## 3. Verify services

```bash
sudo systemctl status press-dashboard.service --no-pager
sudo systemctl status press-ingest.timer --no-pager
sudo systemctl list-timers --all | grep press-ingest
```

## 4. Manual runs (if needed)

```bash
# ingestion + attachment pipeline
./run_daily_ingest.sh

# attachment extraction only
./run_attachment_pipeline.sh

# LLM payload build
.venv/bin/python report_builder.py \
  --from-date 2026-03-01 \
  --to-date 2026-03-06 \
  --topic "주간 금융 규제 동향" \
  --output-json logs/report_payload.json
```

## 5. Stability defaults

- Ingest duplicate-run protection with lock file
- Attachment pipeline lock + WAL + busy_timeout
- Service auto-restart (`press-dashboard.service`)
- Timer-based scheduled ingestion (`press-ingest.timer`)
- Source traceability for report outputs (`report_output_sources`)
