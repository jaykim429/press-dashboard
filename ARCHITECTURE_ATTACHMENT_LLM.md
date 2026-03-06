# Attachment + LLM Report Architecture

This document describes the production-oriented data model and workflow added for:
- reliable attachment lifecycle management
- text extraction from attachments
- LLM-ready report payload generation

## 1) Goals

- Keep ingestion idempotent and restart-safe.
- Avoid duplicate downloads for same file URL.
- Track processing state and errors explicitly.
- Preserve traceability from final report back to sources.

## 2) DB Model

Existing:
- `articles`: article-level metadata and normalized content.
- `attachments`: article-to-file link table.

Added:
- `attachment_documents`
  - deduplicated physical document record by URL/hash
  - stores download status, file metadata, checksum, retry count, errors
- `attachment_extractions`
  - extracted text and extractor metadata/version
  - success/failure state and error message
- `report_jobs`
  - report generation job lifecycle (running/completed/failed)
- `report_outputs`
  - generated report payload/markdown
- `report_output_sources`
  - source mapping for traceability and citations

Key `attachments` extensions:
- `url_hash`, `document_id`, `processing_status`, `last_error`, `last_processed_at`

## 3) Pipeline Stages

1. Ingest stage (`unified_press_ingest.py`)
   - writes `articles` + `attachments`
   - marks attachments as `processing_status='pending'`

2. Attachment stage (`attachment_pipeline.py`)
   - materializes deduplicated `attachment_documents`
   - downloads files to `attachment_store/YYYYMMDD/`
   - extracts text (txt/html/pdf currently)
   - writes `attachment_extractions`
   - updates attachment/doc statuses

3. Report payload stage (`report_builder.py`)
   - selects sources by date window
   - merges article text + extracted attachment text
   - writes `report_jobs`, `report_outputs`, `report_output_sources`
   - optional JSON export for external LLM call

## 4) Operational Commands

```bash
# ingestion
./run_daily_ingest.sh

# attachment processing
./run_attachment_pipeline.sh

# build LLM-ready payload
.venv/bin/python report_builder.py \
  --from-date 2026-03-01 \
  --to-date 2026-03-06 \
  --topic "주간 금융 규제 동향" \
  --output-json logs/report_payload.json
```

## 5) Stability and Scaling Notes

- Lock files prevent duplicate runners.
- SQLite WAL + busy timeout are used in attachment worker.
- URL hash dedup avoids re-downloading the same attachment across articles.
- Extractor versioning supports reprocessing and parser upgrades.
- Report/source mapping enables auditability for generated outputs.

## 6) Next Recommended Step

Add queue-based workers (Redis/Celery or Pub/Sub) when moving beyond single-host SQLite execution.
The current schema is already compatible with that migration path.
