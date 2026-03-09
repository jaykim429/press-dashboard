#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests

try:
    import fcntl  # type: ignore
except Exception:
    fcntl = None

from document_text_extractor import DocumentTextExtractorService


EXTRACTOR_NAME = "attachment_text_extractor"
EXTRACTOR_VERSION = "1.1.0"
USER_AGENT = "press-dashboard-attachment-worker/1.1"
CORE_ANALYSIS_CHANNELS = (
    "fsc_rule_change_notice",
    "ksd_rule_change_notice",
    "krx_rule_change_notice",
    "kofia_rule_change_notice",
    "fsc_regulation_notice",
    "krx_recent_rule_change",
    "kofia_recent_rule_change",
    "fsc_admin_guidance_notice",
    "fss_admin_guidance_notice",
    "fsc_admin_guidance_enforcement",
    "fss_admin_guidance_enforcement",
    "fsc_law_interpretation",
    "fsc_no_action_opinion",
)


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def token_estimate(text: str) -> int:
    return max(1, len(text) // 3)


@dataclass
class AttachmentPipelineConfig:
    db_path: str = "press_unified.db"
    download_dir: str = "attachment_store"
    lock_file: str = "logs/attachment_pipeline.lock"
    batch_size: int = 50
    max_retry: int = 3
    timeout_sec: int = 30
    max_bytes: int = 30 * 1024 * 1024
    core_only: bool = False


class FileLock:
    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.fp = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.fp = self.lock_path.open("w", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


class AttachmentRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def ensure_schema(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attachment_documents (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              canonical_url TEXT NOT NULL UNIQUE,
              url_hash TEXT UNIQUE,
              storage_path TEXT,
              download_status TEXT NOT NULL DEFAULT 'pending',
              http_status INTEGER,
              content_type TEXT,
              file_size INTEGER,
              sha256 TEXT,
              etag TEXT,
              last_modified TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              last_error TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attachment_extractions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              document_id INTEGER NOT NULL,
              extractor_name TEXT NOT NULL,
              extractor_version TEXT NOT NULL,
              status TEXT NOT NULL,
              text_content TEXT,
              char_count INTEGER,
              token_estimate INTEGER,
              metadata_json TEXT,
              error_message TEXT,
              created_at TEXT NOT NULL,
              UNIQUE(document_id, extractor_name, extractor_version)
            )
            """
        )
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(attachments)").fetchall()}
        if "url_hash" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN url_hash TEXT")
        if "document_id" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN document_id INTEGER")
        if "processing_status" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN processing_status TEXT DEFAULT 'pending'")
        if "last_error" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN last_error TEXT")
        if "last_processed_at" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN last_processed_at TEXT")

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_url_hash ON attachments(url_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_document ON attachments(document_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_status ON attachments(processing_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_docs_hash ON attachment_documents(url_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_docs_status ON attachment_documents(download_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_extract_doc ON attachment_extractions(document_id)")
        self.conn.commit()

    def materialize_documents(self) -> int:
        rows = self.conn.execute(
            "SELECT DISTINCT file_url FROM attachments WHERE file_url IS NOT NULL AND file_url <> ''"
        ).fetchall()
        now = now_iso()
        created = 0
        for row in rows:
            url = row["file_url"]
            url_hash = sha256_text(url)
            existing = self.conn.execute(
                "SELECT id FROM attachment_documents WHERE url_hash=? OR canonical_url=?",
                (url_hash, url),
            ).fetchone()
            if existing:
                doc_id = existing["id"]
                self.conn.execute("UPDATE attachment_documents SET last_seen_at=? WHERE id=?", (now, doc_id))
            else:
                self.conn.execute(
                    """
                    INSERT INTO attachment_documents (
                      canonical_url, url_hash, download_status, first_seen_at, last_seen_at
                    ) VALUES (?, ?, 'pending', ?, ?)
                    """,
                    (url, url_hash, now, now),
                )
                doc_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                created += 1
            self.conn.execute(
                """
                UPDATE attachments
                SET url_hash=?, document_id=COALESCE(document_id, ?)
                WHERE file_url=?
                """,
                (url_hash, doc_id, url),
            )
        self.conn.commit()
        return created

    def fetch_candidates(self, batch_size: int, max_retry: int, core_only: bool = False) -> List[sqlite3.Row]:
        channel_filter = ""
        if core_only:
            placeholders = ",".join(["?"] * len(CORE_ANALYSIS_CHANNELS))
            channel_filter = f" AND ar.source_channel IN ({placeholders})"

        params: List[object] = [max_retry]
        if core_only:
            params.extend(CORE_ANALYSIS_CHANNELS)
        params.append(batch_size)
        return self.conn.execute(
            """
            SELECT
              d.id,
              d.canonical_url,
              d.url_hash,
              MIN(
                CASE
                  WHEN ar.source_channel IN (
                    'fsc_rule_change_notice','ksd_rule_change_notice','krx_rule_change_notice','kofia_rule_change_notice',
                    'fsc_regulation_notice','krx_recent_rule_change','kofia_recent_rule_change',
                    'fsc_admin_guidance_notice','fss_admin_guidance_notice',
                    'fsc_admin_guidance_enforcement','fss_admin_guidance_enforcement',
                    'fsc_law_interpretation','fsc_no_action_opinion'
                  ) THEN 0 ELSE 1
                END
              ) AS priority_rank
            FROM attachment_documents d
            JOIN attachments a ON a.document_id = d.id
            JOIN articles ar ON ar.id = a.article_id
            WHERE (d.download_status='pending' OR d.download_status='failed')
              AND d.retry_count < ?
              AND (a.processing_status='pending' OR a.processing_status='failed' OR a.processing_status IS NULL)
              """ + channel_filter + """
            GROUP BY d.id, d.canonical_url, d.url_hash
            ORDER BY priority_rank ASC, d.last_seen_at DESC, d.id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

    def mark_document_processing(self, doc_id: int) -> None:
        self.conn.execute(
            "UPDATE attachment_documents SET download_status='processing', last_error=NULL WHERE id=?",
            (doc_id,),
        )
        self.conn.commit()

    def mark_success(
        self,
        doc_id: int,
        storage_path: str,
        http_status: int,
        content_type: Optional[str],
        file_size: int,
        file_sha: str,
        etag: Optional[str],
        last_modified: Optional[str],
        text: str,
        metadata: Dict[str, object],
    ) -> None:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO attachment_extractions (
              document_id, extractor_name, extractor_version, status, text_content, char_count, token_estimate,
              metadata_json, error_message, created_at
            ) VALUES (?, ?, ?, 'success', ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(document_id, extractor_name, extractor_version) DO UPDATE SET
              status='success',
              text_content=excluded.text_content,
              char_count=excluded.char_count,
              token_estimate=excluded.token_estimate,
              metadata_json=excluded.metadata_json,
              error_message=NULL,
              created_at=excluded.created_at
            """,
            (
                doc_id,
                EXTRACTOR_NAME,
                EXTRACTOR_VERSION,
                text,
                len(text),
                token_estimate(text),
                json.dumps(metadata, ensure_ascii=False),
                ts,
            ),
        )
        self.conn.execute(
            """
            UPDATE attachment_documents
            SET storage_path=?, download_status='downloaded', http_status=?, content_type=?, file_size=?, sha256=?,
                etag=?, last_modified=?, last_error=NULL, last_seen_at=?
            WHERE id=?
            """,
            (storage_path, http_status, content_type, file_size, file_sha, etag, last_modified, ts, doc_id),
        )
        self.conn.execute(
            """
            UPDATE attachments
            SET processing_status='success', last_error=NULL, last_processed_at=?
            WHERE document_id=?
            """,
            (ts, doc_id),
        )
        self.conn.commit()

    def mark_failure(self, doc_id: int, error_message: str) -> None:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO attachment_extractions (
              document_id, extractor_name, extractor_version, status, text_content, char_count, token_estimate,
              metadata_json, error_message, created_at
            ) VALUES (?, ?, ?, 'failed', NULL, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(document_id, extractor_name, extractor_version) DO UPDATE SET
              status='failed',
              text_content=NULL,
              char_count=NULL,
              token_estimate=NULL,
              metadata_json=NULL,
              error_message=excluded.error_message,
              created_at=excluded.created_at
            """,
            (doc_id, EXTRACTOR_NAME, EXTRACTOR_VERSION, error_message, ts),
        )
        self.conn.execute(
            """
            UPDATE attachment_documents
            SET download_status='failed', retry_count=retry_count+1, last_error=?, last_seen_at=?
            WHERE id=?
            """,
            (error_message, ts, doc_id),
        )
        self.conn.execute(
            """
            UPDATE attachments
            SET processing_status='failed', last_error=?, last_processed_at=?
            WHERE document_id=?
            """,
            (error_message, ts, doc_id),
        )
        self.conn.commit()


class AttachmentPipelineApp:
    def __init__(self, cfg: AttachmentPipelineConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.extractor_service = DocumentTextExtractorService()

    @staticmethod
    def _looks_like_html(content_type: Optional[str], file_bytes: bytes) -> bool:
        ct = (content_type or "").lower()
        if "text/html" in ct or "application/xhtml+xml" in ct:
            return True
        head = file_bytes[:1024].decode("utf-8", errors="ignore").lower()
        return "<html" in head or "<!doctype html" in head

    def run(self) -> None:
        lock = FileLock(Path(self.cfg.lock_file))
        lock.acquire()

        conn = sqlite3.connect(self.cfg.db_path)
        repo = AttachmentRepository(conn)
        repo.ensure_schema()

        created = repo.materialize_documents()
        print(f"[INFO] materialized attachment_documents: {created}")

        candidates = repo.fetch_candidates(self.cfg.batch_size, self.cfg.max_retry, core_only=self.cfg.core_only)
        print(f"[INFO] candidates: {len(candidates)}")

        for doc in candidates:
            doc_id = int(doc["id"])
            url = doc["canonical_url"]
            try:
                repo.mark_document_processing(doc_id)
                resp = self.session.get(url, timeout=self.cfg.timeout_sec, stream=True)
                status = int(resp.status_code)
                resp.raise_for_status()
                content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip() or None

                payload = bytearray()
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if not chunk:
                        continue
                    payload.extend(chunk)
                    if len(payload) > self.cfg.max_bytes:
                        raise RuntimeError(f"Attachment exceeds max_bytes={self.cfg.max_bytes}")

                file_bytes = bytes(payload)
                file_sha = sha256_bytes(file_bytes)
                linked = conn.execute(
                    "SELECT file_name FROM attachments WHERE document_id=? ORDER BY id ASC LIMIT 1",
                    (doc_id,),
                ).fetchone()
                file_name = linked[0] if linked else None
                ext = self.extractor_service.infer_extension(file_name, url, content_type)

                subdir = Path(self.cfg.download_dir) / dt.datetime.now().strftime("%Y%m%d")
                subdir.mkdir(parents=True, exist_ok=True)
                path = subdir / f"{doc['url_hash']}.{ext}"
                path.write_bytes(file_bytes)

                outcome = self.extractor_service.extract(path, ext)
                # If declared extension parsing fails but payload is actually HTML,
                # retry with HTML extractor so we can still preserve useful text.
                if not outcome.ok and self._looks_like_html(content_type, file_bytes):
                    html_path = subdir / f"{doc['url_hash']}.html"
                    html_path.write_bytes(file_bytes)
                    html_outcome = self.extractor_service.extract(html_path, "html")
                    if html_outcome.ok:
                        outcome = html_outcome
                        path = html_path

                if not outcome.ok:
                    raise RuntimeError(outcome.error or f"Extraction failed for ext={ext}")

                text, ext_meta = outcome.text, {"extractor": outcome.extractor, **(outcome.metadata or {})}
                repo.mark_success(
                    doc_id=doc_id,
                    storage_path=str(path),
                    http_status=status,
                    content_type=content_type,
                    file_size=len(file_bytes),
                    file_sha=file_sha,
                    etag=resp.headers.get("ETag"),
                    last_modified=resp.headers.get("Last-Modified"),
                    text=text,
                    metadata={"url": url, "content_type": content_type, "ext_meta": ext_meta, "sha256": file_sha},
                )
            except Exception as exc:
                repo.mark_failure(doc_id, str(exc))
        conn.close()
        print("[INFO] attachment pipeline completed")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Attachment extraction pipeline")
    p.add_argument("--db-path", default="press_unified.db")
    p.add_argument("--download-dir", default="attachment_store")
    p.add_argument("--lock-file", default="logs/attachment_pipeline.lock")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--max-retry", type=int, default=3)
    p.add_argument("--timeout-sec", type=int, default=30)
    p.add_argument("--max-bytes", type=int, default=30 * 1024 * 1024)
    p.add_argument(
        "--core-only",
        action="store_true",
        help="Process only core analysis channels (rule/admin/law/no-action types)",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    cfg = AttachmentPipelineConfig(
        db_path=args.db_path,
        download_dir=args.download_dir,
        lock_file=args.lock_file,
        batch_size=args.batch_size,
        max_retry=args.max_retry,
        timeout_sec=args.timeout_sec,
        max_bytes=args.max_bytes,
        core_only=args.core_only,
    )
    AttachmentPipelineApp(cfg).run()


if __name__ == "__main__":
    main()
