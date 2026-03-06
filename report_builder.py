#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


@dataclass
class ReportBuildConfig:
    db_path: str
    from_date: str
    to_date: str
    topic: str = "금융 규제 동향"
    model_name: str = "external-llm"
    language: str = "ko"
    max_sources: int = 80
    max_chars_per_source: int = 3000
    output_json: str = ""


class ReportRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_jobs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_type TEXT NOT NULL,
              status TEXT NOT NULL,
              params_json TEXT,
              model_name TEXT,
              requested_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              error_message TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_outputs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id INTEGER NOT NULL,
              title TEXT,
              summary_text TEXT,
              report_markdown TEXT,
              report_json TEXT,
              created_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_output_sources (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              report_output_id INTEGER NOT NULL,
              article_id INTEGER,
              attachment_id INTEGER,
              document_id INTEGER,
              extraction_id INTEGER,
              relevance_score REAL,
              citation_text TEXT,
              created_at TEXT NOT NULL,
              UNIQUE(report_output_id, article_id, attachment_id, document_id, extraction_id)
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_jobs_status ON report_jobs(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_outputs_job ON report_outputs(job_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_sources_output ON report_output_sources(report_output_id)")
        self.conn.commit()

    def create_job(self, cfg: ReportBuildConfig) -> int:
        ts = now_iso()
        params = {
            "from_date": cfg.from_date,
            "to_date": cfg.to_date,
            "topic": cfg.topic,
            "max_sources": cfg.max_sources,
            "max_chars_per_source": cfg.max_chars_per_source,
            "language": cfg.language,
        }
        self.conn.execute(
            """
            INSERT INTO report_jobs (job_type, status, params_json, model_name, requested_at, started_at)
            VALUES ('draft_payload', 'running', ?, ?, ?, ?)
            """,
            (json.dumps(params, ensure_ascii=False), cfg.model_name, ts, ts),
        )
        job_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.conn.commit()
        return int(job_id)

    def complete_job(self, job_id: int) -> None:
        self.conn.execute(
            "UPDATE report_jobs SET status='completed', completed_at=? WHERE id=?",
            (now_iso(), job_id),
        )
        self.conn.commit()

    def fail_job(self, job_id: int, error_message: str) -> None:
        self.conn.execute(
            "UPDATE report_jobs SET status='failed', completed_at=?, error_message=? WHERE id=?",
            (now_iso(), error_message, job_id),
        )
        self.conn.commit()

    def query_sources(self, cfg: ReportBuildConfig) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
              a.id AS article_id,
              a.title,
              a.published_at,
              a.organization,
              a.source_channel,
              a.content_text,
              at.id AS attachment_id,
              d.id AS document_id,
              e.id AS extraction_id,
              e.text_content AS extraction_text
            FROM articles a
            LEFT JOIN attachments at ON at.article_id = a.id
            LEFT JOIN attachment_documents d ON d.id = at.document_id
            LEFT JOIN attachment_extractions e ON e.document_id = d.id AND e.status = 'success'
            WHERE date(substr(a.published_at,1,10)) BETWEEN date(?) AND date(?)
            ORDER BY date(substr(a.published_at,1,10)) DESC, a.id DESC
            LIMIT ?
            """,
            (cfg.from_date, cfg.to_date, cfg.max_sources),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            article_text = (r["content_text"] or "")[: cfg.max_chars_per_source]
            attachment_text = (r["extraction_text"] or "")[: cfg.max_chars_per_source]
            merged = article_text
            if attachment_text:
                merged = f"{article_text}\n\n[ATTACHMENT]\n{attachment_text}".strip()
            out.append(
                {
                    "article_id": r["article_id"],
                    "attachment_id": r["attachment_id"],
                    "document_id": r["document_id"],
                    "extraction_id": r["extraction_id"],
                    "title": r["title"],
                    "published_at": r["published_at"],
                    "organization": r["organization"],
                    "source_channel": r["source_channel"],
                    "text": merged,
                }
            )
        return out

    def create_output(self, job_id: int, title: str, summary: str, markdown: str, report_json: Dict[str, Any]) -> int:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO report_outputs (job_id, title, summary_text, report_markdown, report_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (job_id, title, summary, markdown, json.dumps(report_json, ensure_ascii=False), ts),
        )
        output_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.conn.commit()
        return int(output_id)

    def add_output_sources(self, output_id: int, sources: List[Dict[str, Any]]) -> None:
        ts = now_iso()
        for src in sources:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO report_output_sources (
                  report_output_id, article_id, attachment_id, document_id, extraction_id, relevance_score, citation_text, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    output_id,
                    src.get("article_id"),
                    src.get("attachment_id"),
                    src.get("document_id"),
                    src.get("extraction_id"),
                    None,
                    src.get("title"),
                    ts,
                ),
            )
        self.conn.commit()


class ReportPayloadBuilder:
    def build_payload(self, cfg: ReportBuildConfig, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "version": "1.0",
            "task": "regulatory_report_generation",
            "language": cfg.language,
            "topic": cfg.topic,
            "instructions": [
                "요약 보고서를 작성하되, 주장마다 출처(source_index)를 명시한다.",
                "핵심 변화, 영향, 리스크, 후속 액션을 분리해 작성한다.",
                "추정/불확실 정보는 명시적으로 표기한다.",
            ],
            "sources": [
                {
                    "source_index": idx + 1,
                    "article_id": s["article_id"],
                    "attachment_id": s["attachment_id"],
                    "document_id": s["document_id"],
                    "extraction_id": s["extraction_id"],
                    "title": s["title"],
                    "published_at": s["published_at"],
                    "organization": s["organization"],
                    "source_channel": s["source_channel"],
                    "text": s["text"],
                }
                for idx, s in enumerate(sources)
            ],
        }

    def build_markdown_template(self, cfg: ReportBuildConfig) -> str:
        return (
            f"# {cfg.topic} 보고서 초안\n\n"
            f"- 기간: {cfg.from_date} ~ {cfg.to_date}\n"
            "- 생성 방식: LLM 입력용 초안 패키지\n\n"
            "## 1. 핵심 요약\n\n"
            "## 2. 주요 제도/정책 변화\n\n"
            "## 3. 영향 분석\n\n"
            "## 4. 리스크 및 모니터링 포인트\n\n"
            "## 5. 실행 권고안\n"
        )


class ReportBuilderApp:
    def __init__(self, cfg: ReportBuildConfig):
        self.cfg = cfg

    def run(self) -> Dict[str, int]:
        conn = sqlite3.connect(self.cfg.db_path)
        repo = ReportRepository(conn)
        repo.ensure_schema()
        payload_builder = ReportPayloadBuilder()
        job_id = repo.create_job(self.cfg)

        try:
            sources = repo.query_sources(self.cfg)
            payload = payload_builder.build_payload(self.cfg, sources)
            markdown = payload_builder.build_markdown_template(self.cfg)
            summary = f"sources={len(sources)} / period={self.cfg.from_date}..{self.cfg.to_date}"
            output_id = repo.create_output(
                job_id=job_id,
                title=f"{self.cfg.topic} 보고서 입력 패키지",
                summary=summary,
                markdown=markdown,
                report_json=payload,
            )
            repo.add_output_sources(output_id, sources)
            repo.complete_job(job_id)

            if self.cfg.output_json:
                Path(self.cfg.output_json).write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            conn.close()
            return {"job_id": job_id, "output_id": output_id, "source_count": len(sources)}
        except Exception as exc:
            repo.fail_job(job_id, str(exc))
            conn.close()
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build LLM-ready report payload from DB")
    parser.add_argument("--db-path", default="press_unified.db")
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--topic", default="금융 규제 동향")
    parser.add_argument("--model-name", default="external-llm")
    parser.add_argument("--language", default="ko")
    parser.add_argument("--max-sources", type=int, default=80)
    parser.add_argument("--max-chars-per-source", type=int, default=3000)
    parser.add_argument("--output-json", default="")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = ReportBuildConfig(
        db_path=args.db_path,
        from_date=args.from_date,
        to_date=args.to_date,
        topic=args.topic,
        model_name=args.model_name,
        language=args.language,
        max_sources=args.max_sources,
        max_chars_per_source=args.max_chars_per_source,
        output_json=args.output_json,
    )
    result = ReportBuilderApp(cfg).run()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
