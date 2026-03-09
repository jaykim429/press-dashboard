#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def sanitize_sensitive_text(text: str) -> str:
    masked = text or ""
    masked = re.sub(r"([?&]key=)[^&\s]+", r"\1***", masked, flags=re.IGNORECASE)
    masked = re.sub(r"(X-goog-api-key[:=]\s*)[^\s,]+", r"\1***", masked, flags=re.IGNORECASE)
    masked = re.sub(r"(Authorization[:=]\s*Bearer\s+)[^\s,]+", r"\1***", masked, flags=re.IGNORECASE)
    masked = re.sub(r"AIza[0-9A-Za-z\-_]{20,}", "***", masked)
    return masked


@dataclass
class LlmPipelineConfig:
    db_path: str = "press_unified.db"
    provider: str = "openai"
    model: str = "gpt-4o-mini"
    api_base: str = "https://api.openai.com/v1"
    api_key: str = ""
    max_outputs: int = 5
    temperature: float = 0.2
    dry_run: bool = False
    only_output_id: Optional[int] = None
    prompt_profile: str = "insurance_impact"
    company_name: str = "MetLife Korea"


class ReportOutputRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_outputs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id INTEGER NOT NULL,
              title TEXT,
              summary_text TEXT,
              report_markdown TEXT,
              report_json TEXT,
              llm_status TEXT DEFAULT 'pending',
              llm_provider TEXT,
              llm_model TEXT,
              llm_prompt TEXT,
              llm_response_raw TEXT,
              llm_completed_at TEXT,
              created_at TEXT NOT NULL
            )
            """
        )
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(report_outputs)").fetchall()}
        if "llm_status" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_status TEXT DEFAULT 'pending'")
        if "llm_provider" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_provider TEXT")
        if "llm_model" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_model TEXT")
        if "llm_prompt" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_prompt TEXT")
        if "llm_response_raw" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_response_raw TEXT")
        if "llm_completed_at" not in cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_completed_at TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_outputs_llm_status ON report_outputs(llm_status)")
        self.conn.commit()

    def fetch_pending_outputs(self, limit: int, only_output_id: Optional[int] = None) -> List[sqlite3.Row]:
        if only_output_id:
            return self.conn.execute(
                """
                SELECT id, title, summary_text, report_markdown, report_json
                FROM report_outputs
                WHERE id = ?
                """,
                (only_output_id,),
            ).fetchall()
        return self.conn.execute(
            """
            SELECT id, title, summary_text, report_markdown, report_json
            FROM report_outputs
            WHERE COALESCE(llm_status, 'pending') IN ('pending', 'failed')
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def mark_running(self, output_id: int, provider: str, model: str, prompt: str) -> None:
        self.conn.execute(
            """
            UPDATE report_outputs
            SET llm_status='running', llm_provider=?, llm_model=?, llm_prompt=?
            WHERE id=?
            """,
            (provider, model, prompt, output_id),
        )
        self.conn.commit()

    def mark_success(self, output_id: int, markdown: str, raw_response: str) -> None:
        self.conn.execute(
            """
            UPDATE report_outputs
            SET llm_status='completed',
                report_markdown=?,
                llm_response_raw=?,
                llm_completed_at=?
            WHERE id=?
            """,
            (markdown, raw_response, now_iso(), output_id),
        )
        self.conn.commit()

    def mark_failed(self, output_id: int, error_message: str) -> None:
        safe_error = sanitize_sensitive_text(error_message)
        self.conn.execute(
            """
            UPDATE report_outputs
            SET llm_status='failed',
                llm_response_raw=?,
                llm_completed_at=?
            WHERE id=?
            """,
            (safe_error, now_iso(), output_id),
        )
        self.conn.commit()


class PromptComposer:
    @staticmethod
    def compose(report_json: Dict[str, Any], title: str, profile: str, company_name: str) -> str:
        topic = report_json.get("topic") or title or "regulatory analysis report"
        instructions = report_json.get("instructions") or []
        sources = report_json.get("sources") or []
        instructions_txt = "\n".join([f"- {x}" for x in instructions])
        sources_txt = []
        for s in sources:
            sources_txt.append(
                (
                    f"[source_index={s.get('source_index')}] {s.get('title')} | {s.get('organization')} | "
                    f"{s.get('published_at')} | {s.get('source_channel')}\n{s.get('text','')}\n"
                )
            )
        source_blob = "\n\n".join(sources_txt)
        if profile == "insurance_impact":
            return (
                f"[Context]\n"
                f"You are an AI analyst in strategy and compliance at {company_name}.\n"
                "Analyze unstructured regulatory text from Korean financial authorities and assess practical impact on a life insurer.\n\n"
                "[Task]\n"
                f"Topic: {topic}\n"
                "Use the provided sources only. Write in Korean Markdown.\n\n"
                "Step 1) Document type classification and 3-line summary.\n"
                "Step 2) Relevance score (1-10) for life insurance with rationale.\n"
                "Evaluate relevance for: underwriting/product disclosure, sales channels (GA/bancassurance/agency), "
                "K-ICS/capital and asset regulation, consumer protection and complaint process, digital finance/IT security/data law.\n"
                "Step 3) Impact analysis in three areas:\n"
                "- Finance/Sales: premium revenue, reserve/cost, sanctions.\n"
                "- System/IT: account/information systems, security or data controls.\n"
                "- Organization/Process: policy/procedure updates, staffing/ownership.\n"
                "Step 4) Priority and action plan:\n"
                "- Priority: urgent / normal / reference\n"
                "- Concrete actions with owners and due date suggestions.\n\n"
                "[Output requirements]\n"
                "- Use readable Markdown report format.\n"
                "- Include at least one table for impact and action plan.\n"
                "- Keep industry terms (K-ICS, bancassurance, Financial Consumer Protection Act, etc.) accurate.\n"
                "- Map key claims to source_index citations.\n"
                "- If evidence is weak, mark as assumption.\n\n"
                "[Additional instructions]\n"
                f"{instructions_txt}\n\n"
                "[Sources]\n"
                f"{source_blob}"
            )
        return (
            "You are a financial regulation analyst writing a Korean Markdown report.\n"
            f"Topic: {topic}\n\n"
            "Requirements:\n"
            f"{instructions_txt}\n\n"
            "Output format:\n"
            "1) Executive summary\n"
            "2) Key changes\n"
            "3) Impact analysis (policy/operations/risk)\n"
            "4) Priority action items\n"
            "5) Evidence citations mapped to source_index\n\n"
            "Use only the sources below as evidence and write in Korean Markdown.\n\n"
            f"{source_blob}"
        )


class OpenAIChatClient:
    def __init__(self, api_key: str, model: str, api_base: str, temperature: float):
        self.api_key = api_key
        self.model = model
        self.api_base = api_base.rstrip("/")
        self.temperature = temperature

    def generate(self, prompt: str) -> Tuple[str, str]:
        url = f"{self.api_base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [
                {"role": "system", "content": "You produce high-quality policy analysis reports in Korean Markdown."},
                {"role": "user", "content": prompt},
            ],
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        return content, json.dumps(data, ensure_ascii=False)


class GeminiChatClient:
    def __init__(self, api_key: str, model: str, api_base: str, temperature: float):
        self.api_key = api_key
        self.model = model
        self.api_base = api_base.rstrip("/")
        self.temperature = temperature

    def generate(self, prompt: str) -> Tuple[str, str]:
        url = f"{self.api_base}/models/{self.model}:generateContent"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": self.temperature},
        }
        resp = requests.post(url, headers=headers, params={"key": self.api_key}, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        candidates = data.get("candidates") or []
        if not candidates:
            raise ValueError("Gemini response has no candidates")
        parts = (candidates[0].get("content") or {}).get("parts") or []
        text_chunks = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
        content = "\n".join(text_chunks).strip()
        if not content:
            raise ValueError("Gemini response contains no text")
        return content, json.dumps(data, ensure_ascii=False)


class LlmReportPipelineApp:
    def __init__(self, cfg: LlmPipelineConfig):
        self.cfg = cfg

    def _build_client(self):
        provider = self.cfg.provider.lower().strip()
        if provider == "openai":
            if not self.cfg.api_key and not self.cfg.dry_run:
                raise ValueError("OPENAI_API_KEY is required unless --dry-run is used")
            return OpenAIChatClient(
                api_key=self.cfg.api_key,
                model=self.cfg.model,
                api_base=self.cfg.api_base,
                temperature=self.cfg.temperature,
            )
        if provider == "google":
            if not self.cfg.api_key and not self.cfg.dry_run:
                raise ValueError("GOOGLE_API_KEY is required unless --dry-run is used")
            return GeminiChatClient(
                api_key=self.cfg.api_key,
                model=self.cfg.model,
                api_base=self.cfg.api_base,
                temperature=self.cfg.temperature,
            )
        raise ValueError(f"Unsupported provider: {self.cfg.provider}")

    def run(self) -> Dict[str, Any]:
        conn = sqlite3.connect(self.cfg.db_path)
        repo = ReportOutputRepository(conn)
        repo.ensure_schema()
        rows = repo.fetch_pending_outputs(limit=self.cfg.max_outputs, only_output_id=self.cfg.only_output_id)
        if not rows:
            conn.close()
            return {"processed": 0, "message": "no pending report_outputs"}

        client = None if self.cfg.dry_run else self._build_client()
        processed = 0
        for row in rows:
            output_id = int(row["id"])
            title = row["title"] or ""
            report_json_raw = row["report_json"] or "{}"
            try:
                report_json = json.loads(report_json_raw)
            except Exception:
                report_json = {}
            prompt = PromptComposer.compose(
                report_json=report_json,
                title=title,
                profile=self.cfg.prompt_profile,
                company_name=self.cfg.company_name,
            )
            repo.mark_running(output_id, self.cfg.provider, self.cfg.model, prompt)
            try:
                if self.cfg.dry_run:
                    markdown = (
                        f"# {title or 'LLM Dry Run Report'}\n\n"
                        "- status: dry-run\n"
                        "- no external LLM call executed\n"
                    )
                    raw = '{"dry_run": true}'
                else:
                    markdown, raw = client.generate(prompt)  # type: ignore
                repo.mark_success(output_id, markdown, raw)
                processed += 1
            except Exception as exc:
                repo.mark_failed(output_id, str(exc))
        conn.close()
        return {"processed": processed, "total_candidates": len(rows)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LLM report generation pipeline")
    p.add_argument("--db-path", default="press_unified.db")
    p.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "openai"))
    p.add_argument("--model", default=os.getenv("LLM_MODEL", "gpt-4o-mini"))
    p.add_argument("--api-base", default=os.getenv("LLM_API_BASE", ""))
    p.add_argument("--api-key", default=os.getenv("LLM_API_KEY", ""))
    p.add_argument("--max-outputs", type=int, default=5)
    p.add_argument("--temperature", type=float, default=0.2)
    p.add_argument("--prompt-profile", default=os.getenv("LLM_PROMPT_PROFILE", "insurance_impact"))
    p.add_argument("--company-name", default=os.getenv("LLM_COMPANY_NAME", "MetLife Korea"))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--only-output-id", type=int, default=0)
    return p


def main() -> None:
    args = build_parser().parse_args()
    provider = (args.provider or "openai").strip().lower()
    model = args.model
    api_base = args.api_base
    api_key = args.api_key

    if provider == "openai":
        if not api_base:
            api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        if not api_key:
            api_key = os.getenv("OPENAI_API_KEY", "")
    elif provider == "google":
        if not api_base:
            api_base = os.getenv("GOOGLE_API_BASE", "https://generativelanguage.googleapis.com/v1beta")
        if model == "gpt-4o-mini":
            model = os.getenv("GOOGLE_MODEL", "gemini-2.0-flash")
        if not api_key:
            api_key = os.getenv("GOOGLE_API_KEY", "")

    cfg = LlmPipelineConfig(
        db_path=args.db_path,
        provider=provider,
        model=model,
        api_base=api_base,
        api_key=api_key,
        max_outputs=args.max_outputs,
        temperature=args.temperature,
        prompt_profile=args.prompt_profile,
        company_name=args.company_name,
        dry_run=args.dry_run,
        only_output_id=(args.only_output_id or None),
    )
    result = LlmReportPipelineApp(cfg).run()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()


