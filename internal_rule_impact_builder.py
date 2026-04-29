#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

from document_text_extractor import DocumentTextExtractorService
from unified_retrieval import CorpusDocument, TextChunker, UnifiedRetriever


ADMIN_GUIDANCE_CHANNELS = (
    "fsc_admin_guidance_notice",
    "fss_admin_guidance_notice",
    "fsc_admin_guidance_enforcement",
    "fss_admin_guidance_enforcement",
)

SUPPORTED_RULE_EXTS = {".hwp", ".hwpx", ".pdf", ".docx", ".xlsx", ".xls", ".txt", ".md"}
EXT_PRIORITY = {
    ".hwpx": 1,
    ".hwp": 2,
    ".pdf": 3,
    ".docx": 4,
    ".xlsx": 5,
    ".xls": 6,
    ".md": 7,
    ".txt": 8,
}

STOPWORDS = {
    "행정지도",
    "예고",
    "시행",
    "관련",
    "업무",
    "기준",
    "규정",
    "지침",
    "세칙",
    "개정",
    "사항",
    "금융",
    "금융위원회",
    "금융감독원",
    "그리고",
    "또는",
    "대한",
    "관한",
    "위한",
    "통해",
    "부터",
    "까지",
    "은행",
}


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def normalize_text(text: str) -> str:
    text = text or ""
    text = text.replace("\ufeff", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_rule_name(path: Path) -> str:
    stem = path.stem
    stem = re.sub(r"^DRM_", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"\s*원문[-_]?원본\s*$", "", stem)
    stem = re.sub(r"\s*\(수정본\)\s*$", "", stem)
    stem = re.sub(r"\s+", " ", stem).strip().lower()
    return stem


def stable_doc_id(path: Path) -> str:
    h = hashlib.sha256()
    h.update(str(path.resolve()).encode("utf-8", errors="ignore"))
    try:
        stat = path.stat()
        h.update(str(stat.st_size).encode())
        h.update(str(int(stat.st_mtime)).encode())
    except OSError:
        pass
    return h.hexdigest()[:24]


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[가-힣A-Za-z0-9][가-힣A-Za-z0-9·ㆍ\-/]{1,}", text or "")
    out: List[str] = []
    for token in tokens:
        t = token.strip("._-:/()[]{}<>").lower()
        if len(t) < 2 or t in STOPWORDS:
            continue
        if re.fullmatch(r"\d+", t):
            continue
        out.append(t)
    return out


def top_terms(text: str, limit: int = 80) -> List[str]:
    counts: Dict[str, int] = {}
    for token in tokenize(text):
        counts[token] = counts.get(token, 0) + 1
    return [k for k, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]


@dataclass
class InternalRuleImpactConfig:
    db_path: str
    rule_dir: str
    from_date: str
    to_date: str
    max_guidance: int = 20
    max_rules: int = 30
    max_chars_per_guidance: int = 5000
    max_chars_per_rule: int = 4500
    cache_dir: str = "tmp/internal_rule_cache"
    output_json: str = ""
    only_article_id: Optional[int] = None


class KordocParser:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.fallback = DocumentTextExtractorService()

    def parse(self, path: Path) -> Tuple[str, str, Dict[str, Any]]:
        cache_key = stable_doc_id(path)
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            return data.get("text", ""), data.get("extractor", "cache"), data.get("metadata", {})

        text = ""
        extractor = "kordoc"
        metadata: Dict[str, Any] = {"file": str(path)}
        error: Optional[str] = None

        npx = shutil.which("npx.cmd") or shutil.which("npx")
        if npx:
            try:
                proc = subprocess.run(
                    [npx, "-y", "kordoc", str(path), "--format", "json"],
                    check=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=180,
                )
                stdout = proc.stdout.strip()
                start = stdout.find("{")
                end = stdout.rfind("}")
                if start >= 0 and end >= start:
                    data = json.loads(stdout[start : end + 1])
                    text = normalize_text(data.get("markdown") or data.get("text") or "")
                    metadata["kordoc"] = {
                        "success": data.get("success"),
                        "fileType": data.get("fileType"),
                        "metadata": data.get("metadata") or {},
                    }
                else:
                    error = "kordoc returned no JSON object"
            except Exception as exc:
                error = str(exc)
        else:
            error = "npx not found"

        if not text:
            outcome = self.fallback.extract(path, path.suffix.lower().lstrip("."))
            extractor = outcome.extractor
            text = normalize_text(outcome.text)
            metadata["fallback"] = outcome.metadata
            if error:
                metadata["kordoc_error"] = error
            if outcome.error:
                metadata["fallback_error"] = outcome.error

        cache_path.write_text(
            json.dumps(
                {
                    "source_path": str(path),
                    "extractor": extractor,
                    "text": text,
                    "metadata": metadata,
                    "parsed_at": now_iso(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return text, extractor, metadata


class InternalRuleRepository:
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
        self.conn.commit()

    def fetch_guidance(self, cfg: InternalRuleImpactConfig) -> List[Dict[str, Any]]:
        placeholders = ",".join("?" for _ in ADMIN_GUIDANCE_CHANNELS)
        params: List[Any] = [cfg.from_date, cfg.to_date, *ADMIN_GUIDANCE_CHANNELS]
        article_filter = ""
        if cfg.only_article_id:
            article_filter = " AND a.id = ?"
            params.append(cfg.only_article_id)
        params.append(cfg.max_guidance * 6)
        rows = self.conn.execute(
            f"""
            SELECT
              a.id AS article_id,
              a.title,
              a.published_at,
              a.organization,
              a.source_channel,
              a.content_text,
              at.id AS attachment_id,
              at.file_name,
              at.file_ext,
              at.file_url,
              d.storage_path,
              d.id AS document_id,
              e.id AS extraction_id,
              e.text_content AS extraction_text
            FROM articles a
            LEFT JOIN attachments at ON at.article_id = a.id
            LEFT JOIN attachment_documents d ON d.id = at.document_id
            LEFT JOIN attachment_extractions e ON e.document_id = d.id AND e.status = 'success'
            WHERE date(substr(a.published_at,1,10)) BETWEEN date(?) AND date(?)
              AND a.source_channel IN ({placeholders})
              {article_filter}
            ORDER BY date(substr(a.published_at,1,10)) DESC, a.id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

        picked: Dict[Tuple[int, str], sqlite3.Row] = {}
        for row in rows:
            att_key = row["file_name"] or "__article_body__"
            key = (int(row["article_id"]), normalize_rule_name(Path(att_key)))
            existing = picked.get(key)
            if existing is None:
                picked[key] = row
                continue
            old_rank = EXT_PRIORITY.get(("." + (existing["file_ext"] or "").lower().lstrip(".")), 99)
            new_rank = EXT_PRIORITY.get(("." + (row["file_ext"] or "").lower().lstrip(".")), 99)
            if new_rank < old_rank:
                picked[key] = row

        out: List[Dict[str, Any]] = []
        for idx, row in enumerate(picked.values(), 1):
            article_text = normalize_text(row["content_text"] or "")
            attachment_text = normalize_text(row["extraction_text"] or "")
            merged = article_text
            if attachment_text:
                merged = f"{article_text}\n\n[첨부문서]\n{attachment_text}".strip()
            out.append(
                {
                    "source_index": idx,
                    "article_id": row["article_id"],
                    "attachment_id": row["attachment_id"],
                    "document_id": row["document_id"],
                    "extraction_id": row["extraction_id"],
                    "title": row["title"],
                    "published_at": row["published_at"],
                    "organization": row["organization"],
                    "source_channel": row["source_channel"],
                    "file_name": row["file_name"],
                    "file_url": row["file_url"],
                    "storage_path": row["storage_path"],
                    "text": merged[: cfg.max_chars_per_guidance],
                }
            )

        out.sort(key=lambda x: (x.get("published_at") or "", x.get("article_id") or 0), reverse=True)
        return out[: cfg.max_guidance]

    def create_report(self, cfg: InternalRuleImpactConfig, payload: Dict[str, Any]) -> Dict[str, int]:
        ts = now_iso()
        params = {
            "from_date": cfg.from_date,
            "to_date": cfg.to_date,
            "rule_dir": cfg.rule_dir,
            "max_guidance": cfg.max_guidance,
            "max_rules": cfg.max_rules,
        }
        self.conn.execute(
            """
            INSERT INTO report_jobs (job_type, status, params_json, model_name, requested_at, started_at)
            VALUES ('internal_rule_impact_payload', 'running', ?, 'external-llm', ?, ?)
            """,
            (json.dumps(params, ensure_ascii=False), ts, ts),
        )
        job_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        title = f"행정지도 내규 영향도 분석 입력 패키지 ({cfg.from_date}~{cfg.to_date})"
        summary = (
            f"guidance={len(payload.get('guidance_sources') or [])}, "
            f"internal_rules={len(payload.get('internal_rules') or [])}, "
            f"dedup_groups={payload.get('rule_inventory', {}).get('dedup_group_count', 0)}"
        )
        markdown = (
            "# 행정지도 내규 영향도 분석\n\n"
            f"- 기간: {cfg.from_date} ~ {cfg.to_date}\n"
            "- 생성 방식: LLM 입력 패키지\n\n"
            "## 1. 행정지도 요지\n\n"
            "## 2. 영향 가능 내규\n\n"
            "## 3. 개정/점검 권고\n\n"
            "## 4. 모니터링 포인트\n"
        )
        self.conn.execute(
            """
            INSERT INTO report_outputs (job_id, title, summary_text, report_markdown, report_json, llm_status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (job_id, title, summary, markdown, json.dumps(payload, ensure_ascii=False), ts),
        )
        output_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        for src in payload.get("guidance_sources") or []:
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
        self.conn.execute("UPDATE report_jobs SET status='completed', completed_at=? WHERE id=?", (now_iso(), job_id))
        self.conn.commit()
        return {"job_id": job_id, "output_id": output_id}


class InternalRuleImpactBuilder:
    def __init__(self, cfg: InternalRuleImpactConfig):
        self.cfg = cfg
        self.parser = KordocParser(Path(cfg.cache_dir))

    def iter_rule_groups(self) -> Iterable[Tuple[str, List[Path]]]:
        root = Path(self.cfg.rule_dir)
        files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_RULE_EXTS]
        seen_hashes: set[str] = set()
        groups: Dict[str, List[Path]] = {}
        for path in files:
            try:
                digest = file_sha256(path)
            except OSError:
                continue
            if digest in seen_hashes:
                continue
            seen_hashes.add(digest)
            groups.setdefault(normalize_rule_name(path), []).append(path)
        for name, paths in sorted(groups.items()):
            yield name, sorted(paths, key=lambda p: (EXT_PRIORITY.get(p.suffix.lower(), 99), len(p.name), p.name))

    def parse_rules(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        parsed: List[Dict[str, Any]] = []
        group_count = 0
        candidate_count = 0
        for group_name, paths in self.iter_rule_groups():
            group_count += 1
            candidate_count += len(paths)
            best: Optional[Dict[str, Any]] = None
            errors: List[str] = []
            for path in paths:
                text, extractor, metadata = self.parser.parse(path)
                if text:
                    best = {
                        "rule_id": len(parsed) + 1,
                        "rule_name": path.stem,
                        "normalized_name": group_name,
                        "source_file": str(path),
                        "alternate_files": [str(p) for p in paths if p != path],
                        "extractor": extractor,
                        "char_count": len(text),
                        "terms": top_terms(f"{path.stem}\n{text}", 50),
                        "text": text,
                        "metadata": metadata,
                    }
                    break
                errors.append(f"{path.name}: empty parse")
            if best is None:
                best = {
                    "rule_id": len(parsed) + 1,
                    "rule_name": paths[0].stem,
                    "normalized_name": group_name,
                    "source_file": str(paths[0]),
                    "alternate_files": [str(p) for p in paths[1:]],
                    "extractor": "none",
                    "char_count": 0,
                    "terms": top_terms(paths[0].stem, 20),
                    "text": "",
                    "metadata": {"errors": errors},
                }
            parsed.append(best)
        inventory = {
            "dedup_group_count": group_count,
            "candidate_file_count_after_hash_dedup": candidate_count,
            "parsed_rule_count": len(parsed),
        }
        return parsed, inventory

    def enrich_guidance_attachments(self, guidance: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched: List[Dict[str, Any]] = []
        for item in guidance:
            next_item = dict(item)
            storage_path = item.get("storage_path") or ""
            file_url = item.get("file_url") or ""
            if storage_path:
                path = Path(storage_path)
                if not path.is_absolute():
                    path = Path(self.cfg.db_path).resolve().parent / path
                if path.exists() and path.suffix.lower() in SUPPORTED_RULE_EXTS:
                    parsed_text, extractor, metadata = self.parser.parse(path)
                    parsed_text = normalize_text(parsed_text)
                    if parsed_text and len(parsed_text) > len(item.get("text") or "") * 0.7:
                        base_text = normalize_text(item.get("text") or "")
                        next_item["text"] = (
                            f"{base_text}\n\n[kordoc 첨부 파싱: {path.name}]\n{parsed_text}"
                            if base_text
                            else parsed_text
                        )[: self.cfg.max_chars_per_guidance]
                        next_item["attachment_parser"] = extractor
                        next_item["attachment_parser_metadata"] = metadata
            elif file_url:
                ext = Path(item.get("file_name") or "").suffix.lower()
                if ext in SUPPORTED_RULE_EXTS:
                    download_dir = Path(self.cfg.cache_dir) / "downloads"
                    download_dir.mkdir(parents=True, exist_ok=True)
                    path = download_dir / f"{hashlib.sha256(file_url.encode('utf-8')).hexdigest()[:24]}{ext}"
                    if not path.exists():
                        resp = requests.get(
                            file_url,
                            timeout=45,
                            headers={
                                "User-Agent": "Mozilla/5.0",
                                "Referer": "https://better.fsc.go.kr/",
                            },
                        )
                        resp.raise_for_status()
                        path.write_bytes(resp.content)
                    parsed_text, extractor, metadata = self.parser.parse(path)
                    parsed_text = normalize_text(parsed_text)
                    if parsed_text:
                        base_text = normalize_text(item.get("text") or "")
                        next_item["text"] = (
                            f"{base_text}\n\n[kordoc 첨부 파싱: {item.get('file_name') or path.name}]\n{parsed_text}"
                            if base_text
                            else parsed_text
                        )[: self.cfg.max_chars_per_guidance]
                        next_item["storage_path"] = str(path)
                        next_item["attachment_parser"] = extractor
                        next_item["attachment_parser_metadata"] = metadata
            enriched.append(next_item)
        return enriched

    def score_rules(self, guidance: Sequence[Dict[str, Any]], rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        guidance_blob = "\n\n".join(f"{g.get('title','')}\n{g.get('text','')}" for g in guidance)
        g_terms = set(top_terms(guidance_blob, 140))
        guidance_lower = guidance_blob.lower()
        investment_guidance = any(
            key in guidance_lower
            for key in ("\uacf5\ubaa8\ud380\ub4dc", "\ud380\ub4dc", "\ud22c\uc790", "\uc628\ub77c\uc778", "\ud310\ub9e4\ucc44\ub110", "\uc99d\uad8c")
        )
        consumer_guidance = any(
            key in guidance_lower
            for key in ("\uae08\uc735\uc18c\ube44\uc790", "\uc18c\ube44\uc790", "\uc548\ub0b4", "\uc124\uba85", "\uacf5\uc2dc")
        )

        docs = [
            CorpusDocument(
                doc_id=str(rule.get("rule_id")),
                title=rule.get("rule_name") or "",
                text=rule.get("text") or "",
                metadata={k: v for k, v in rule.items() if k != "text"},
            )
            for rule in rules
            if rule.get("text")
        ]
        retriever = UnifiedRetriever(chunker=TextChunker(chunk_size=1400, overlap=180))
        retriever.index_documents(docs)
        hits = retriever.search(guidance_blob, limit=max(self.cfg.max_rules * 10, 80), candidate_limit=160)

        by_rule: Dict[str, Dict[str, Any]] = {}
        rule_map = {str(rule.get("rule_id")): rule for rule in rules}
        for hit in hits:
            rule_id = str(hit.chunk.doc_id)
            rule = rule_map.get(rule_id)
            if not rule:
                continue
            title = (rule.get("rule_name") or "").lower()
            rule_terms = set(rule.get("terms") or [])
            overlap = sorted(g_terms & rule_terms)
            title_hits = [t for t in g_terms if t and t in title]
            domain_boost = 0
            if any(key in title for key in ("\ub0b4\uaddc\uad00\ub9ac", "\ub0b4\ubd80\ud1b5\uc81c", "\uc900\ubc95", "\uae08\uc735\uc18c\ube44\uc790", "\uc815\ubcf4\ubcf4\ud638", "\uac1c\uc778", "\uc790\uae08\uc138\ud0c1")):
                domain_boost += 4
            if investment_guidance:
                if any(key in title for key in ("\uae08\uc735\ud22c\uc790", "\ud22c\uc790\uc0c1\ud488", "\uc601\uc5c5\ud589\uc704", "\uc0c1\ud488\uacf5\uc2dc", "\ube44\uc608\uae08\uc0c1\ud488", "\uc2e0\ud0c1", "\ud380\ub4dc")):
                    domain_boost += 60
                if any(key in title for key in ("\uae08\uc735\uc18c\ube44\uc790", "\ub0b4\ubd80\ud1b5\uc81c", "\ubd88\uac74\uc804 \uc601\uc5c5")):
                    domain_boost += 32
                if "it\uc5c5\ubb34" in title or "\uac1c\uc778\u00b7\uc2e0\uc6a9\uc815\ubcf4" in title or "\uc815\ubcf4\ubcf4\ud638" in title:
                    domain_boost += 6
                if any(key in title for key in ("\uc0c1\ubc8c", "\uc778\uc0ac", "\ubcf4\uc218", "\ubcf5\ubb34", "\ud1f4\uc9c1", "\ud734\uac00", "\uc548\uc804\ubcf4\uac74", "\ubcf4\uc548\uc5c5\ubb34", "\ud589\ub3d9\uac15\ub839", "\uba74\ucc45", "\uc790\uae08\uc138\ud0c1")):
                    domain_boost -= 36
            if consumer_guidance and any(key in title for key in ("\uae08\uc735\uc18c\ube44\uc790", "\ubbfc\uc6d0", "\uad11\uace0", "\uc0c1\ud488\uacf5\uc2dc", "\ub0b4\ubd80\ud1b5\uc81c")):
                domain_boost += 32

            lexical_score = len(overlap) * 3 + len(title_hits) * 6
            score = (hit.score * 650.0) + lexical_score + domain_boost
            existing = by_rule.get(rule_id)
            if existing and existing["match_score"] >= score:
                continue
            clipped = dict(rule)
            chunk_text = hit.chunk.text.strip()
            full_text = rule.get("text") or ""
            remaining = max(0, self.cfg.max_chars_per_rule - len(chunk_text) - 24)
            clipped["text"] = f"{chunk_text}\n\n[document context]\n{full_text[:remaining]}".strip()
            clipped["match_score"] = round(score, 4)
            clipped["match_terms"] = sorted(set(overlap + title_hits))[:30]
            clipped["retrieval"] = {
                "rank": hit.rank,
                "rrf_score": round(hit.score, 6),
                "component_ranks": hit.component_ranks,
                "component_scores": {k: round(v, 6) for k, v in hit.component_scores.items()},
                "chunk_id": hit.chunk.chunk_id,
            }
            by_rule[rule_id] = clipped

        scored = list(by_rule.values())
        scored.sort(key=lambda r: (-float(r.get("match_score") or 0), r.get("rule_name") or ""))
        return scored[: self.cfg.max_rules]

    def build_payload(self, guidance: List[Dict[str, Any]], rules: List[Dict[str, Any]], inventory: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "version": "1.0",
            "task": "internal_rule_impact_report",
            "language": "ko",
            "topic": "행정지도 예고 및 시행에 따른 내규 영향도 분석",
            "instructions": [
                "행정지도 예고와 행정지도 시행 문서가 내부 규정, 지침, 세칙, 기준에 미치는 영향을 분석한다.",
                "내규관리 담당자가 바로 확인할 수 있도록 개정 필요, 유권해석/준수 점검 필요, 모니터링 필요, 영향 낮음으로 구분한다.",
                "근거는 guidance source_index와 internal rule rule_id를 함께 표시한다.",
                "내규 원문에 직접 근거가 약한 경우 추정이라고 명시한다.",
                "결과에는 우선순위, 담당부서 후보, 점검 항목, 기한 제안을 포함한다.",
            ],
            "output_requirements": [
                "Korean Markdown report",
                "include an impact matrix table",
                "include an action and monitoring checklist table",
                "include duplicate/parse caveat when relevant",
            ],
            "guidance_sources": guidance,
            "internal_rules": rules,
            "rule_inventory": inventory,
        }

    def run(self) -> Dict[str, int]:
        conn = sqlite3.connect(self.cfg.db_path)
        repo = InternalRuleRepository(conn)
        repo.ensure_schema()
        guidance = repo.fetch_guidance(self.cfg)
        guidance = self.enrich_guidance_attachments(guidance)
        rules, inventory = self.parse_rules()
        selected_rules = self.score_rules(guidance, rules)
        payload = self.build_payload(guidance, selected_rules, inventory)
        result = repo.create_report(self.cfg, payload)
        conn.close()
        if self.cfg.output_json:
            Path(self.cfg.output_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        result.update({"guidance_count": len(guidance), "rule_count": len(selected_rules)})
        return result


def default_from_date(days: int) -> str:
    return (dt.date.today() - dt.timedelta(days=days)).isoformat()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build LLM-ready internal rule impact report payload")
    p.add_argument("--db-path", default="press_unified.db")
    p.add_argument("--rule-dir", default="3. 내규목록")
    p.add_argument("--from-date", default="")
    p.add_argument("--to-date", default=dt.date.today().isoformat())
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--max-guidance", type=int, default=20)
    p.add_argument("--max-rules", type=int, default=30)
    p.add_argument("--max-chars-per-guidance", type=int, default=5000)
    p.add_argument("--max-chars-per-rule", type=int, default=4500)
    p.add_argument("--cache-dir", default="tmp/internal_rule_cache")
    p.add_argument("--output-json", default="")
    p.add_argument("--article-id", type=int, default=0)
    return p


def main() -> None:
    args = build_parser().parse_args()
    cfg = InternalRuleImpactConfig(
        db_path=args.db_path,
        rule_dir=args.rule_dir,
        from_date=args.from_date or default_from_date(args.days),
        to_date=args.to_date,
        max_guidance=args.max_guidance,
        max_rules=args.max_rules,
        max_chars_per_guidance=args.max_chars_per_guidance,
        max_chars_per_rule=args.max_chars_per_rule,
        cache_dir=args.cache_dir,
        output_json=args.output_json,
        only_article_id=(args.article_id or None),
    )
    print(json.dumps(InternalRuleImpactBuilder(cfg).run(), ensure_ascii=False))


if __name__ == "__main__":
    main()
