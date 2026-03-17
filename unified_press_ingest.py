import argparse
import datetime as dt
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, urlencode, unquote, urljoin, urlparse
import xml.etree.ElementTree as ET
import abc
import math
from collections import Counter
from pathlib import Path
try:
    import yaml  # optional – only needed when using ingest_config.yaml
except ImportError:
    yaml = None


class BaseCollector(abc.ABC):
    """Abstract base class for all press-release collectors.

    To add a new data source:
      1. Create a class that inherits from BaseCollector.
      2. Implement the `collector_key` property and `collect()` method.
      3. Register it in COLLECTOR_REGISTRY at the bottom of this file.
    """

    @property
    @abc.abstractmethod
    def collector_key(self) -> str:
        """Unique snake_case identifier used as the key in ingest_config.yaml."""

    @abc.abstractmethod
    def collect(self, cfg: dict) -> list:
        """Run the collector and return a list of article dicts.

        Args:
            cfg: The per-collector config dict from ingest_config.yaml
                 (may include ``max_pages``, ``api_orgs``, etc.)
        Returns:
            List[Dict[str, Any]] – articles with standard schema fields.
        """


def load_ingest_config(path: str) -> dict:
    """Load ingest_config.yaml. Returns empty dict if yaml is unavailable."""
    if yaml is None:
        print("[WARN] PyYAML not installed – skipping config file. pip install pyyaml")
        return {}
    p = Path(path)
    if not p.exists():
        print(f"[WARN] Config file not found: {p}")
        return {}
    with p.open(encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scrape_ksd_press import scrape_ksd_press_releases

API_BASE = "https://apis.data.go.kr/1371000/pressReleaseService/pressReleaseList"
FSS_BASE = "https://fss.or.kr"
KSD_BASE = "https://ksd.or.kr"
BOK_BASE = "https://www.bok.or.kr"
KFB_BASE = "https://www.kfb.or.kr"
FSEC_BASE = "https://www.fsec.or.kr"
KRX_BASE = "https://rule.krx.co.kr"
KOFIA_BASE = "https://law.kofia.or.kr"
FSC_ADMIN_BASE = "https://better.fsc.go.kr"

TIMEOUT = (10, 30)
MAX_RETRIES = 3

FSS_BOARDS = {
    "fss_press_release": {"bbs_id": "B0000188", "menu_no": "200218", "name": "보도자료"},
    "fss_press_explainer": {"bbs_id": "B0000189", "menu_no": "200219", "name": "보도설명자료"},
}


DEFAULT_API_ORGS: List[str] = [
    "\uae08\uc735\uc704\uc6d0\ud68c",
    "\uae08\uc735\uac10\ub3c5\uc6d0",
    "\ud55c\uad6d\uc740\ud589",
    "\uac1c\uc778\uc815\ubcf4\ubcf4\ud638\uc704\uc6d0\ud68c",
    "\uae30\ud68d\uc7ac\uc815\ubd80",
    "\uc7ac\uc815\uacbd\uc81c\ubd80",
]


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def parse_api_datetime(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = re.sub(r"\s+", " ", raw.strip())
    text = text.rstrip(".")

    date_only_formats = ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y%m%d")
    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        *date_only_formats,
    ):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            if fmt in date_only_formats:
                return parsed.date().isoformat()
            return parsed.isoformat(sep=" ", timespec="seconds")
        except ValueError:
            pass
    normalized = re.sub(r"[./]", "-", text)
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})(.*)$", normalized)
    if m:
        yyyy, mm, dd, rest = m.groups()
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}{rest}"
    return normalized


def parse_krx_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    text = re.sub(r"\s+", "", text)
    text = text.replace("년", ".").replace("월", ".").replace("일", "")
    text = text.replace("/", ".").replace("-", ".")
    text = text.strip(".")
    m = re.match(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})$", text)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return parse_api_datetime(text)


def parse_date_range(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None
    text = re.sub(r"\s+", " ", raw.strip())
    if not text:
        return None, None
    parts = re.split(r"\s*~\s*", text)
    if len(parts) >= 2:
        return parse_krx_date(parts[0]), parse_krx_date(parts[1])
    one = parse_krx_date(text)
    return one, None


def infer_amendment_type(*texts: Optional[str]) -> Optional[str]:
    merged = " ".join([t for t in texts if t]).strip()
    if not merged:
        return None
    for key in ("전부개정", "일부개정", "개정", "제정", "폐지"):
        if key in merged:
            return key
    return None


def extract_effective_date_from_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    normalized = re.sub(r"\s+", " ", text)
    patterns = (
        r"(?:시행(?:예정)?일)\s*[:：]?\s*(20\d{2}[./-]\d{1,2}[./-]\d{1,2})",
        r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2})\s*시행",
    )
    for pattern in patterns:
        m = re.search(pattern, normalized)
        if m:
            return parse_krx_date(m.group(1))
    return None


def html_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def get_text(elem: ET.Element, tag: str) -> Optional[str]:
    node = elem.find(tag)
    return node.text.strip() if node is not None and node.text else None


def extract_fss_ntt_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    qs = parse_qs(urlparse(url).query)
    return qs.get("nttId", [None])[0]


def extract_ksd_ntt_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/press-release/(\d+)", url)
    return m.group(1) if m else None


def extract_fsec_bbs_no(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    qs = parse_qs(urlparse(url).query)
    return qs.get("bbsNo", [None])[0]


def file_ext_from_name(name: Optional[str]) -> Optional[str]:
    if not name or "." not in name:
        return None
    return name.rsplit(".", 1)[-1].lower()


def infer_file_name_from_url(file_url: Optional[str]) -> str:
    if not file_url:
        return "unknown"
    parsed = urlparse(file_url)
    path = parsed.path
    if path.endswith("/"):
        path = path[:-1]
    name = path.split("/")[-1]
    return name if name else "unknown_file"


def normalize_attachment_title(file_name: Optional[str], file_url: Optional[str]) -> str:
    raw = (file_name or "").strip()
    if not raw and file_url:
        raw = infer_file_name_from_url(file_url)
    raw = re.sub(r"\s+", " ", raw).strip().lower()
    raw = re.sub(r"\.(pdf|hwpx|hwp|docx|doc|xlsx|xls|pptx|ppt|zip|txt)$", "", raw)
    return raw


def attachment_ext_priority(ext: Optional[str]) -> int:
    e = (ext or "").strip().lower().lstrip(".")
    ranking = {
        "pdf": 1,
        "hwpx": 2,
        "hwp": 3,
        "docx": 4,
        "doc": 5,
        "xlsx": 6,
        "xls": 7,
        "pptx": 8,
        "ppt": 9,
        "txt": 10,
        "zip": 11,
    }
    return ranking.get(e, 99)


def hash_text_sha256(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def normalize_published_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return date_str
    # E.g. "2026.03.04", "2026-03-04 12:34:56" -> "2026-03-04"
    return date_str[:10].replace(".", "-").replace("/", "-")


def extract_attachments_from_soup(soup: BeautifulSoup, base_url: str, source: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href_raw = (anchor.get("href") or "").strip()
        if not href_raw:
            continue
        href_lower = href_raw.lower()
        if href_lower.startswith(("javascript:", "#")):
            continue

        text = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True))
        text_lower = text.lower()
        looks_like_download = (
            "filedown" in href_lower
            or "download" in href_lower
            or "down.do" in href_lower
            or "/filesrc/" in href_lower
            or "/comm/getfile" in href_lower
            or "filety=attach" in href_lower
            or "file_ty=attach" in href_lower
            or bool(re.search(r"\.(pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip)(\b|$)", href_lower))
            or bool(re.search(r"\.(pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip)(\b|$)", text_lower))
        )
        if not looks_like_download:
            continue

        # Some pages expose a viewer URL that wraps the real file path in `file=...`.
        if "viewer.html" in href_lower and "file=" in href_lower:
            file_param = parse_qs(urlparse(href_raw).query).get("file", [None])[0]
            if file_param:
                href_raw = unquote(file_param)

        file_url = urljoin(base_url, href_raw)
        if file_url in seen:
            continue
        seen.add(file_url)

        # Ignore viewer page links when they are not resolved to the underlying file.
        if "viewer.html" in file_url.lower():
            continue

        file_name = text
        if file_name in {"", "다운로드", "파일다운로드", "download", "viewer", "뷰어"}:
            file_name = infer_file_name_from_url(file_url) or "attached_file"

        out.append(
            {
                "file_name": file_name,
                "file_url": file_url,
                "file_ext": file_ext_from_name(file_name) or file_ext_from_name(infer_file_name_from_url(file_url)),
                "source": source,
            }
        )

    return out


def normalize_org_name(name: Optional[str]) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", "", name).strip()


CANONICAL_ORG_BY_CHANNEL = {
    "bok_press_release": "\ud55c\uad6d\uc740\ud589",
    "fsc_press_explainer": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_rule_change_notice": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_regulation_notice": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_admin_guidance_notice": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_admin_guidance_enforcement": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_law_interpretation": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fsc_no_action_opinion": "\uae08\uc735\uc704\uc6d0\ud68c",
    "fss_press_release": "\uae08\uc735\uac10\ub3c5\uc6d0",
    "fss_press_explainer": "\uae08\uc735\uac10\ub3c5\uc6d0",
    "fss_admin_guidance_notice": "\uae08\uc735\uac10\ub3c5\uc6d0",
    "fss_admin_guidance_enforcement": "\uae08\uc735\uac10\ub3c5\uc6d0",
    "ksd_press_release": "\ud55c\uad6d\uc608\ud0c1\uacb0\uc81c\uc6d0",
    "ksd_rule_change_notice": "\ud55c\uad6d\uc608\ud0c1\uacb0\uc81c\uc6d0",
    "krx_recent_rule_change": "\ud55c\uad6d\uac70\ub798\uc18c",
    "krx_rule_change_notice": "\ud55c\uad6d\uac70\ub798\uc18c",
    "kofia_recent_rule_change": "\uae08\uc735\ud22c\uc790\ud611\ud68c",
    "kofia_rule_change_notice": "\uae08\uc735\ud22c\uc790\ud611\ud68c",
    "kfb_publicdata_other": "\uc804\uad6d\uc740\ud589\uc5f0\ud569\ud68c",
    "fsec_bbs_222": "\uae08\uc735\ubcf4\uc548\uc6d0",
}


def normalize_organization_by_channel(source_channel: Optional[str], organization: Optional[str]) -> Optional[str]:
    canonical = CANONICAL_ORG_BY_CHANNEL.get(source_channel or "")
    if canonical:
        return canonical
    return organization


def validate_dates(start_date: str, end_date: str) -> None:
    sd = dt.datetime.strptime(start_date, "%Y%m%d").date()
    ed = dt.datetime.strptime(end_date, "%Y%m%d").date()
    if ed < sd:
        raise ValueError("end-date must be >= start-date")


class HttpClient:
    def __init__(self, timeout: Tuple[int, int] = TIMEOUT, max_retries: int = MAX_RETRIES):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def _merged_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    def _request_with_retries(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(method=method, url=url, timeout=self.timeout, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(attempt)
        if last_error is None:
            raise RuntimeError("Unknown HTTP error")
        raise last_error

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        return self._request_with_retries(
            "GET",
            url,
            params=params,
            headers=self._merged_headers(headers),
        )

    def post(
        self,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        return self._request_with_retries(
            "POST",
            url,
            data=data,
            json=json_data,
            headers=self._merged_headers(headers),
        )


class ArticleRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_system TEXT NOT NULL,
              source_channel TEXT NOT NULL,
              source_item_id TEXT NOT NULL,
              title TEXT,
              published_at TEXT,
              organization TEXT,
              department TEXT,
              original_url TEXT,
              detail_url TEXT,
              effective_date TEXT,
              amendment_type TEXT,
              content_html TEXT,
              content_text TEXT,
              raw_json TEXT,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              UNIQUE(source_system, source_channel, source_item_id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attachments (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              article_id INTEGER NOT NULL,
              file_name TEXT,
              file_url TEXT NOT NULL,
              file_ext TEXT,
              url_hash TEXT,
              document_id INTEGER,
              processing_status TEXT DEFAULT 'pending',
              last_error TEXT,
              last_processed_at TEXT,
              raw_json TEXT,
              UNIQUE(article_id, file_url),
              FOREIGN KEY(article_id) REFERENCES articles(id)
            )
            """
        )
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
              UNIQUE(document_id, extractor_name, extractor_version),
              FOREIGN KEY(document_id) REFERENCES attachment_documents(id)
            )
            """
        )
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
              created_at TEXT NOT NULL,
              FOREIGN KEY(job_id) REFERENCES report_jobs(id)
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
              UNIQUE(report_output_id, article_id, attachment_id, document_id, extraction_id),
              FOREIGN KEY(report_output_id) REFERENCES report_outputs(id)
            )
            """
        )
        # FTS5 for Search Optimization
        self.conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                title, content_text, content='articles', content_rowid='id', tokenize='unicode61'
            )
            """
        )
        # Sync triggers for FTS
        self.conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
              INSERT INTO articles_fts(rowid, title, content_text) VALUES (new.id, new.title, new.content_text);
            END;
        """)
        self.conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
              INSERT INTO articles_fts(articles_fts, rowid, title, content_text) VALUES('delete', old.id, old.title, old.content_text);
            END;
        """)
        self.conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
              INSERT INTO articles_fts(articles_fts, rowid, title, content_text) VALUES('delete', old.id, old.title, old.content_text);
              INSERT INTO articles_fts(rowid, title, content_text) VALUES (new.id, new.title, new.content_text);
            END;
        """)

        existing_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(articles)").fetchall()}
        if "effective_date" not in existing_cols:
            self.conn.execute("ALTER TABLE articles ADD COLUMN effective_date TEXT")
        if "amendment_type" not in existing_cols:
            self.conn.execute("ALTER TABLE articles ADD COLUMN amendment_type TEXT")
        attachment_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(attachments)").fetchall()}
        if "url_hash" not in attachment_cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN url_hash TEXT")
        if "document_id" not in attachment_cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN document_id INTEGER")
        if "processing_status" not in attachment_cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN processing_status TEXT DEFAULT 'pending'")
        if "last_error" not in attachment_cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN last_error TEXT")
        if "last_processed_at" not in attachment_cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN last_processed_at TEXT")
        attachment_doc_cols = {
            row[1] for row in self.conn.execute("PRAGMA table_info(attachment_documents)").fetchall()
        }
        if "download_status" not in attachment_doc_cols:
            self.conn.execute("ALTER TABLE attachment_documents ADD COLUMN download_status TEXT DEFAULT 'pending'")
        if "url_hash" not in attachment_doc_cols:
            self.conn.execute("ALTER TABLE attachment_documents ADD COLUMN url_hash TEXT")
        report_output_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(report_outputs)").fetchall()}
        if "llm_status" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_status TEXT DEFAULT 'pending'")
        if "llm_provider" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_provider TEXT")
        if "llm_model" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_model TEXT")
        if "llm_prompt" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_prompt TEXT")
        if "llm_response_raw" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_response_raw TEXT")
        if "llm_completed_at" not in report_output_cols:
            self.conn.execute("ALTER TABLE report_outputs ADD COLUMN llm_completed_at TEXT")

        # Performance indexes must be created after compatibility ALTERs above.
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_org ON articles(organization)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_channel ON articles(source_channel)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_article ON attachments(article_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_url_hash ON attachments(url_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_document ON attachments(document_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_status ON attachments(processing_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_docs_status ON attachment_documents(download_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_docs_hash ON attachment_documents(url_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_attachment_extract_doc ON attachment_extractions(document_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_jobs_status ON report_jobs(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_outputs_job ON report_outputs(job_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_outputs_llm_status ON report_outputs(llm_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_report_sources_output ON report_output_sources(report_output_id)")
        self.conn.commit()

    def upsert_article(self, article: Dict[str, Any]) -> int:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO articles (
              source_system, source_channel, source_item_id, title, published_at, organization, department,
              original_url, detail_url, effective_date, amendment_type, content_html, content_text, raw_json, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_system, source_channel, source_item_id) DO UPDATE SET
              title=excluded.title,
              published_at=excluded.published_at,
              organization=excluded.organization,
              department=excluded.department,
              original_url=excluded.original_url,
              detail_url=excluded.detail_url,
              effective_date=excluded.effective_date,
              amendment_type=excluded.amendment_type,
              content_html=excluded.content_html,
              content_text=excluded.content_text,
              raw_json=excluded.raw_json,
              last_seen_at=excluded.last_seen_at
            """,
            (
                article["source_system"],
                article["source_channel"],
                article["source_item_id"],
                article.get("title"),
                normalize_published_date(article.get("published_at")),
                article.get("organization"),
                article.get("department"),
                article.get("original_url"),
                article.get("detail_url"),
                article.get("effective_date"),
                article.get("amendment_type"),
                article.get("content_html"),
                article.get("content_text"),
                json.dumps(article.get("raw", {}), ensure_ascii=False),
                ts,
                ts,
            ),
        )
        row = self.conn.execute(
            """
            SELECT id
            FROM articles
            WHERE source_system=? AND source_channel=? AND source_item_id=?
            """,
            (article["source_system"], article["source_channel"], article["source_item_id"]),
        ).fetchone()
        return int(row[0])

    def upsert_attachments(self, article_id: int, attachments: Sequence[Dict[str, Any]]) -> None:
        # Deduplicate by logical title within an article.
        # If duplicate titles exist, prefer richer/standard formats:
        # pdf > hwpx > hwp > others.
        best_by_title: Dict[str, Dict[str, Any]] = {}
        for att in attachments:
            file_url = att.get("file_url")
            title_key = normalize_attachment_title(att.get("file_name"), file_url)
            if not title_key:
                title_key = file_url or f"unknown_{len(best_by_title)}"
            current = best_by_title.get(title_key)
            if current is None:
                best_by_title[title_key] = att
                continue

            cur_pri = attachment_ext_priority(current.get("file_ext") or file_ext_from_name(current.get("file_name")))
            new_pri = attachment_ext_priority(att.get("file_ext") or file_ext_from_name(att.get("file_name")))
            if new_pri < cur_pri:
                best_by_title[title_key] = att

        for att in best_by_title.values():
            file_url = att.get("file_url")
            url_hash = hash_text_sha256(file_url)
            self.conn.execute(
                """
                INSERT INTO attachments (
                  article_id, file_name, file_url, file_ext, url_hash, processing_status, raw_json
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(article_id, file_url) DO UPDATE SET
                  file_name=excluded.file_name,
                  file_ext=excluded.file_ext,
                  url_hash=excluded.url_hash,
                  processing_status='pending',
                  last_error=NULL,
                  last_processed_at=NULL,
                  raw_json=excluded.raw_json
                """,
                (
                    article_id,
                    att.get("file_name"),
                    file_url,
                    att.get("file_ext"),
                    url_hash,
                    json.dumps(att, ensure_ascii=False),
                ),
            )

    def fetch_summary(self) -> List[Tuple[str, str, int]]:
        return self.conn.execute(
            """
            SELECT source_system, source_channel, COUNT(*) AS cnt
            FROM articles
            GROUP BY source_system, source_channel
            ORDER BY source_system, source_channel
            """
        ).fetchall()

    def fetch_latest(self, limit: int = 20) -> List[Tuple[str, str, str, str, str, str]]:
        return self.conn.execute(
            """
            SELECT source_system, source_channel, source_item_id, title, published_at, original_url
            FROM articles
            ORDER BY COALESCE(published_at, '') DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


class DataGoApiCollector:
    def __init__(self, http: HttpClient):
        self.http = http
        self.alias_map = {
            "재정경제부": {"재정경제부", "기획재정부"},
            "기획재정부": {"재정경제부", "기획재정부"},
        }

    def ingest(
        self,
        service_key: str,
        start_date: str,
        end_date: str,
        allowed_orgs: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        sd = dt.datetime.strptime(start_date, "%Y%m%d").date()
        ed = dt.datetime.strptime(end_date, "%Y%m%d").date()
        
        allow_set: set[str] = set()
        if allowed_orgs:
            allow_set = {normalize_org_name(x) for x in allowed_orgs if x}
            expanded = set(allow_set)
            for org in allow_set:
                expanded.update(self.alias_map.get(org, set()))
            allow_set = expanded

        out: List[Dict[str, Any]] = []
        current_sd = sd
        while current_sd <= ed:
            current_ed = current_sd + dt.timedelta(days=2)
            if current_ed > ed:
                current_ed = ed
            
            chunk_start = current_sd.strftime("%Y%m%d")
            chunk_end = current_ed.strftime("%Y%m%d")
            
            print(f"  [Data.go.kr] Fetching chunk {chunk_start} ~ {chunk_end}")
            chunk_results = self._fetch_chunk(service_key, chunk_start, chunk_end, allow_set)
            out.extend(chunk_results)
            
            current_sd = current_ed + dt.timedelta(days=1)
            if current_sd <= ed:
                time.sleep(1)
        
        return out

    def _fetch_chunk(self, service_key: str, start_date: str, end_date: str, allow_set: set[str]) -> List[Dict[str, Any]]:
        params = {
            "serviceKey": service_key,
            "startDate": start_date,
            "endDate": end_date,
            "numOfRows": "99999",
            "pageNo": "1",
        }
        resp = self.http.get(API_BASE, params=params)
        root = ET.fromstring(resp.content)

        result_code = root.findtext("./header/resultCode") or root.findtext(".//resultCode")
        result_msg = root.findtext("./header/resultMsg") or root.findtext(".//resultMsg")
        if result_code and result_code != "0":
            if result_code == "03":  # NODATA_ERROR
                return []
            raise RuntimeError(f"API error: code={result_code}, msg={result_msg}")

        out: List[Dict[str, Any]] = []
        for item in root.findall(".//NewsItem"):
            source_item_id = get_text(item, "NewsItemId")
            title = get_text(item, "Title")
            minister = get_text(item, "MinisterCode")
            minister_norm = normalize_org_name(minister)
            if allow_set and minister_norm not in allow_set:
                continue

            published_at = parse_api_datetime(get_text(item, "ApproveDate"))
            original_url = get_text(item, "OriginalUrl")
            content_html = get_text(item, "DataContents")
            content_text = html_to_text(content_html)

            file_names = [n.text.strip() if n is not None and n.text else None for n in item.findall("FileName")]
            file_urls = [n.text.strip() if n is not None and n.text else None for n in item.findall("FileUrl")]
            attachments = []
            for idx, file_url in enumerate(file_urls):
                if not file_url:
                    continue
                file_name = file_names[idx] if idx < len(file_names) else None
                attachments.append(
                    {
                        "file_name": file_name,
                        "file_url": file_url,
                        "file_ext": file_ext_from_name(file_name),
                        "source": "data_go_api",
                    }
                )

            out.append(
                {
                    "source_system": "data_go_api",
                    "source_channel": "korea_policy_briefing_press_release",
                    "source_item_id": source_item_id or original_url or title,
                    "title": title,
                    "published_at": published_at,
                    "organization": minister,
                    "department": None,
                    "original_url": original_url,
                    "detail_url": None,
                    "content_html": content_html,
                    "content_text": content_text,
                    "attachments": attachments,
                    "raw": {
                        "NewsItemId": source_item_id,
                        "MinisterCode": minister,
                        "ApproveDate": get_text(item, "ApproveDate"),
                        "OriginalUrl": original_url,
                    },
                }
            )
        return out

class ArirangNewsCollector:
    def __init__(
        self,
        http: HttpClient,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        source_file: Optional[str] = None,
        sleep_sec: float = 0.2,
    ):
        self.http = http
        self.api_url = api_url or os.getenv("ARIRANG_NEWS_API_URL")
        self.api_key = api_key or os.getenv("ARIRANG_NEWS_API_KEY")
        self.source_file = source_file or os.getenv("ARIRANG_NEWS_SOURCE_FILE")
        self.sleep_sec = sleep_sec

    @staticmethod
    def _pick(item: Dict[str, Any], *keys: str) -> Optional[str]:
        for key in keys:
            value = item.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    def _normalize_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source_item_id = self._pick(item, "id", "uuid", "hash", "article_id", "news_id", "source_item_id")
        title = self._pick(item, "title", "headline", "newsTitle")
        published_raw = self._pick(item, "published_at", "publishedAt", "pubDate", "regDate", "date")
        content_html = self._pick(item, "content_html", "contentHtml", "body_html", "bodyHtml")
        content_text = self._pick(
            item,
            "content_text",
            "contentText",
            "body",
            "full_text",
            "fullText",
            "description",
            "summary",
        )
        if not content_text and content_html:
            content_text = html_to_text(content_html)

        if not source_item_id:
            fallback_source = self._pick(item, "detail_url", "original_url", "link", "url")
            source_item_id = fallback_source or title
        if not source_item_id or not title:
            return None

        published_at = parse_api_datetime(published_raw or "")
        original_url = self._pick(item, "original_url", "originalUrl", "link", "url")
        detail_url = self._pick(item, "detail_url", "detailUrl", "link", "url") or original_url
        organization = self._pick(item, "organization", "publisher", "press", "source") or "Arirang News"

        raw = dict(item)
        raw["collector"] = "arirang_news_api"
        return {
            "source_system": "arirang_news_api",
            "source_channel": "arirang_news_api",
            "source_item_id": source_item_id,
            "title": title,
            "published_at": published_at,
            "organization": organization,
            "department": None,
            "original_url": original_url,
            "detail_url": detail_url,
            "content_html": content_html,
            "content_text": content_text,
            "attachments": [],
            "raw": raw,
        }

    def _load_from_file(self) -> List[Dict[str, Any]]:
        if not self.source_file:
            return []
        path = Path(self.source_file)
        if not path.exists():
            raise FileNotFoundError(f"Arirang news source file not found: {path}")

        text = path.read_text(encoding="utf-8-sig")
        payload = json.loads(text)
        if isinstance(payload, dict):
            items = payload.get("items") or payload.get("data") or payload.get("articles") or []
        elif isinstance(payload, list):
            items = payload
        else:
            items = []

        out: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            normalized = self._normalize_item(item)
            if normalized:
                out.append(normalized)
        return out

    def _load_from_api(self, start_date: str, end_date: str, max_pages: int = 1) -> List[Dict[str, Any]]:
        if not self.api_url:
            return []

        out: List[Dict[str, Any]] = []
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        for page in range(1, max_pages + 1):
            params = {
                "startDate": start_date,
                "endDate": end_date,
                "page": str(page),
            }
            resp = self.http.get(self.api_url, params=params, headers=headers)
            body = resp.json()
            if isinstance(body, dict):
                items = body.get("items") or body.get("data") or body.get("articles") or []
            elif isinstance(body, list):
                items = body
            else:
                items = []

            page_items = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                normalized = self._normalize_item(item)
                if normalized:
                    out.append(normalized)
                    page_items += 1
            if page_items == 0:
                break
            time.sleep(self.sleep_sec)
        return out

    def ingest(self, start_date: str, end_date: str, max_pages: int = 1) -> List[Dict[str, Any]]:
        file_items = self._load_from_file()
        if file_items:
            print(f"[ArirangNews:file] loaded {len(file_items)} items from {self.source_file}")
            return file_items

        api_items = self._load_from_api(start_date=start_date, end_date=end_date, max_pages=max_pages)
        print(f"[ArirangNews:api] loaded {len(api_items)} items")
        return api_items


class FssCollector:
    def __init__(
        self,
        http: HttpClient,
        base_url: str = FSS_BASE,
        boards: Optional[Dict[str, Dict[str, str]]] = None,
        sleep_sec: float = 0.05,
    ):
        self.http = http
        self.base_url = base_url
        self.boards = boards or FSS_BOARDS
        self.sleep_sec = sleep_sec

    def _list_url(self, board: Dict[str, str]) -> str:
        return f"{self.base_url}/fss/bbs/{board['bbs_id']}/list.do"

    def _get_total_pages(self, board: Dict[str, str]) -> int:
        resp = self.http.get(self._list_url(board), params={"menuNo": board["menu_no"]})
        soup = BeautifulSoup(resp.text, "html.parser")
        max_page = 1
        for link in soup.select(".pagination a[href]"):
            href = link.get("href", "")
            m = re.search(r"fnSearch\((\d+)\)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _parse_list(self, board: Dict[str, str], page_index: int) -> List[Dict[str, Any]]:
        params = {"menuNo": board["menu_no"], "pageIndex": page_index}
        resp = self.http.get(self._list_url(board), params=params)
        soup = BeautifulSoup(resp.text, "html.parser")

        headers = [normalize_org_name(th.get_text(" ", strip=True)) for th in soup.select("thead th")]
        idx = {name: i for i, name in enumerate(headers)}

        title_idx = idx.get("제목", 1)
        dept_idx = idx.get("담당부서", 2)
        date_idx = idx.get("등록일", 3)
        no_idx = idx.get("번호", 0)
        views_idx = idx.get("조회수", -1)

        rows: List[Dict[str, Any]] = []
        for tr in soup.select("tbody tr"):
            tds = tr.find_all("td")
            if not tds or title_idx >= len(tds):
                continue

            anchor = tds[title_idx].find("a")
            if not anchor or not anchor.get("href"):
                continue

            detail_url = urljoin(self.base_url, anchor["href"])
            if views_idx < 0:
                real_views_idx = len(tds) - 1
            else:
                real_views_idx = views_idx

            rows.append(
                {
                    "number": tds[no_idx].get_text(" ", strip=True) if no_idx < len(tds) else None,
                    "title": anchor.get_text(" ", strip=True),
                    "department": tds[dept_idx].get_text(" ", strip=True) if dept_idx < len(tds) else None,
                    "date": tds[date_idx].get_text(" ", strip=True) if date_idx < len(tds) else None,
                    "views": tds[real_views_idx].get_text(" ", strip=True) if real_views_idx < len(tds) else None,
                    "detail_url": detail_url,
                    "ntt_id": extract_fss_ntt_id(detail_url),
                }
            )
        return rows

    def _parse_detail(self, detail_url: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(detail_url)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  [FSS Detail] error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": []}
        view = soup.select_one(".bd-view")
        if not view:
            return {"content_html": None, "content_text": None, "attachments": [], "department_detail": None}

        department_detail = None
        for dl in view.select("dl"):
            dt_nodes = dl.find_all("dt", recursive=False)
            dd_nodes = dl.find_all("dd", recursive=False)
            for dtn, ddn in zip(dt_nodes, dd_nodes):
                if "담당부서" in dtn.get_text(" ", strip=True):
                    department_detail = ddn.get_text(" ", strip=True)

        content_tag = view.select_one(".dbdata")
        content_html = str(content_tag) if content_tag else None
        content_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not content_text and view:
            content_text = view.get_text("\n", strip=True)

        attachments = []
        for anchor in view.select(".file-list a[href*='fileDown.do']"):
            href = anchor.get("href")
            if not href:
                continue
            file_url = urljoin(self.base_url, href)
            file_name = anchor.get_text(" ", strip=True)
            attachments.append(
                {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_ext": file_ext_from_name(file_name),
                    "source": "fss_scrape",
                }
            )
        return {
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
            "department_detail": department_detail,
        }

    def ingest(self, max_pages_each: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for channel_key, board in self.boards.items():
            total = self._get_total_pages(board)
            pages = min(total, max_pages_each) if max_pages_each else total
            print(f"[FSS:{board['name']}] pages={pages} (total={total})")

            for page in range(1, pages + 1):
                rows = self._parse_list(board, page)
                print(f"  page {page}/{pages}: {len(rows)} items")
                for row in rows:
                    detail = self._parse_detail(row["detail_url"])
                    published_at = parse_api_datetime(row.get("date") or "")
                    out.append(
                        {
                            "source_system": "fss_scrape",
                            "source_channel": channel_key,
                            "source_item_id": row["ntt_id"] or row["detail_url"],
                            "title": row["title"],
                            "published_at": published_at,
                            "organization": "금융감독원",
                            "department": detail["department_detail"] or row["department"],
                            "original_url": row["detail_url"],
                            "detail_url": row["detail_url"],
                            "content_html": detail["content_html"],
                            "content_text": detail["content_text"],
                            "attachments": detail["attachments"],
                            "raw": row,
                        }
                    )
                    time.sleep(self.sleep_sec)
        return out


FSC_BASE = "https://www.fsc.go.kr"
FSC_BOARDS = {
    "fsc_press_explainer": {"board_id": "no010102", "name": "보도설명자료"},
    "fsc_rule_change_notice": {"board_id": "po040301", "name": "Rule Change Notice"},
    "fsc_regulation_notice": {"board_id": "po040200", "name": "Regulation/Notice"},
}

class FscCollector:
    def __init__(
        self,
        http: HttpClient,
        base_url: str = FSC_BASE,
        boards: Optional[Dict[str, Dict[str, str]]] = None,
        sleep_sec: float = 0.05,
    ):
        self.http = http
        self.base_url = base_url
        self.boards = boards or FSC_BOARDS
        self.sleep_sec = sleep_sec

    def _list_url(self, board: Dict[str, str]) -> str:
        return f"{self.base_url}/{board['board_id']}"

    def _get_total_pages(self, board: Dict[str, str]) -> int:
        resp = self.http.get(self._list_url(board), params={"curPage": 1})
        soup = BeautifulSoup(resp.text, "html.parser")
        max_page = 1
        for link in soup.select(".pagination a[href]"):
            href = link.get("href", "")
            m = re.search(r"curPage=(\d+)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _parse_list(self, board: Dict[str, str], page_index: int) -> List[Dict[str, Any]]:
        params = {"curPage": page_index}
        resp = self.http.get(self._list_url(board), params=params)
        soup = BeautifulSoup(resp.text, "html.parser")

        wrap = soup.select_one(".board-list-wrap")
        rows: List[Dict[str, Any]] = []
        if not wrap:
            return rows

        for li in wrap.select("li"):
            anchor = li.select_one("a")
            if not anchor:
                continue

            title_tag = anchor.select_one(".subject")
            title = title_tag.get_text(strip=True) if title_tag else anchor.get_text(strip=True)
            title = re.sub(r"\.\s*금일 등록된 게시글$", "", title).strip()

            detail_url = urljoin(self.base_url, anchor.get("href", ""))
            
            department = None
            date_str = None
            
            info = li.select_one(".info")
            if info:
                for span in info.select("span"):
                    text = span.get_text(" ", strip=True)
                    if "담당부서" in text:
                        department = text.replace("담당부서 :", "").strip()

            date_div = li.select_one(".date")
            if date_div:
                date_str = date_div.get_text(strip=True)
            else:
                for s in li.stripped_strings:
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
                        date_str = s
                        break

            ntt_id = ""
            board_id = board.get("board_id", "")
            m = re.search(rf"/{re.escape(board_id)}/(\d+)", detail_url)
            if not m:
                m = re.search(r"/(?:no|po)\d{6}/(\d+)", detail_url)
            if m:
                ntt_id = m.group(1)
            if not ntt_id:
                q = parse_qs(urlparse(detail_url).query)
                ntt_id = q.get("noticeId", [""])[0] or q.get("nttId", [""])[0] or q.get("bbsNo", [""])[0]

            rows.append(
                {
                    "title": title,
                    "department": department,
                    "date": date_str,
                    "detail_url": detail_url,
                    "ntt_id": ntt_id,
                }
            )

        return rows

    def _parse_detail(self, detail_url: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(detail_url)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  [FSC Detail] error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": []}
        
        view = soup.select_one(".board-view-wrap") or soup.select_one(".board-view") or soup.select_one(".bdView")
        if not view:
            view = soup

        content_tag = view.select_one(".cont") or view.select_one(".dbdata") or view.select_one(".txt")
        content_html = str(content_tag) if content_tag else None
        content_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not content_text and view:
            content_text = view.get_text("\n", strip=True)

        attachments = extract_attachments_from_soup(soup, self.base_url, "fsc_scrape")
        return {
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
        }

    def ingest(self, max_pages_each: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for channel_key, board in self.boards.items():
            total = self._get_total_pages(board)
            if total < 1:
                total = 1
            pages = min(total, max_pages_each) if max_pages_each else total
            print(f"[FSC:{board['name']}] pages={pages} (total={total})")

            for page in range(1, pages + 1):
                rows = self._parse_list(board, page)
                print(f"  page {page}/{pages}: {len(rows)} items")
                for row in rows:
                    if not row["detail_url"]:
                        continue
                    detail = self._parse_detail(row["detail_url"])
                    published_at = parse_api_datetime(row.get("date") or "")

                    out.append(
                        {
                            "source_system": "fsc_scrape",
                            "source_channel": channel_key,
                            "source_item_id": row.get("ntt_id") or row["detail_url"],
                            "title": row.get("title"),
                            "published_at": published_at,
                            "organization": "금융위원회",
                            "department": row.get("department"),
                            "original_url": row["detail_url"],
                            "detail_url": row["detail_url"],
                            "content_html": detail.get("content_html"),
                            "content_text": detail.get("content_text"),
                            "attachments": detail.get("attachments", []),
                            "raw": row,
                        }
                    )
                    time.sleep(self.sleep_sec)
        return out


FSC_ADMIN_CHANNELS = {
    "fsc_admin_guidance_notice": {
        "name": "\ud589\uc815\uc9c0\ub3c4 \uc608\uace0",
        "mu_no": "144",
        "list_referer_path": "/fsc_new/status/adminMap/PrvntcList.do?stNo=11&muNo=144&muGpNo=60",
        "list_path": "/fsc_new/status/adminMap/selectPrvntcList.do?actCd=R",
        "detail_path": "/fsc_new/status/adminMap/PrvntcDetail.do",
    },
    "fsc_admin_guidance_enforcement": {
        "name": "\ud589\uc815\uc9c0\ub3c4 \uc2dc\ud589",
        "mu_no": "145",
        "list_referer_path": "/fsc_new/status/adminMap/OpertnList.do?stNo=11&muNo=145&muGpNo=60",
        "list_path": "/fsc_new/status/adminMap/selectOpertnList.do?actCd=R",
        "detail_path": "/fsc_new/status/adminMap/OpertnDetail.do",
    },
}


class FscAdminGuidanceCollector:
    def __init__(self, http: HttpClient, base_url: str = FSC_ADMIN_BASE, sleep_sec: float = 0.05):
        self.http = http
        self.base_url = base_url
        self.sleep_sec = sleep_sec

    def _post_json(self, path: str, data: Dict[str, Any], referer: Optional[str] = None) -> Dict[str, Any]:
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if referer:
            headers["Referer"] = referer
        resp = self.http.post(urljoin(self.base_url, path), data=data, headers=headers)
        return resp.json()

    def _fetch_list_page(self, config: Dict[str, str], page: int) -> Dict[str, Any]:
        length = 10
        payload = {
            "draw": str(page),
            "start": str((page - 1) * length),
            "length": str(length),
            "searchKeyword": "",
            "searchCondition": "",
            "searchDpNo": "",
            "searchDpNo1": "",
            "searchDpNo2": "",
            "searchDpNo3": "",
            "searchStartDt": "",
            "searchEndDt": "",
            "searchAddFild4": "",
            "muNo": config["mu_no"],
        }
        referer = urljoin(self.base_url, config.get("list_referer_path", ""))
        return self._post_json(config["list_path"], payload, referer=referer)

    def _th_text(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        for th in soup.select("th"):
            if normalize_org_name(label) in normalize_org_name(th.get_text(" ", strip=True)):
                td = th.find_next("td")
                if td:
                    return td.get_text(" ", strip=True)
        return None

    def _fetch_detail(self, config: Dict[str, str], post_no: str) -> Dict[str, Any]:
        payload = {
            "muNo": config["mu_no"],
            "postNo": str(post_no),
            "stNo": "11",
            "prevStNo": "11",
            "prevMuNo": config["mu_no"],
            "prevTab1": "",
            "prevTab2": "",
            "actCd": "R",
        }
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": urljoin(self.base_url, config.get("list_referer_path", "")),
        }
        resp = self.http.post(urljoin(self.base_url, config["detail_path"]), data=payload, headers=headers)
        soup = BeautifulSoup(resp.text, "html.parser")
        view = soup.select_one(".board-view") or soup
        content_node = view.select_one(".contents") or view.select_one(".cont") or view
        content_html = str(content_node) if content_node else None
        content_text = html_to_text(content_html)
        attachments = extract_attachments_from_soup(soup, self.base_url, "fsc_admin_scrape")
        return {
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
            "department_path": self._th_text(soup, "\uae30\uad00"),
            "effective_date": parse_krx_date(self._th_text(soup, "\uc2dc\ud589\uc77c")),
            "status": self._th_text(soup, "\uc2dc\ud589\uc5ec\ubd80"),
            "final_extension_date": parse_krx_date(self._th_text(soup, "\ucd5c\uc885\uc5f0\uc7a5\uc77c")),
            "validity_period": self._th_text(soup, "\uc874\uc18d\uae30\uac04"),
            "notice_period": self._th_text(soup, "\uc608\uace0\uae30\uac04"),
        }

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        failed_channels: List[str] = []
        for channel, config in FSC_ADMIN_CHANNELS.items():
            try:
                first = self._fetch_list_page(config, 1)
            except Exception as e:
                print(f"[WARN] [FSC-ADMIN:{config['name']}] list page 1 failed: {e}")
                failed_channels.append(channel)
                continue

            total = int(first.get("recordsTotal") or 0)
            pages_total = max(1, (total + 9) // 10)
            pages = min(pages_total, max_pages) if max_pages else pages_total
            print(f"[FSC-ADMIN:{config['name']}] page 1/{pages}: {len(first.get('data') or [])} items")
            rows = list(first.get("data") or [])
            for page in range(2, pages + 1):
                try:
                    data = self._fetch_list_page(config, page)
                except Exception as e:
                    print(f"[WARN] [FSC-ADMIN:{config['name']}] list page {page}/{pages} failed: {e}")
                    continue
                page_rows = data.get("data") or []
                print(f"[FSC-ADMIN:{config['name']}] page {page}/{pages}: {len(page_rows)} items")
                rows.extend(page_rows)

            for row in rows:
                post_no = str(row.get("postNo") or "").strip()
                if not post_no:
                    continue
                try:
                    detail = self._fetch_detail(config, post_no)
                except Exception as e:
                    print(f"  [FSC ADMIN Detail] error postNo={post_no}: {e}")
                    detail = {"content_html": None, "content_text": None, "attachments": []}

                title = (row.get("title") or "").strip()
                department_path = detail.get("department_path") or (row.get("addFild2") or "")
                department = department_path.split(">")[-1].strip() if ">" in department_path else (row.get("dpNm") or None)

                if channel == "fsc_admin_guidance_notice":
                    notice_start = parse_krx_date(str(row.get("eventStartDate") or ""))
                    notice_end = parse_krx_date(str(row.get("eventEndDate") or ""))
                    if not notice_start and detail.get("notice_period"):
                        notice_start, notice_end = parse_date_range(detail["notice_period"])
                    published_at = notice_start
                    effective_date = extract_effective_date_from_text(detail.get("content_text"))
                    amendment_type = infer_amendment_type(title, detail.get("content_text"))
                else:
                    published_at = parse_krx_date(str(row.get("addFild2") or "")) or detail.get("effective_date")
                    effective_date = parse_krx_date(str(row.get("addFild2") or "")) or detail.get("effective_date")
                    amendment_type = (row.get("addFild4") or "").strip() or detail.get("status")
                    notice_start = parse_krx_date(str(row.get("eventStartDate") or ""))
                    notice_end = parse_krx_date(str(row.get("eventEndDate") or ""))

                detail_url = (
                    f"{self.base_url}{config['detail_path']}?muNo={config['mu_no']}&postNo={quote(post_no)}&stNo=11&actCd=R"
                )
                out.append(
                    {
                        "source_system": "fsc_admin_scrape",
                        "source_channel": channel,
                        "source_item_id": post_no,
                        "title": title,
                        "published_at": published_at,
                        "organization": "\uae08\uc735\uc704\uc6d0\ud68c",
                        "department": department,
                        "original_url": detail_url,
                        "detail_url": detail_url,
                        "effective_date": effective_date,
                        "amendment_type": amendment_type,
                        "content_html": detail.get("content_html"),
                        "content_text": detail.get("content_text"),
                        "attachments": detail.get("attachments", []),
                        "raw": {
                            "list_row": row,
                            "notice_start": notice_start,
                            "notice_end": notice_end,
                            "department_path": department_path,
                            "final_extension_date": detail.get("final_extension_date"),
                            "validity_period": detail.get("validity_period"),
                            "enforcement_status": detail.get("status"),
                        },
                    }
                )
                time.sleep(self.sleep_sec)

        if failed_channels and len(failed_channels) == len(FSC_ADMIN_CHANNELS):
            raise RuntimeError("FSC Admin list endpoints failed for all channels")
        return out


FSS_ADMIN_CHANNELS = {
    "fss_admin_guidance_notice": {
        "name": "\ud589\uc815\uc9c0\ub3c4 \uc608\uace0",
        "menu_no": "200491",
        "list_path": "/fss/job/admnPrvntc/list.do",
    },
    "fss_admin_guidance_enforcement": {
        "name": "\ud589\uc815\uc9c0\ub3c4 \uc2dc\ud589",
        "menu_no": "200492",
        "list_path": "/fss/job/admnstgudc/list.do",
    },
}


class FssAdminGuidanceCollector:
    def __init__(self, http: HttpClient, base_url: str = FSS_BASE, sleep_sec: float = 0.05):
        self.http = http
        self.base_url = base_url
        self.sleep_sec = sleep_sec

    def _list_url(self, config: Dict[str, str]) -> str:
        return f"{self.base_url}{config['list_path']}"

    def _get_total_pages(self, config: Dict[str, str]) -> int:
        resp = self.http.get(self._list_url(config), params={"menuNo": config["menu_no"], "pageIndex": 1})
        soup = BeautifulSoup(resp.text, "html.parser")
        max_page = 1
        for link in soup.select(".pagination a[href]"):
            href = link.get("href", "")
            m = re.search(r"fnSearch\((\d+)\)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _fetch_list_rows(self, config: Dict[str, str], page_index: int) -> List[Dict[str, Any]]:
        list_url = self._list_url(config)
        resp = self.http.get(list_url, params={"menuNo": config["menu_no"], "pageIndex": page_index})
        soup = BeautifulSoup(resp.text, "html.parser")
        rows: List[Dict[str, Any]] = []
        for tr in soup.select("tbody tr"):
            tds = tr.find_all("td")
            anchor = tr.select_one("a[href]")
            if not tds or not anchor:
                continue
            detail_url = urljoin(list_url, anchor.get("href", ""))
            qs = parse_qs(urlparse(detail_url).query)
            item_id = qs.get("guGuidanceMgrSeq", [None])[0] or qs.get("seqno", [None])[0]
            if "admnPrvntc" in config["list_path"]:
                period = tds[2].get_text(" ", strip=True) if len(tds) > 2 else ""
                notice_start, notice_end = parse_date_range(period)
                rows.append(
                    {
                        "seq": tds[0].get_text(" ", strip=True),
                        "item_id": item_id,
                        "title": anchor.get_text(" ", strip=True),
                        "detail_url": detail_url,
                        "notice_start": notice_start,
                        "notice_end": notice_end,
                    }
                )
            else:
                rows.append(
                    {
                        "seq": tds[0].get_text(" ", strip=True),
                        "item_id": item_id,
                        "title": anchor.get_text(" ", strip=True),
                        "department": tds[2].get_text(" ", strip=True) if len(tds) > 2 else None,
                        "effective_date": parse_krx_date(tds[3].get_text(" ", strip=True) if len(tds) > 3 else None),
                        "status": tds[4].get_text(" ", strip=True) if len(tds) > 4 else None,
                        "detail_url": detail_url,
                    }
                )
        return rows

    def _parse_detail(self, detail_url: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(detail_url)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  [FSS Admin Detail] error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": [], "department": None}

        view = soup.select_one(".bd-view")
        if not view:
            return {"content_html": None, "content_text": None, "attachments": [], "department": None}

        department = None
        for dl in view.select("dl"):
            dt_nodes = dl.find_all("dt", recursive=False)
            dd_nodes = dl.find_all("dd", recursive=False)
            for dtn, ddn in zip(dt_nodes, dd_nodes):
                if "\ub2f4\ub2f9\ubd80\uc11c" in dtn.get_text(" ", strip=True):
                    department = ddn.get_text(" ", strip=True)

        content_tag = view.select_one(".dbdata") or view
        raw_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not raw_text:
            raw_text = view.get_text("\n", strip=True)

        # Keep only the actual detailed guidance block (bullet lines), excluding header/meta table text.
        norm_lines = []
        for line in (raw_text or "").splitlines():
            line = re.sub(r"\s+", " ", line).strip()
            if line:
                norm_lines.append(line)
        bullet_lines = [line for line in norm_lines if re.match(r"^-\s*\[[^\]]+\]\s*", line)]
        if bullet_lines:
            content_text = "- 아래 -\n" + "\n".join(bullet_lines)
            content_html = (
                "<div class=\"fss-admin-guidance-body\">"
                + "<p>- 아래 -</p>"
                + "".join(f"<p>{html.escape(line)}</p>" for line in bullet_lines)
                + "</div>"
            )
        else:
            content_text = raw_text
            content_html = str(content_tag) if content_tag else None

        attachments: List[Dict[str, Any]] = []
        for anchor in soup.select("a[href*='/fss.hpdownload']"):
            href = anchor.get("href")
            if not href:
                continue
            file_url = urljoin(self.base_url, href)
            q = parse_qs(urlparse(file_url).query)
            file_name = anchor.get_text(" ", strip=True) or unquote((q.get("filere", [""])[0] or "").replace("+", " "))
            if not file_name:
                file_name = unquote((q.get("file", [""])[0] or "").replace("+", " "))
            attachments.append(
                {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_ext": file_ext_from_name(file_name),
                    "source": "fss_admin_scrape",
                }
            )
        return {
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
            "department": department,
        }

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for channel, config in FSS_ADMIN_CHANNELS.items():
            total_pages = self._get_total_pages(config)
            pages = min(total_pages, max_pages) if max_pages else total_pages
            for page in range(1, pages + 1):
                rows = self._fetch_list_rows(config, page)
                print(f"[FSS-ADMIN:{config['name']}] page {page}/{pages}: {len(rows)} items")
                for row in rows:
                    detail = self._parse_detail(row["detail_url"])
                    seq = row.get("item_id") or row.get("seq") or extract_fss_ntt_id(row["detail_url"]) or row["detail_url"]
                    title = row.get("title")
                    if channel == "fss_admin_guidance_notice":
                        notice_start = row.get("notice_start")
                        notice_end = row.get("notice_end")
                        effective_date = extract_effective_date_from_text(detail.get("content_text"))
                        amendment_type = infer_amendment_type(title, detail.get("content_text"))
                        published_at = notice_start
                    else:
                        notice_start = None
                        notice_end = None
                        effective_date = row.get("effective_date")
                        amendment_type = row.get("status")
                        published_at = row.get("effective_date")
                    out.append(
                        {
                            "source_system": "fss_admin_scrape",
                            "source_channel": channel,
                            "source_item_id": str(seq),
                            "title": title,
                            "published_at": published_at,
                            "organization": "\uae08\uc735\uac10\ub3c5\uc6d0",
                            "department": detail.get("department") or row.get("department"),
                            "original_url": row["detail_url"],
                            "detail_url": row["detail_url"],
                            "effective_date": effective_date,
                            "amendment_type": amendment_type,
                            "content_html": detail.get("content_html"),
                            "content_text": detail.get("content_text"),
                            "attachments": detail.get("attachments", []),
                            "raw": {"list_row": row, "notice_start": notice_start, "notice_end": notice_end},
                        }
                    )
                    time.sleep(self.sleep_sec)
        return out


FSC_REPLY_CASE_CHANNELS = {
    "fsc_law_interpretation": {
        "name": "\ubc95\ub839\ud574\uc11d",
        "mu_no": "85",
        "list_path": "/fsc_new/replyCase/selectReplyCaseLawreqList.do",
        "detail_path": "/fsc_new/replyCase/LawreqDetail.do",
        "id_key": "lawreqIdx",
        "number_key": "lawreqNumber",
    },
    "fsc_no_action_opinion": {
        "name": "\ube44\uc870\uce58\uc758\uacac\uc11c",
        "mu_no": "86",
        "list_path": "/fsc_new/replyCase/selectReplyCaseOpinionList.do",
        "detail_path": "/fsc_new/replyCase/OpinionDetail.do",
        "id_key": "opinionIdx",
        "number_key": "opinionNumber",
    },
}


class FscReplyCaseCollector:
    def __init__(self, http: HttpClient, base_url: str = FSC_ADMIN_BASE, sleep_sec: float = 0.05):
        self.http = http
        self.base_url = base_url
        self.sleep_sec = sleep_sec

    def _post_json(self, path: str, data: Dict[str, Any], referer: Optional[str] = None) -> Dict[str, Any]:
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if referer:
            headers["Referer"] = referer
        resp = self.http.post(urljoin(self.base_url, path), data=data, headers=headers)
        return resp.json()

    def _fetch_list_page(self, config: Dict[str, str], page: int) -> Dict[str, Any]:
        length = 10
        payload = {
            "draw": str(page),
            "start": str((page - 1) * length),
            "length": str(length),
            "searchKeyword": "",
            "searchCondition": "",
            "searchReplyRegDateStart": "",
            "searchReplyRegDateEnd": "",
            "searchStatus": "",
            "searchCategory": "",
            "searchLawType": "",
            "searchChartIdx": "",
        }
        return self._post_json(config["list_path"], payload, referer=urljoin(self.base_url, "/fsc_new/replyCase/List.do"))

    def _th_map(self, soup: BeautifulSoup) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for th in soup.select("th"):
            key = th.get_text(" ", strip=True)
            td = th.find_next("td")
            if not td:
                continue
            val = td.get_text(" ", strip=True)
            if key not in out or not out[key]:
                out[key] = val
        return out

    def _build_content(self, meta: Dict[str, str]) -> Tuple[str, str]:
        query = (meta.get("\uc9c8\uc758\uc694\uc9c0") or "").strip()
        reply = (meta.get("\ud68c\ub2f5") or "").strip()
        reason = (meta.get("\uc774\uc720") or "").strip()
        parts_text: List[str] = []
        parts_html = ["<div class=\"fsc-reply-case-body\">"]
        if query:
            parts_text.append(f"[질의요지] {query}")
            parts_html.append(f"<h4>질의요지</h4><p>{html.escape(query)}</p>")
        if reply:
            parts_text.append(f"[회답] {reply}")
            parts_html.append(f"<h4>회답</h4><p>{html.escape(reply)}</p>")
        if reason:
            parts_text.append(f"[이유] {reason}")
            parts_html.append(f"<h4>이유</h4><p>{html.escape(reason)}</p>")
        parts_html.append("</div>")
        return "".join(parts_html), "\n\n".join(parts_text).strip()

    def _fetch_detail(self, config: Dict[str, str], item_id: str) -> Dict[str, Any]:
        payload = {"muNo": config["mu_no"], "stNo": "11", config["id_key"]: str(item_id), "actCd": "R"}
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": urljoin(self.base_url, "/fsc_new/replyCase/List.do"),
        }
        resp = self.http.post(urljoin(self.base_url, config["detail_path"]), data=payload, headers=headers)
        soup = BeautifulSoup(resp.text, "html.parser")
        meta = self._th_map(soup)
        content_html, content_text = self._build_content(meta)
        attachments = extract_attachments_from_soup(soup, self.base_url, "fsc_reply_scrape")
        return {
            "meta": meta,
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
        }

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        failed_channels: List[str] = []
        for channel, config in FSC_REPLY_CASE_CHANNELS.items():
            try:
                first = self._fetch_list_page(config, 1)
            except Exception as e:
                print(f"[WARN] [FSC-REPLY:{config['name']}] list page 1 failed: {e}")
                failed_channels.append(channel)
                continue

            total = int(first.get("recordsTotal") or 0)
            pages_total = max(1, (total + 9) // 10)
            pages = min(pages_total, max_pages) if max_pages else pages_total
            rows = list(first.get("data") or [])
            print(f"[FSC-REPLY:{config['name']}] page 1/{pages}: {len(rows)} items")
            for page in range(2, pages + 1):
                try:
                    data = self._fetch_list_page(config, page)
                except Exception as e:
                    print(f"[WARN] [FSC-REPLY:{config['name']}] list page {page}/{pages} failed: {e}")
                    continue
                page_rows = data.get("data") or []
                print(f"[FSC-REPLY:{config['name']}] page {page}/{pages}: {len(page_rows)} items")
                rows.extend(page_rows)

            for row in rows:
                item_id = str(row.get(config["id_key"]) or "").strip()
                if not item_id:
                    continue
                try:
                    detail = self._fetch_detail(config, item_id)
                except Exception as e:
                    print(f"  [FSC Reply Detail] error {config['id_key']}={item_id}: {e}")
                    continue

                meta = detail.get("meta", {})
                reply_date = parse_krx_date(meta.get("\ud68c\uc2e0\uc77c"))
                status = (meta.get("\ucc98\ub9ac\uad6c\ubd84") or row.get("status") or "").strip()
                detail_url = (
                    f"{self.base_url}{config['detail_path']}?muNo={config['mu_no']}&stNo=11&{config['id_key']}={quote(item_id)}&actCd=R"
                )
                department = (meta.get("\uc18c\uad00\ubd80\uc11c") or "").replace(",", " > ").strip() or None
                out.append(
                    {
                        "source_system": "fsc_reply_scrape",
                        "source_channel": channel,
                        "source_item_id": item_id,
                        "title": (row.get("title") or "").strip(),
                        "published_at": reply_date,
                        "organization": "\uae08\uc735\uc704\uc6d0\ud68c",
                        "department": department,
                        "original_url": detail_url,
                        "detail_url": detail_url,
                        "effective_date": reply_date,
                        "amendment_type": status,
                        "content_html": detail.get("content_html"),
                        "content_text": detail.get("content_text"),
                        "attachments": detail.get("attachments", []),
                        "raw": {
                            "list_row": row,
                            "processing_status": status,
                            "public_yn": (meta.get("\uacf5\uac1c\uc5ec\ubd80") or "").strip(),
                            "registrant": (meta.get("\ub4f1\ub85d\uc790") or "").strip(),
                            "reply_date": reply_date,
                            "query_summary": (meta.get("\uc9c8\uc758\uc694\uc9c0") or "").strip(),
                            "reply_text": (meta.get("\ud68c\ub2f5") or "").strip(),
                            "reason_text": (meta.get("\uc774\uc720") or "").strip(),
                            "case_number": (meta.get("\uc77c\ub828\ubc88\ud638") or row.get(config["number_key"]) or "").strip(),
                        },
                    }
                )
                time.sleep(self.sleep_sec)

        if failed_channels and len(failed_channels) == len(FSC_REPLY_CASE_CHANNELS):
            raise RuntimeError("FSC ReplyCase list endpoints failed for all channels")
        return out


class KsdCollector:
    def __init__(
        self,
        http: HttpClient,
        base_url: str = KSD_BASE,
        menu_id: str = "KR_ABT_070200",
        file_store: str = "KR_NOT_000003",
    ):
        self.http = http
        self.base_url = base_url
        self.menu_id = menu_id
        self.file_store = file_store

    def _fetch_detail(self, ntt_id: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(f"{self.base_url}/ko/api/content/{ntt_id}", params={"menuId": self.menu_id})
            body = resp.json()
            if str(body.get("status")) != "200":
                return {}
            return body.get("body") or {}
        except Exception:
            return {}

    def _fetch_attachments(self, atch_file_id: Optional[str]) -> List[Dict[str, Any]]:
        if not atch_file_id:
            return []
        try:
            resp = self.http.get(
                f"{self.base_url}/ko/api/content/attach/{atch_file_id}",
                params={"fileStreCours": self.file_store},
            )
            body = resp.json()
            if str(body.get("status")) != "200":
                return []

            rows = (body.get("body") or {}).get("list") or []
            out: List[Dict[str, Any]] = []
            for row in rows:
                params = {
                    "atchFileId": row.get("atchFileId"),
                    "fileSn": row.get("fileSn"),
                    "fileStreCours": row.get("fileStreCours") or self.file_store,
                    "streFileNm": row.get("streFileNm"),
                    "orignlFileNm": row.get("orignlFileNm"),
                    "fileExtsn": row.get("fileExtsn"),
                    "fileSize": row.get("fileSize"),
                }
                params = {k: v for k, v in params.items() if v is not None}
                download_url = f"{self.base_url}/ko/api/download/attach?{urlencode(params)}"
                file_name = row.get("orignlFileNm")
                ext = (row.get("fileExtsn") or "").strip().lstrip(".").lower() or file_ext_from_name(file_name)
                out.append(
                    {
                        "file_name": file_name,
                        "file_url": download_url,
                        "file_ext": ext,
                        "source": "ksd_scrape",
                    }
                )
            return out
        except Exception:
            return []

    def ingest(self, max_pages: int = 3) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        rows = scrape_ksd_press_releases(max_pages=max_pages)
        for row in rows:
            detail_url = row.get("link")
            source_item_id = extract_ksd_ntt_id(detail_url) or detail_url or row.get("title")
            detail: Dict[str, Any] = {}
            if source_item_id and str(source_item_id).isdigit():
                detail = self._fetch_detail(str(source_item_id))

            content_html = detail.get("bbsCn")
            attachments = self._fetch_attachments(detail.get("atchFileId"))
            out.append(
                {
                    "source_system": "ksd_scrape",
                    "source_channel": "ksd_press_release",
                    "source_item_id": source_item_id,
                    "title": row.get("title"),
                    "published_at": parse_api_datetime(row.get("date") or ""),
                    "organization": "한국예탁결제원",
                    "department": None,
                    "original_url": detail_url,
                    "detail_url": detail_url,
                    "content_html": content_html,
                    "content_text": html_to_text(content_html),
                    "attachments": attachments,
                    "raw": {"list_row": row, "detail": detail},
                }
            )
        return out


class KfbCollector:
    def __init__(self, http: HttpClient, base_url: str = KFB_BASE, sleep_sec: float = 0.05):
        self.http = http
        self.base_url = base_url
        self.sleep_sec = sleep_sec

    def _list_url(self, page_index: int) -> str:
        return f"{self.base_url}/publicdata/data_other.php?pg={page_index}"

    def _parse_list(self, page_index: int, page) -> List[Dict[str, Any]]:
        url = self._list_url(page_index)
        page.goto(url)
        page.wait_for_load_state("networkidle", timeout=15000)
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        table = soup.select_one("table.pan_table")
        if not table:
            return []

        out: List[Dict[str, Any]] = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 6:
                continue

            title_anchor = tds[2].find("a", href=True)
            if not title_anchor:
                continue

            js_href = title_anchor.get("href", "")
            idx = None
            m = re.search(r"readRun\((\d+)\)", js_href, re.I)
            if m:
                idx = m.group(1)
            elif js_href:
                idx = parse_qs(urlparse(urljoin(self.base_url, js_href)).query).get("idx", [None])[0]
            if not idx:
                continue

            out.append(
                {
                    "number": tds[0].get_text(" ", strip=True),
                    "title": title_anchor.get_text(" ", strip=True),
                    "department": tds[3].get_text(" ", strip=True),
                    "date": tds[4].get_text(" ", strip=True),
                    "views": tds[5].get_text(" ", strip=True),
                    "idx": idx,
                    "detail_url": f"{self.base_url}/publicdata/data_other_view.php?idx={idx}&pg={page_index}",
                }
            )
        return out

    def _parse_detail(self, detail_url: str, page) -> Dict[str, Any]:
        try:
            page.goto(detail_url)
            page.wait_for_load_state("networkidle", timeout=15000)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            print(f"  [KFB Detail] Playwright error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": []}

        view = soup.select_one(".panViewArea")
        if not view:
            return {"content_html": None, "content_text": None, "attachments": []}

        content_tag = view.select_one(".viewInfo .txt") or view.select_one(".txt")
        content_html = str(content_tag) if content_tag else None
        content_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not content_text:
            content_text = view.get_text("\n", strip=True)

        attachments = extract_attachments_from_soup(view, self.base_url, "kfb_scrape")
        return {"content_html": content_html, "content_text": content_text, "attachments": attachments}

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        print(f"[KFB:기타자료] max_pages={max_pages}")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            list_page = browser.new_page()
            detail_page = browser.new_page()

            for page_index in range(1, max_pages + 1):
                rows = self._parse_list(page_index, list_page)
                if page_index > 1 and not rows:
                    break
                print(f"  page {page_index}: {len(rows)} items")
                for row in rows:
                    if not row.get("detail_url"):
                        continue
                    detail = self._parse_detail(row["detail_url"], detail_page)
                    out.append(
                        {
                            "source_system": "kfb_scrape",
                            "source_channel": "kfb_publicdata_other",
                            "source_item_id": row["idx"],
                            "title": row["title"],
                            "published_at": parse_api_datetime(row.get("date") or ""),
                            "organization": "전국은행연합회",
                            "department": row.get("department"),
                            "original_url": row["detail_url"],
                            "detail_url": row["detail_url"],
                            "content_html": detail.get("content_html"),
                            "content_text": detail.get("content_text"),
                            "attachments": detail.get("attachments", []),
                            "raw": row,
                        }
                    )
                    time.sleep(self.sleep_sec)
            browser.close()
        return out


class FsecCollector:
    def __init__(
        self,
        http: HttpClient,
        base_url: str = FSEC_BASE,
        menu_no: str = "222",
        sleep_sec: float = 0.05,
    ):
        self.http = http
        self.base_url = base_url
        self.menu_no = str(menu_no)
        self.sleep_sec = sleep_sec

    def _list_payload(self, page: int) -> Dict[str, Any]:
        return {
            "menuNo": self.menu_no,
            "pagingSearchDto": {
                "page": str(page),
                "pageSize": "6",
                "searchContents": "",
                "searchType": "TITLE",
            },
        }

    def _fetch_list(self, page: int) -> List[Dict[str, Any]]:
        try:
            api_resp = requests.post(
                f"{self.base_url}/bbs/list",
                json=self._list_payload(page),
                timeout=TIMEOUT,
            )
            api_resp.raise_for_status()
            body = api_resp.json()
            if body.get("resultCode") != "00":
                return []
            rows = body.get("resultData") or []
            return [r for r in rows if r.get("bbsNo")]
        except Exception:
            return []

    def _detail_url(self, bbs_no: str) -> str:
        return f"{self.base_url}/bbs/detail?menuNo={self.menu_no}&bbsNo={bbs_no}"

    def _parse_detail(self, detail_url: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(detail_url)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"  [FSEC Detail] error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": []}

        view = soup.select_one("#boardView")
        if not view:
            return {"content_html": None, "content_text": None, "attachments": []}

        title = (view.select_one(".titleBox h3") or view.select_one("h3"))
        info_tag = view.select_one(".titleBox .info .tag")
        info_date = view.select_one(".titleBox .info .date")
        content_tag = view.select_one(".cont")

        content_html = str(content_tag) if content_tag else None
        content_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not content_text:
            content_text = view.get_text("\n", strip=True)

        attachments: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in view.select(".downBox a"):
            file_no = anchor.get("fileno") or anchor.get("fileNo")
            file_page = anchor.get("filepage") or anchor.get("filePage") or "board"
            file_name = anchor.get_text(" ", strip=True)
            if file_no:
                query = urlencode({"fileNo": str(file_no), "filePage": str(file_page)})
                file_url = f"{self.base_url}/file/downloadFile?{query}"
            else:
                href = (anchor.get("href") or "").strip()
                if not href or href.lower().startswith("javascript"):
                    continue
                file_url = urljoin(self.base_url, href)
            if file_url in seen:
                continue
            seen.add(file_url)
            if not file_name:
                file_name = infer_file_name_from_url(file_url) or "attached_file"
            attachments.append(
                {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_ext": file_ext_from_name(file_name) or file_ext_from_name(infer_file_name_from_url(file_url)),
                    "source": "fsec_scrape",
                }
            )

        return {
            "title": title.get_text(" ", strip=True) if title else None,
            "reg_user": info_tag.get_text(" ", strip=True) if info_tag else None,
            "reg_date": info_date.get_text(" ", strip=True) if info_date else None,
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
        }

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        print(f"[FSEC:자료실] max_pages={max_pages}")
        for page in range(1, max_pages + 1):
            rows = self._fetch_list(page)
            if not rows:
                break
            print(f"  page {page}: {len(rows)} items")
            for row in rows:
                bbs_no = str(row.get("bbsNo"))
                detail_url = self._detail_url(bbs_no)
                detail = self._parse_detail(detail_url)

                attachments = detail.get("attachments") or []
                if not attachments and row.get("fileCount") and row.get("filePath"):
                    fallback_url = urljoin(self.base_url, row.get("filePath"))
                    fallback_name = row.get("fileNameOrg") or infer_file_name_from_url(fallback_url) or "attached_file"
                    attachments = [
                        {
                            "file_name": fallback_name,
                            "file_url": fallback_url,
                            "file_ext": file_ext_from_name(fallback_name),
                            "source": "fsec_scrape",
                        }
                    ]

                content_html = detail.get("content_html") or row.get("contents")
                content_text = detail.get("content_text") or html_to_text(content_html)

                out.append(
                    {
                        "source_system": "fsec_scrape",
                        "source_channel": "fsec_bbs_222",
                        "source_item_id": bbs_no,
                        "title": detail.get("title") or row.get("title"),
                        "published_at": parse_api_datetime(detail.get("reg_date") or row.get("regDate") or ""),
                        "organization": "금융보안원",
                        "department": detail.get("reg_user") or row.get("regUser"),
                        "original_url": detail_url,
                        "detail_url": detail_url,
                        "content_html": content_html,
                        "content_text": content_text,
                        "attachments": attachments,
                        "raw": row,
                    }
                )
                time.sleep(self.sleep_sec)
        return out


class BokCollector:
    def __init__(self, http: HttpClient, base_url: str = BOK_BASE, sleep_sec: float = 1.0):
        self.http = http
        self.base_url = base_url
        self.sleep_sec = sleep_sec

    def _parse_list(self, page_index: int, list_page) -> List[Dict[str, Any]]:
        url = (
            f"{self.base_url}/portal/singl/newsData/list.do?pageIndex={page_index}"
            "&targetDepth=3&menuNo=201263&searchCnd=1&searchKwd=%EC%99%B8%ED%99%98"
        )
        print(f"  [BOK] Fetching list page {page_index}: {url}")

        articles: List[Dict[str, Any]] = []
        try:
            list_page.goto(url)
            list_page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(2)

            html = list_page.content()
            soup = BeautifulSoup(html, "html.parser")

            for a_tag in soup.find_all("a", class_="title"):
                href = a_tag.get("href", "")
                if not href or "view.do" not in href:
                    continue

                title = a_tag.get_text(" ", strip=True)
                if not title:
                    continue

                parent = a_tag.parent
                if parent and parent.name == "div" and "set" in parent.get("class", []):
                    li = parent.parent
                else:
                    li = a_tag.find_parent("li")

                date_str = ""
                views = ""
                if li:
                    date_span = li.find("span", class_="date")
                    if date_span:
                        date_str = date_span.get_text(strip=True).replace("???", "").strip()
                        date_str = re.sub(r"[./]", "-", date_str).strip(" -")
                        date_str = re.sub(r"^[^\d]*(\d{4}-\d{1,2}-\d{1,2}).*$", r"\1", date_str)

                    hits_span = li.find("span", class_="hits")
                    if hits_span:
                        views = hits_span.get_text(strip=True).replace("???", "").replace("??", "").strip()

                detail_url = self.base_url + href
                articles.append(
                    {
                        "title": title,
                        "date": date_str,
                        "views": views,
                        "detail_url": detail_url,
                    }
                )
        except Exception as e:
            print(f"  [BOK] Error loading list page {page_index}: {e}")

        return articles

    def _parse_detail(self, detail_url: str, page) -> Dict[str, Any]:
        try:
            page.goto(detail_url)
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(1)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            print(f"  [BOK Detail] Playwright error {detail_url}: {e}")
            return {"content_html": None, "content_text": None, "attachments": []}

        view = soup.find("div", class_="bdView") or soup.find("div", class_="board-view") or soup.find("div", class_="content")
        if not view:
            return {"content_html": None, "content_text": None, "attachments": []}

        content_tag = view.find("div", class_="dbdata") or view.find("div", class_="cont") or view.find("div", class_="txt")
        if not content_tag:
            content_tag = view

        content_html = str(content_tag)
        content_text = content_tag.get_text("\n", strip=True) if content_tag else ""
        if not content_text and view:
            content_text = view.get_text("\n", strip=True)

        attachments = extract_attachments_from_soup(soup, self.base_url, "bok_scrape")
        return {
            "content_html": content_html,
            "content_text": content_text,
            "attachments": attachments,
        }

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        print(f"[BOK:press_release] max_pages={max_pages}")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            list_page = browser.new_page()
            detail_page = browser.new_page()

            for page in range(1, max_pages + 1):
                rows = self._parse_list(page, list_page)
                print(f"  page {page}: {len(rows)} items")
                for row in rows:
                    if not row.get("detail_url"):
                        continue

                    detail = self._parse_detail(row["detail_url"], detail_page)
                    published_at = parse_api_datetime(row.get("date") or "")
                    ntt_id = parse_qs(urlparse(row["detail_url"]).query).get("nttId", [None])[0]

                    out.append(
                        {
                            "source_system": "bok_scrape",
                            "source_channel": "bok_press_release",
                            "source_item_id": ntt_id or row["detail_url"],
                            "title": row.get("title"),
                            "published_at": published_at,
                            "organization": "한국은행",
                            "department": None,
                            "original_url": row["detail_url"],
                            "detail_url": row["detail_url"],
                            "content_html": detail.get("content_html"),
                            "content_text": detail.get("content_text"),
                            "attachments": detail.get("attachments", []),
                            "raw": row,
                        }
                    )
                    time.sleep(self.sleep_sec)

            browser.close()
        return out


class KsdRuleChangeNoticeCollector:
    def __init__(
        self,
        http: HttpClient,
        base_url: str = KSD_BASE,
        menu_id: str = "KR_IDR_040100",
        file_store: str = "KR_NOR_000003",
    ):
        self.http = http
        self.base_url = base_url
        self.menu_id = menu_id
        self.file_store = file_store

    def _fetch_list(self, page: int = 1, page_size: int = 10) -> List[Dict[str, Any]]:
        resp = self.http.get(
            f"{self.base_url}/ko/api/content",
            params={
                "menuId": self.menu_id,
                "pagingYn": "Y",
                "currentPage": page,
                "recordCountPerPage": page_size,
            },
        )
        body = resp.json()
        if str(body.get("status")) != "200":
            return []
        return (body.get("body") or {}).get("list") or []

    def _fetch_detail(self, ntt_id: str) -> Dict[str, Any]:
        try:
            resp = self.http.get(f"{self.base_url}/ko/api/content/{ntt_id}", params={"menuId": self.menu_id})
            body = resp.json()
            if str(body.get("status")) != "200":
                return {}
            return body.get("body") or {}
        except Exception:
            return {}

    def _fetch_attachments(self, atch_file_id: Optional[str]) -> List[Dict[str, Any]]:
        if not atch_file_id:
            return []
        try:
            resp = self.http.get(
                f"{self.base_url}/ko/api/content/attach/{atch_file_id}",
                params={"fileStreCours": self.file_store},
            )
            body = resp.json()
            if str(body.get("status")) != "200":
                return []

            rows = (body.get("body") or {}).get("list") or []
            out: List[Dict[str, Any]] = []
            for row in rows:
                params = {
                    "atchFileId": row.get("atchFileId"),
                    "fileSn": row.get("fileSn"),
                    "fileStreCours": row.get("fileStreCours") or self.file_store,
                    "streFileNm": row.get("streFileNm"),
                    "orignlFileNm": row.get("orignlFileNm"),
                    "fileExtsn": row.get("fileExtsn"),
                    "fileSize": row.get("fileSize"),
                }
                params = {k: v for k, v in params.items() if v is not None}
                download_url = f"{self.base_url}/ko/api/download/attach?{urlencode(params)}"
                file_name = row.get("orignlFileNm")
                ext = (row.get("fileExtsn") or "").strip().lstrip(".").lower() or file_ext_from_name(file_name)
                out.append(
                    {
                        "file_name": file_name,
                        "file_url": download_url,
                        "file_ext": ext,
                        "source": "ksd_rule_scrape",
                    }
                )
            return out
        except Exception:
            return []

    def ingest(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            rows = self._fetch_list(page=page, page_size=10)
            if not rows:
                break
            print(f"[KSD:rule_change_notice] page {page}: {len(rows)} items")

            for row in rows:
                ntt_id = str(row.get("nttId") or "").strip()
                if not ntt_id:
                    continue
                detail = self._fetch_detail(ntt_id)
                content_html = detail.get("bbsCn")
                attachments = self._fetch_attachments(detail.get("atchFileId"))
                out.append(
                    {
                        "source_system": "ksd_rule_scrape",
                        "source_channel": "ksd_rule_change_notice",
                        "source_item_id": ntt_id,
                        "title": row.get("bbsSj") or detail.get("bbsSj"),
                        "published_at": parse_api_datetime(str(row.get("frstRegistPnttm") or "")),
                        "organization": "한국예탁결제원",
                        "department": None,
                        "original_url": f"{self.base_url}/ko/information-disclosure/legal-information/notice-of-rules-revision/{ntt_id}",
                        "detail_url": f"{self.base_url}/ko/information-disclosure/legal-information/notice-of-rules-revision/{ntt_id}",
                        "content_html": content_html,
                        "content_text": html_to_text(content_html),
                        "attachments": attachments,
                        "raw": {"list_row": row, "detail": detail},
                    }
                )
        return out


class KrxCollector:
    def __init__(self, base_url: str = KRX_BASE):
        self.base_url = base_url

    def _bootstrap(self) -> Tuple[requests.Session, str]:
        session = requests.Session()
        resp = session.get(f"{self.base_url}/out/index.do", timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        csrf_node = soup.select_one("meta[name='_csrf']")
        csrf = csrf_node.get("content") if csrf_node else None
        if not csrf:
            raise RuntimeError("Failed to obtain KRX CSRF token")
        return session, csrf

    def _post(
        self,
        session: requests.Session,
        path: str,
        csrf: str,
        data: Optional[Dict[str, Any]] = None,
        referer_path: str = "/out/index.do",
        expect_json: bool = False,
        is_ajax: bool = False,
    ) -> Any:
        payload = dict(data or {})
        payload.setdefault("_csrf", csrf)
        headers = {
            "Origin": self.base_url,
            "Referer": f"{self.base_url}{referer_path}",
            "X-CSRF-TOKEN": csrf,
        }
        if is_ajax:
            headers["X-Requested-With"] = "XMLHttpRequest"
        resp = session.post(f"{self.base_url}{path}", data=payload, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json() if expect_json else resp.text

    def _extract_krx_attachments(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for li in soup.select("li.filename[onclick]"):
            onclick = li.get("onclick") or ""
            m = re.search(r"downFile\('([^']*)','([^']*)','([^']*)'\)", onclick)
            if not m:
                continue
            org_name, saved_name, file_gb = m.groups()
            file_name = (li.get_text(" ", strip=True) or org_name or saved_name).strip()
            file_url = (
                f"{self.base_url}/out/down/downLoad2.do"
                f"?orgFileName={quote(org_name)}&newFileName={quote(saved_name)}&fileGb={quote(file_gb)}"
            )
            out.append(
                {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_ext": file_ext_from_name(file_name),
                    "source": "krx_scrape",
                }
            )
        return out

    def ingest_recent_rule_changes(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        session, csrf = self._bootstrap()
        self._post(
            session,
            "/out/regulation/regulationMain.do",
            csrf,
            data={"Menucd": "BYLAW", "index": "law_rule", "gbn": "out", "mtype": "recent", "smType": "recent"},
        )
        self._post(
            session,
            "/out/regulation/regulationList.do",
            csrf,
            data={
                "Menucd": "BYLAW",
                "gbnid": "0",
                "mtype": "recent",
                "smType": "recent",
                "index": "law_rule",
                "gbn": "out",
                "pageno": "1",
                "rulepageno": "1",
            },
            referer_path="/out/regulation/regulationMain.do",
        )

        out: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            start = (page - 1) * 20
            data = self._post(
                session,
                "/web/regulation/regulationListData.do",
                csrf,
                data={
                    "start": str(start),
                    "limit": "20",
                    "pagesize": "20",
                    "mtype": "recent",
                    "Menucd": "BYLAW",
                    "stit": "규정",
                    "gbnid": "0",
                    "webgbn": "OUT",
                    "gbn2": "out",
                },
                referer_path="/out/regulation/regulationList.do",
                expect_json=True,
                is_ajax=True,
            )
            rows = data.get("result") or []
            if not rows:
                break
            print(f"[KRX:recent_rule_change] page {page}: {len(rows)} items")
            for row in rows:
                book_id = str(row.get("BOOKID") or "").strip()
                if not book_id:
                    continue
                noformyn = str(row.get("NOFORMYN") or "N")
                out.append(
                    {
                        "source_system": "krx_scrape",
                        "source_channel": "krx_recent_rule_change",
                        "source_item_id": book_id,
                        "title": row.get("TITLE"),
                        "published_at": parse_krx_date(str(row.get("PROMULDT") or "")),
                        "organization": "한국거래소",
                        "department": row.get("DEPTNAME"),
                        "original_url": f"{self.base_url}/out/regulation/regulationMain.do",
                        "detail_url": f"{self.base_url}/out/regulation/regulationViewPop.do?bookid={book_id}&noformyn={noformyn}",
                        "effective_date": parse_krx_date(str(row.get("STARTDT") or "")),
                        "amendment_type": (str(row.get("REVCD") or "").strip() or None),
                        "content_html": None,
                        "content_text": None,
                        "attachments": [],
                        "raw": row,
                    }
                )
        return out

    def _fetch_notice_detail(
        self,
        session: requests.Session,
        csrf: str,
        bbs_id: str,
        menu_id: str,
    ) -> Dict[str, Any]:
        try:
            html = self._post(
                session,
                "/out/pds/pdsView.do",
                csrf,
                data={"BBSID": bbs_id, "BBSCD": "PDS", "Menuid": menu_id},
                referer_path="/out/pds/pdsList.do",
            )
            soup = BeautifulSoup(html, "html.parser")
            content_node = soup.select_one("#conts")
            content_html = str(content_node) if content_node else None
            content_text = html_to_text(content_html)
            attachments = self._extract_krx_attachments(soup)
            return {
                "content_html": content_html,
                "content_text": content_text,
                "attachments": attachments,
                "raw_html": html,
            }
        except Exception:
            return {"content_html": None, "content_text": None, "attachments": [], "raw_html": None}

    def ingest_rule_change_notices(self, max_pages: int = 1, menu_id: str = "10000016") -> List[Dict[str, Any]]:
        session, csrf = self._bootstrap()
        self._post(
            session,
            "/out/pds/goPdsMain.do",
            csrf,
            data={"Menucd": "PDS", "index": "law_rule", "gbn": "out"},
        )
        self._post(
            session,
            "/out/pds/pdsList.do",
            csrf,
            data={"Menuid": menu_id, "index": "law_rule", "gbn": "out", "pageno": "1"},
            referer_path="/out/pds/goPdsMain.do",
        )

        out: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            start = (page - 1) * 20
            data = self._post(
                session,
                "/web/pds/pdsListData.do",
                csrf,
                data={
                    "start": str(start),
                    "limit": "20",
                    "pagesize": "20",
                    "Menuid": menu_id,
                    "gbn2": "out",
                },
                referer_path="/out/pds/pdsList.do",
                expect_json=True,
                is_ajax=True,
            )
            rows = data.get("result") or []
            if not rows:
                break
            print(f"[KRX:rule_change_notice] page {page}: {len(rows)} items")
            for row in rows:
                bbs_id = str(row.get("BBSID") or "").replace("T", "").strip()
                if not bbs_id:
                    continue
                detail = self._fetch_notice_detail(session, csrf, bbs_id, menu_id)
                title = row.get("TITLE")
                content_text = detail.get("content_text")
                attachment_names = " ".join([a.get("file_name") or "" for a in detail.get("attachments", [])])
                amendment_type = infer_amendment_type(title, content_text, attachment_names)
                effective_date = extract_effective_date_from_text(content_text)
                out.append(
                    {
                        "source_system": "krx_scrape",
                        "source_channel": "krx_rule_change_notice",
                        "source_item_id": bbs_id,
                        "title": title,
                        "published_at": parse_krx_date(str(row.get("MAINDT") or "")),
                        "organization": "한국거래소",
                        "department": row.get("DEPTNAME"),
                        "original_url": f"{self.base_url}/out/pds/pdsList.do",
                        "detail_url": f"{self.base_url}/out/pds/pdsView.do?BBSID={bbs_id}&Menuid={menu_id}",
                        "effective_date": effective_date,
                        "amendment_type": amendment_type,
                        "content_html": detail.get("content_html"),
                        "content_text": content_text,
                        "attachments": detail.get("attachments", []),
                        "raw": {"list_row": row, "detail_html": detail.get("raw_html")},
                    }
                )
        return out


class KofiaCollector:
    def __init__(self, base_url: str = KOFIA_BASE):
        self.base_url = base_url
        self.session = requests.Session()

    def _post(self, path: str, data: Optional[Dict[str, Any]] = None) -> str:
        resp = self.session.post(f"{self.base_url}{path}", data=data or {}, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        resp = self.session.get(f"{self.base_url}{path}", params=params or {}, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _text_by_th(soup: BeautifulSoup, th_label: str) -> Optional[str]:
        th = soup.find("th", string=lambda s: s and th_label in s)
        if not th:
            return None
        td = th.find_next("td")
        if not td:
            return None
        text = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
        return text or None

    def _parse_kofia_attachments(self, scope: BeautifulSoup, source: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for a in scope.select("a[href*='/download.do']"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            file_url = urljoin(self.base_url, href)
            if file_url in seen:
                continue
            seen.add(file_url)
            # KOFIA download anchors are often icon-only (<a><img .../></a>), so text() can be empty.
            # Fall back to title/alt attributes, then URL query tokens to preserve logical attachment identity.
            file_name = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            if not file_name:
                file_name = (a.get("title") or "").strip()
            if not file_name:
                img = a.find("img")
                if img:
                    file_name = ((img.get("title") or "") or (img.get("alt") or "")).strip()
            file_name = re.sub(r"\(.*?\)", "", file_name).strip()
            if not file_name:
                q = parse_qs(urlparse(file_url).query)
                gubun = (q.get("gubun", [None])[0] or "").strip()
                seq = (q.get("seq", [None])[0] or "").strip()
                kind_map = {
                    "101": "full_text",
                    "104": "amendment_text",
                    "105": "comparison_table",
                }
                kind = kind_map.get(gubun, f"gubun_{gubun}" if gubun else "attachment")
                file_name = f"kofia_{kind}_{seq}" if seq else f"kofia_{kind}"
            out.append(
                {
                    "file_name": file_name,
                    "file_url": file_url,
                    "file_ext": file_ext_from_name(file_name),
                    "source": source,
                }
            )
        return out

    def _fetch_notice_detail(self, revision_seq: str, list_page: int) -> Dict[str, Any]:
        try:
            html = self._post(
                "/service/revisionNotice/revisionNoticeView.do",
                data={"revisionSeq": revision_seq, "page": str(list_page)},
            )
            soup = BeautifulSoup(html, "html.parser")
            content_node = soup.select_one("td.story div.storyIn")
            content_html = str(content_node) if content_node else None
            content_text = html_to_text(content_html)
            attachments = self._parse_kofia_attachments(soup, "kofia_notice_scrape")
            return {
                "reg_name": self._text_by_th(soup, "규정명"),
                "department": self._text_by_th(soup, "담당부서"),
                "amendment_type": self._text_by_th(soup, "제ㆍ개정구분"),
                "notice_start": parse_krx_date(self._text_by_th(soup, "예고시작일")),
                "notice_end": parse_krx_date(self._text_by_th(soup, "예고종료일")),
                "content_html": content_html,
                "content_text": content_text,
                "attachments": attachments,
                "raw_html": html,
            }
        except Exception:
            return {
                "reg_name": None,
                "department": None,
                "amendment_type": None,
                "notice_start": None,
                "notice_end": None,
                "content_html": None,
                "content_text": None,
                "attachments": [],
                "raw_html": None,
            }

    def ingest_rule_change_notices(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            html = self._post("/service/revisionNotice/revisionNoticeList.do", data={"page": str(page)})
            soup = BeautifulSoup(html, "html.parser")
            table = soup.select_one("table.brdComList")
            if not table:
                break
            rows = table.select("tbody tr")
            if not rows:
                break
            print(f"[KOFIA:rule_change_notice] page {page}: {len(rows)} items")
            for row in rows:
                seq_match = re.search(r"goRevisionInfoDetail\('(\d+)'\)", str(row))
                if not seq_match:
                    continue
                revision_seq = seq_match.group(1)
                tds = row.find_all("td")
                if len(tds) < 5:
                    continue
                title = re.sub(r"\s+", " ", tds[1].get_text(" ", strip=True)).strip()
                amendment_type = re.sub(r"\s+", " ", tds[2].get_text(" ", strip=True)).strip() or None
                notice_start = parse_krx_date(tds[3].get_text(" ", strip=True))
                notice_end = parse_krx_date(tds[4].get_text(" ", strip=True))
                row_attachments = self._parse_kofia_attachments(row, "kofia_notice_scrape")
                detail = self._fetch_notice_detail(revision_seq, page)
                attachments = row_attachments[:]
                known_urls = {a.get("file_url") for a in attachments}
                for att in detail.get("attachments", []):
                    if att.get("file_url") in known_urls:
                        continue
                    attachments.append(att)
                content_text = detail.get("content_text")
                effective_date = extract_effective_date_from_text(content_text)
                out.append(
                    {
                        "source_system": "kofia_notice_scrape",
                        "source_channel": "kofia_rule_change_notice",
                        "source_item_id": revision_seq,
                        "title": detail.get("reg_name") or title,
                        "published_at": notice_start,
                        "organization": "금융투자협회",
                        "department": detail.get("department"),
                        "original_url": f"{self.base_url}/service/revisionNotice/revisionNoticeList.do",
                        "detail_url": f"{self.base_url}/service/revisionNotice/revisionNoticeView.do?revisionSeq={revision_seq}",
                        "effective_date": effective_date,
                        "amendment_type": detail.get("amendment_type") or amendment_type,
                        "content_html": detail.get("content_html"),
                        "content_text": content_text,
                        "attachments": attachments,
                        "raw": {
                            "list_page": page,
                            "list_row_html": str(row),
                            "notice_end": notice_end,
                            "detail_html": detail.get("raw_html"),
                        },
                    }
                )
        return out

    def _fetch_recent_detail(self, history_seq: str) -> Dict[str, Any]:
        try:
            html = self._get("/service/revision/revisionView.do", params={"historySeq": history_seq})
            soup = BeautifulSoup(html, "html.parser")
            content_node = soup.select_one("td.story div.storyIn")
            content_html = str(content_node) if content_node else None
            content_text = html_to_text(content_html)
            attachments = self._parse_kofia_attachments(soup, "kofia_recent_scrape")
            return {
                "reg_name": self._text_by_th(soup, "규정명"),
                "revision_date": parse_krx_date(self._text_by_th(soup, "제ㆍ개정일")),
                "amendment_type": self._text_by_th(soup, "개정구분"),
                "effective_date": parse_krx_date(self._text_by_th(soup, "시행일")),
                "content_html": content_html,
                "content_text": content_text,
                "attachments": attachments,
                "raw_html": html,
            }
        except Exception:
            return {
                "reg_name": None,
                "revision_date": None,
                "amendment_type": None,
                "effective_date": None,
                "content_html": None,
                "content_text": None,
                "attachments": [],
                "raw_html": None,
            }

    def ingest_recent_rule_changes(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            html = self._post("/service/revision/revisionlist.do", data={"page": str(page)})
            soup = BeautifulSoup(html, "html.parser")
            table = soup.select_one("table.brdComList")
            if not table:
                break
            rows = table.select("tbody tr")
            if not rows:
                break
            print(f"[KOFIA:recent_rule_change] page {page}: {len(rows)} items")
            for row in rows:
                link = row.select_one("a[href*='revisionView.do?historySeq=']")
                if not link:
                    continue
                history_seq = parse_qs(urlparse(urljoin(self.base_url, link.get("href") or "")).query).get("historySeq", [None])[0]
                if not history_seq:
                    continue
                tds = row.find_all("td")
                if len(tds) < 4:
                    continue
                title = re.sub(r"\s+", " ", link.get_text(" ", strip=True)).strip()
                amendment_type = re.sub(r"\s+", " ", tds[2].get_text(" ", strip=True)).strip() or None
                revision_date = parse_krx_date(tds[3].get_text(" ", strip=True))
                row_attachments = self._parse_kofia_attachments(row, "kofia_recent_scrape")
                detail = self._fetch_recent_detail(str(history_seq))
                attachments = row_attachments[:]
                known_urls = {a.get("file_url") for a in attachments}
                for att in detail.get("attachments", []):
                    if att.get("file_url") in known_urls:
                        continue
                    attachments.append(att)
                out.append(
                    {
                        "source_system": "kofia_recent_scrape",
                        "source_channel": "kofia_recent_rule_change",
                        "source_item_id": str(history_seq),
                        "title": detail.get("reg_name") or title,
                        "published_at": detail.get("revision_date") or revision_date,
                        "organization": "금융투자협회",
                        "department": None,
                        "original_url": f"{self.base_url}/service/revision/revisionlist.do",
                        "detail_url": f"{self.base_url}/service/revision/revisionView.do?historySeq={history_seq}",
                        "effective_date": detail.get("effective_date"),
                        "amendment_type": detail.get("amendment_type") or amendment_type,
                        "content_html": detail.get("content_html"),
                        "content_text": detail.get("content_text"),
                        "attachments": attachments,
                        "raw": {"list_page": page, "list_row_html": str(row), "detail_html": detail.get("raw_html")},
                    }
                )
        return out


@dataclass
class IngestRunOptions:
    db_path: str
    service_key: str
    start_date: str
    end_date: str
    include_api: bool = True
    include_fss: bool = True
    include_ksd: bool = True
    include_fsc: bool = True
    include_fsc_admin: bool = True
    include_fsc_reply: bool = True
    include_bok: bool = True
    include_kfb: bool = True
    include_fsec: bool = True
    include_ksd_rule: bool = True
    include_krx_recent: bool = True
    include_krx_notice: bool = True
    include_kofia_recent: bool = True
    include_kofia_notice: bool = True
    include_arirang_news: bool = False
    include_fss_admin: bool = True
    fss_max_pages: int = 1
    ksd_max_pages: int = 3
    fsc_max_pages: int = 1
    fsc_admin_max_pages: int = 1
    fsc_reply_max_pages: int = 1
    bok_max_pages: int = 1
    kfb_max_pages: int = 1
    fsec_max_pages: int = 1
    ksd_rule_max_pages: int = 1
    krx_recent_max_pages: int = 1
    krx_notice_max_pages: int = 1
    kofia_recent_max_pages: int = 1
    kofia_notice_max_pages: int = 1
    arirang_news_max_pages: int = 1
    fss_admin_max_pages: int = 1
    api_orgs: Optional[Sequence[str]] = None
    precompute_analytics: bool = True
    collector_retry_attempts: int = 2
    collector_retry_backoff_sec: float = 2.0


CollectorFetchFn = Callable[[], List[Dict[str, Any]]]


@dataclass
class CollectorRunSpec:
    label: str
    source_key: str
    enabled: bool
    fetch_fn: CollectorFetchFn


class UnifiedPressIngestService:
    def __init__(
        self,
        http: Optional[HttpClient] = None,
        api_collector: Optional[DataGoApiCollector] = None,
        fss_collector: Optional[FssCollector] = None,
        ksd_collector: Optional[KsdCollector] = None,
        fsc_collector: Optional[FscCollector] = None,
        fsc_admin_collector: Optional[FscAdminGuidanceCollector] = None,
        fsc_reply_collector: Optional[FscReplyCaseCollector] = None,
        bok_collector: Optional[BokCollector] = None,
        kfb_collector: Optional[KfbCollector] = None,
        fsec_collector: Optional[FsecCollector] = None,
        ksd_rule_collector: Optional[KsdRuleChangeNoticeCollector] = None,
        krx_collector: Optional[KrxCollector] = None,
        kofia_collector: Optional[KofiaCollector] = None,
        arirang_news_collector: Optional[ArirangNewsCollector] = None,
        fss_admin_collector: Optional[FssAdminGuidanceCollector] = None,
    ):
        self.http = http or HttpClient()
        self.api_collector = api_collector or DataGoApiCollector(self.http)
        self.fss_collector = fss_collector or FssCollector(self.http)
        self.ksd_collector = ksd_collector or KsdCollector(self.http)
        self.fsc_collector = fsc_collector or FscCollector(self.http)
        self.fsc_admin_collector = fsc_admin_collector or FscAdminGuidanceCollector(self.http)
        self.fsc_reply_collector = fsc_reply_collector or FscReplyCaseCollector(self.http)
        self.bok_collector = bok_collector or BokCollector(self.http)
        self.kfb_collector = kfb_collector or KfbCollector(self.http)
        self.fsec_collector = fsec_collector or FsecCollector(self.http)
        self.ksd_rule_collector = ksd_rule_collector or KsdRuleChangeNoticeCollector(self.http)
        self.krx_collector = krx_collector or KrxCollector()
        self.kofia_collector = kofia_collector or KofiaCollector()
        self.arirang_news_collector = arirang_news_collector or ArirangNewsCollector(self.http)
        self.fss_admin_collector = fss_admin_collector or FssAdminGuidanceCollector(self.http)

    @staticmethod
    def _persist(repo: ArticleRepository, articles: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for article in articles:
            article["organization"] = normalize_organization_by_channel(
                article.get("source_channel"),
                article.get("organization"),
            )
            article_id = repo.upsert_article(article)
            repo.upsert_attachments(article_id, article.get("attachments", []))
            count += 1
        return count

    def _build_collector_specs(self, options: IngestRunOptions) -> List[CollectorRunSpec]:
        return [
            CollectorRunSpec(
                label="DataGoAPI",
                source_key="data_go_api",
                enabled=options.include_api,
                fetch_fn=lambda: self.api_collector.ingest(
                    service_key=options.service_key,
                    start_date=options.start_date,
                    end_date=options.end_date,
                    allowed_orgs=options.api_orgs,
                ),
            ),
            CollectorRunSpec(
                label="FSS",
                source_key="fss_scrape",
                enabled=options.include_fss,
                fetch_fn=lambda: self.fss_collector.ingest(max_pages_each=options.fss_max_pages),
            ),
            CollectorRunSpec(
                label="FSS Admin",
                source_key="fss_admin_scrape",
                enabled=options.include_fss_admin,
                fetch_fn=lambda: self.fss_admin_collector.ingest(max_pages=options.fss_admin_max_pages),
            ),
            CollectorRunSpec(
                label="KSD",
                source_key="ksd_scrape",
                enabled=options.include_ksd,
                fetch_fn=lambda: self.ksd_collector.ingest(max_pages=options.ksd_max_pages),
            ),
            CollectorRunSpec(
                label="KSD Rule",
                source_key="ksd_rule_scrape",
                enabled=options.include_ksd_rule,
                fetch_fn=lambda: self.ksd_rule_collector.ingest(max_pages=options.ksd_rule_max_pages),
            ),
            CollectorRunSpec(
                label="FSC",
                source_key="fsc_scrape",
                enabled=options.include_fsc,
                fetch_fn=lambda: self.fsc_collector.ingest(max_pages_each=options.fsc_max_pages),
            ),
            CollectorRunSpec(
                label="FSC Admin",
                source_key="fsc_admin_scrape",
                enabled=options.include_fsc_admin,
                fetch_fn=lambda: self.fsc_admin_collector.ingest(max_pages=options.fsc_admin_max_pages),
            ),
            CollectorRunSpec(
                label="FSC ReplyCase",
                source_key="fsc_reply_scrape",
                enabled=options.include_fsc_reply,
                fetch_fn=lambda: self.fsc_reply_collector.ingest(max_pages=options.fsc_reply_max_pages),
            ),
            CollectorRunSpec(
                label="BOK",
                source_key="bok_scrape",
                enabled=options.include_bok,
                fetch_fn=lambda: self.bok_collector.ingest(max_pages=options.bok_max_pages),
            ),
            CollectorRunSpec(
                label="KFB",
                source_key="kfb_scrape",
                enabled=options.include_kfb,
                fetch_fn=lambda: self.kfb_collector.ingest(max_pages=options.kfb_max_pages),
            ),
            CollectorRunSpec(
                label="FSEC",
                source_key="fsec_scrape",
                enabled=options.include_fsec,
                fetch_fn=lambda: self.fsec_collector.ingest(max_pages=options.fsec_max_pages),
            ),
            CollectorRunSpec(
                label="KRX Recent Rules",
                source_key="krx_recent_scrape",
                enabled=options.include_krx_recent,
                fetch_fn=lambda: self.krx_collector.ingest_recent_rule_changes(max_pages=options.krx_recent_max_pages),
            ),
            CollectorRunSpec(
                label="KRX Rule Notice",
                source_key="krx_notice_scrape",
                enabled=options.include_krx_notice,
                fetch_fn=lambda: self.krx_collector.ingest_rule_change_notices(max_pages=options.krx_notice_max_pages),
            ),
            CollectorRunSpec(
                label="KOFIA Recent Rules",
                source_key="kofia_recent_scrape",
                enabled=options.include_kofia_recent,
                fetch_fn=lambda: self.kofia_collector.ingest_recent_rule_changes(max_pages=options.kofia_recent_max_pages),
            ),
            CollectorRunSpec(
                label="KOFIA Rule Notice",
                source_key="kofia_notice_scrape",
                enabled=options.include_kofia_notice,
                fetch_fn=lambda: self.kofia_collector.ingest_rule_change_notices(max_pages=options.kofia_notice_max_pages),
            ),
            CollectorRunSpec(
                label="Arirang News",
                source_key="arirang_news_api",
                enabled=options.include_arirang_news,
                fetch_fn=lambda: self.arirang_news_collector.ingest(
                    start_date=options.start_date,
                    end_date=options.end_date,
                    max_pages=options.arirang_news_max_pages,
                ),
            ),
        ]

    def run(self, options: IngestRunOptions) -> Dict[str, Any]:
        conn = sqlite3.connect(options.db_path)
        try:
            repo = ArticleRepository(conn)
            repo.init_schema()

            total_articles = 0
            source_counts: Dict[str, int] = {}


            collector_errors: Dict[str, str] = {}

            def run_collector(spec: CollectorRunSpec) -> None:
                nonlocal total_articles
                if not spec.enabled:
                    return

                attempts = max(1, int(options.collector_retry_attempts))
                base_wait = max(0.0, float(options.collector_retry_backoff_sec))
                last_exc: Optional[Exception] = None

                for attempt in range(1, attempts + 1):
                    try:
                        items = spec.fetch_fn()
                        source_counts[spec.source_key] = len(items)
                        total_articles += self._persist(repo, items)
                        conn.commit()
                        if attempt > 1:
                            print(f"[INFO] {spec.label} recovered on retry attempt {attempt}/{attempts}")
                        return
                    except Exception as exc:
                        last_exc = exc
                        if attempt < attempts:
                            wait_sec = base_wait * attempt
                            print(
                                f"[WARN] {spec.label} attempt {attempt}/{attempts} failed: {exc}. "
                                f"Retrying in {wait_sec:.1f}s"
                            )
                            if wait_sec > 0:
                                time.sleep(wait_sec)
                            continue

                collector_errors[spec.label] = str(last_exc) if last_exc is not None else "unknown error"
                print(f"[Error in {spec.label}] {last_exc}")

            for spec in self._build_collector_specs(options):
                run_collector(spec)

            if options.precompute_analytics:
                print("[INFO] Pre-computing analytics (keywords)...")
                try:
                    from local_dashboard import DashboardHandler
                    # Limit to 300 recent articles for realistic extraction speed while maintaining quality
                    rows = conn.execute("SELECT title, content_text FROM articles ORDER BY id DESC LIMIT 300").fetchall()
                    docs = [{"title": r[0], "content_text": r[1]} for r in rows]
                    keywords = DashboardHandler._extract_keywords(docs, top_n=12)

                    conn.execute("CREATE TABLE IF NOT EXISTS precomputed_keywords (keyword TEXT, score REAL, computed_at TEXT)")
                    conn.execute("DELETE FROM precomputed_keywords")
                    ts = now_iso()
                    for kw in keywords:
                        conn.execute(
                            "INSERT INTO precomputed_keywords (keyword, score, computed_at) VALUES (?, ?, ?)",
                            (kw["keyword"], kw["score"], ts),
                        )
                    conn.commit()
                    print("[INFO] Pre-computation complete.")
                except ImportError as e:
                    print(f"[Warning] Failed to import local_dashboard to precompute keywords: {e}")
                except Exception as e:
                    print(f"[Error in Analytics Pre-computation] {e}")
            else:
                print("[INFO] Skipping analytics pre-computation (--skip-analytics)")

            summary = repo.fetch_summary()
            latest = repo.fetch_latest(limit=20)
            return {
                "run_at": now_iso(),
                "inserted_or_updated_this_run": total_articles,
                "source_counts_this_run": source_counts,
                "collector_errors": collector_errors,
                "failed_collectors": sorted(collector_errors.keys()),
                "table_summary": [
                    {"source_system": source_system, "source_channel": source_channel, "count": count}
                    for (source_system, source_channel, count) in summary
                ],
                "latest_20": [
                    {
                        "source_system": source_system,
                        "source_channel": source_channel,
                        "source_item_id": source_item_id,
                        "title": title,
                        "published_at": published_at,
                        "url": url,
                    }
                    for (source_system, source_channel, source_item_id, title, published_at, url) in latest
                ],
            }
        finally:
            conn.close()

class UnifiedIngestCliApp:
    COLLECTOR_KEYS = [
        "api",
        "fss",
        "fss-admin",
        "ksd",
        "fsc",
        "fsc-admin",
        "fsc-reply",
        "bok",
        "kfb",
        "fsec",
        "ksd-rule",
        "krx-recent",
        "krx-notice",
        "kofia-recent",
        "kofia-notice",
        "arirang-news",
    ]
    COLLECTOR_NO_FLAG = {
        "api": "no_api",
        "fss": "no_fss",
        "fss-admin": "no_fss_admin",
        "ksd": "no_ksd",
        "fsc": "no_fsc",
        "fsc-admin": "no_fsc_admin",
        "fsc-reply": "no_fsc_reply",
        "bok": "no_bok",
        "kfb": "no_kfb",
        "fsec": "no_fsec",
        "ksd-rule": "no_ksd_rule",
        "krx-recent": "no_krx_recent",
        "krx-notice": "no_krx_notice",
        "kofia-recent": "no_kofia_recent",
        "kofia-notice": "no_kofia_notice",
        "arirang-news": "no_arirang_news",
    }

    def __init__(self, http: Optional[HttpClient] = None):
        self.http = http or HttpClient()

    def build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Unified press ingestion pipeline")
        parser.add_argument("--config", default="ingest_config.yaml", help="Path to config file")
        parser.add_argument("--service-key", required=True, help="data.go.kr service key")
        parser.add_argument("--start-date", required=True, help="YYYYMMDD")
        parser.add_argument("--end-date", required=True, help="YYYYMMDD")
        parser.add_argument("--db-path", default="press_unified.db", help="SQLite DB path")
        parser.add_argument(
            "--only-collector",
            choices=self.COLLECTOR_KEYS,
            default=None,
            help="Run only one collector key (e.g. fss, fsc, bok)",
        )
        parser.add_argument("--analytics-only", action="store_true", help="Skip collectors and run analytics pre-computation only")
        parser.add_argument("--skip-analytics", action="store_true", help="Skip analytics pre-computation at end")
        parser.add_argument("--fail-on-collector-error", action="store_true", help="Exit non-zero when any collector fails")
        parser.add_argument("--collector-retry-attempts", type=int, default=2, help="Retry attempts per collector on failure")
        parser.add_argument("--collector-retry-backoff-sec", type=float, default=2.0, help="Base backoff seconds between collector retries")

        parser.add_argument("--no-api", action="store_true", help="Disable Data.go.kr API collector")
        parser.add_argument("--no-fss", action="store_true", help="Disable FSS collector")
        parser.add_argument("--no-fss-admin", action="store_true", help="Disable FSS admin-guidance collector")
        parser.add_argument("--no-ksd", action="store_true", help="Disable KSD collector")
        parser.add_argument("--no-fsc", action="store_true", help="Disable FSC collector")
        parser.add_argument("--no-fsc-admin", action="store_true", help="Disable FSC admin-guidance collector")
        parser.add_argument("--no-fsc-reply", action="store_true", help="Disable FSC reply-case collector")
        parser.add_argument("--no-bok", action="store_true", help="Disable BOK collector")
        parser.add_argument("--no-kfb", action="store_true", help="Disable KFB collector")
        parser.add_argument("--no-fsec", action="store_true", help="Disable FSEC collector")
        parser.add_argument("--no-ksd-rule", action="store_true", help="Disable KSD rule-change collector")
        parser.add_argument("--no-krx-recent", action="store_true", help="Disable KRX recent rule-change collector")
        parser.add_argument("--no-krx-notice", action="store_true", help="Disable KRX rule-change notice collector")
        parser.add_argument("--no-kofia-recent", action="store_true", help="Disable KOFIA recent rule-change collector")
        parser.add_argument("--no-kofia-notice", action="store_true", help="Disable KOFIA rule-change notice collector")
        parser.add_argument("--no-arirang-news", action="store_true", help="Disable Arirang news collector")

        parser.add_argument("--fss-max-pages", type=int, default=1, help="Max pages per FSS board")
        parser.add_argument("--fss-admin-max-pages", type=int, default=1, help="Max pages for FSS admin-guidance")
        parser.add_argument("--ksd-max-pages", type=int, default=3, help="Max pages for KSD")
        parser.add_argument("--fsc-max-pages", type=int, default=1, help="Max pages per FSC board")
        parser.add_argument("--fsc-admin-max-pages", type=int, default=1, help="Max pages for FSC admin-guidance")
        parser.add_argument("--fsc-reply-max-pages", type=int, default=1, help="Max pages for FSC reply-case channels")
        parser.add_argument("--bok-max-pages", type=int, default=1, help="Max pages for BOK")
        parser.add_argument("--kfb-max-pages", type=int, default=1, help="Max pages for KFB")
        parser.add_argument("--fsec-max-pages", type=int, default=1, help="Max pages for FSEC")
        parser.add_argument("--ksd-rule-max-pages", type=int, default=1, help="Max pages for KSD rule-change notices")
        parser.add_argument("--krx-recent-max-pages", type=int, default=1, help="Max pages for KRX recent rule changes")
        parser.add_argument("--krx-notice-max-pages", type=int, default=1, help="Max pages for KRX rule-change notices")
        parser.add_argument("--kofia-recent-max-pages", type=int, default=1, help="Max pages for KOFIA recent rule changes")
        parser.add_argument("--kofia-notice-max-pages", type=int, default=1, help="Max pages for KOFIA rule-change notices")
        parser.add_argument("--arirang-news-max-pages", type=int, default=1, help="Max pages for Arirang news collector")

        parser.add_argument("--preview-json", default="ingest_preview.json", help="Output summary JSON path")
        parser.add_argument(
            "--api-orgs",
            nargs="+",
            default=None,
            help="Allowed organizations for API collector",
        )
        return parser

    def build_options(self, args: argparse.Namespace) -> IngestRunOptions:
        return IngestRunOptions(
            db_path=args.db_path,
            service_key=args.service_key,
            start_date=args.start_date,
            end_date=args.end_date,
            include_api=not args.no_api,
            include_fss=not args.no_fss,
            include_fss_admin=not args.no_fss_admin,
            include_ksd=not args.no_ksd,
            include_fsc=not args.no_fsc,
            include_fsc_admin=not args.no_fsc_admin,
            include_fsc_reply=not args.no_fsc_reply,
            include_bok=not args.no_bok,
            include_kfb=not args.no_kfb,
            include_fsec=not args.no_fsec,
            include_ksd_rule=not args.no_ksd_rule,
            include_krx_recent=not args.no_krx_recent,
            include_krx_notice=not args.no_krx_notice,
            include_kofia_recent=not args.no_kofia_recent,
            include_kofia_notice=not args.no_kofia_notice,
            include_arirang_news=not args.no_arirang_news,
            fss_max_pages=args.fss_max_pages,
            fss_admin_max_pages=args.fss_admin_max_pages,
            ksd_max_pages=args.ksd_max_pages,
            fsc_max_pages=args.fsc_max_pages,
            fsc_admin_max_pages=args.fsc_admin_max_pages,
            fsc_reply_max_pages=args.fsc_reply_max_pages,
            bok_max_pages=args.bok_max_pages,
            kfb_max_pages=args.kfb_max_pages,
            fsec_max_pages=args.fsec_max_pages,
            ksd_rule_max_pages=args.ksd_rule_max_pages,
            krx_recent_max_pages=args.krx_recent_max_pages,
            krx_notice_max_pages=args.krx_notice_max_pages,
            kofia_recent_max_pages=args.kofia_recent_max_pages,
            kofia_notice_max_pages=args.kofia_notice_max_pages,
            arirang_news_max_pages=args.arirang_news_max_pages,
            api_orgs=args.api_orgs,
            precompute_analytics=not args.skip_analytics,
            collector_retry_attempts=args.collector_retry_attempts,
            collector_retry_backoff_sec=args.collector_retry_backoff_sec,
        )

    def _apply_scope_options(self, args: argparse.Namespace) -> None:
        if args.analytics_only:
            for no_attr in self.COLLECTOR_NO_FLAG.values():
                setattr(args, no_attr, True)
            args.skip_analytics = False
            return

        if args.only_collector:
            for key, no_attr in self.COLLECTOR_NO_FLAG.items():
                setattr(args, no_attr, key != args.only_collector)

    def _merge_yaml_collector_config(self, args: argparse.Namespace, collectors_cfg: Dict[str, Any]) -> None:
        if not isinstance(collectors_cfg, dict):
            print("[WARN] Invalid config: collectors must be an object. Ignoring collectors section.")
            return

        for key, col_cfg in collectors_cfg.items():
            if not isinstance(col_cfg, dict):
                print(f"[WARN] Invalid collector config for '{key}': expected object, got {type(col_cfg).__name__}")
                continue

            # Map yaml max_pages -> args.xxx_max_pages if CLI left it at default (1)
            max_pages = col_cfg.get("max_pages")
            arg_attr = f"{key}_max_pages"
            if max_pages is not None and hasattr(args, arg_attr):
                if getattr(args, arg_attr) == 1:
                    setattr(args, arg_attr, max_pages)

            # Honour enabled: false in yaml (only if CLI didn't explicitly include/exclude)
            if not col_cfg.get("enabled", True):
                no_attr = f"no_{key}"
                if hasattr(args, no_attr):
                    setattr(args, no_attr, True)

            if key == "data_go_api" and "api_orgs" in col_cfg:
                if not getattr(args, "api_orgs", None):
                    args.api_orgs = col_cfg["api_orgs"]

    @staticmethod
    def _ensure_default_api_orgs(args: argparse.Namespace) -> None:
        if not getattr(args, "api_orgs", None):
            args.api_orgs = list(DEFAULT_API_ORGS)

    def run(self, args: argparse.Namespace) -> Dict[str, Any]:
        # Load YAML config and merge into args (args CLI values take precedence)
        yaml_cfg = load_ingest_config(getattr(args, "config", "ingest_config.yaml"))
        collectors_cfg = yaml_cfg.get("collectors", {})
        self._merge_yaml_collector_config(args, collectors_cfg)
        self._ensure_default_api_orgs(args)
        self._apply_scope_options(args)
        validate_dates(args.start_date, args.end_date)
        service = UnifiedPressIngestService(http=self.http)
        options = self.build_options(args)
        result = service.run(options)

        with open(args.preview_json, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        result_text = json.dumps(result, ensure_ascii=False, indent=2)
        try:
            print(result_text)
        except UnicodeEncodeError:
            out_enc = sys.stdout.encoding or "utf-8"
            safe = result_text.encode(out_enc, errors="replace").decode(out_enc, errors="replace")
            print(safe)

        print(f"saved preview: {args.preview_json}")
        print(f"db: {args.db_path}")
        if args.fail_on_collector_error and result.get("collector_errors"):
            failed = ", ".join(sorted(result["collector_errors"].keys()))
            print(f"[ERROR] Collector failures detected: {failed}")
            raise SystemExit(2)
        return result


def main() -> None:
    app = UnifiedIngestCliApp()
    parser = app.build_parser()
    args = parser.parse_args()
    app.run(args)


if __name__ == "__main__":
    main()
