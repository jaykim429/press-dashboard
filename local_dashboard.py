import argparse
import difflib
import json
import math
import re
import smtplib
import socketserver
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    from kiwipiepy import Kiwi
except Exception:
    Kiwi = None


BASE_DIR = Path(__file__).resolve().parent
LOGIN_HTML_PATH = BASE_DIR / "login.html"
HTML_PATH = BASE_DIR / "dashboard.html"
ARTICLE_HTML_PATH = BASE_DIR / "article.html"
CGIN_LOGO_PATH = BASE_DIR / "cgin_logo.png"
ADMIN_HTML_PATH = BASE_DIR / "admin.html"


class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "testprocess429@gmail.com"
SMTP_PASS = "tbphgenptykcbhsl"


def to_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class DashboardHandler(BaseHTTPRequestHandler):
    db_path = None
    login_id = "test123"
    login_pw = "test123"
    _kiwi = None
    _stats_cache = {}    # {(title_q, press_type, org, from, to): (timestamp, data)}
    CACHE_TTL = 300      # 5 minutes
    PRESS_TYPE_CHANNELS = {
        "press_release": [
            "korea_policy_briefing_press_release",
            "fss_press_release",
            "ksd_press_release",
            "bok_press_release",
        ],
        "press_explainer": ["fss_press_explainer", "fsc_press_explainer"],
        "rule_change_notice": [
            "fsc_rule_change_notice",
            "ksd_rule_change_notice",
            "krx_rule_change_notice",
            "kofia_rule_change_notice",
        ],
        "recent_rule_change_info": [
            "fsc_regulation_notice",
            "krx_recent_rule_change",
            "kofia_recent_rule_change",
        ],
        "admin_guidance_notice": ["fsc_admin_guidance_notice", "fss_admin_guidance_notice"],
        "admin_guidance_enforcement": ["fsc_admin_guidance_enforcement", "fss_admin_guidance_enforcement"],
        "law_interpretation": ["fsc_law_interpretation"],
        "no_action_opinion": ["fsc_no_action_opinion"],
        "other_data": ["kfb_publicdata_other", "fsec_bbs_222"],
    }

    def _json_response(self, payload, status=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _text_response(self, text, status=200, content_type="text/plain; charset=utf-8"):
        data = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str, status=302):
        self.send_response(status)
        self.send_header("Location", location)
        self.end_headers()

    def _parse_cookies(self):
        raw = self.headers.get("Cookie", "")
        out = {}
        for chunk in raw.split(";"):
            if "=" not in chunk:
                continue
            k, v = chunk.split("=", 1)
            out[k.strip()] = v.strip()
        return out

    def _is_authenticated(self):
        cookies = self._parse_cookies()
        return cookies.get("press_auth") == "1"

    def _require_auth_api(self):
        if self._is_authenticated():
            return True
        self._json_response({"error": "Unauthorized"}, status=401)
        return False

    def _db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in {"/", "/login"}:
            if self._is_authenticated():
                self._redirect("/dashboard")
                return
            if not LOGIN_HTML_PATH.exists():
                self._text_response("login.html not found", status=500)
                return
            html = LOGIN_HTML_PATH.read_text(encoding="utf-8")
            self._text_response(html, content_type="text/html; charset=utf-8")
            return

        if path == "/dashboard":
            if not self._is_authenticated():
                self._redirect("/login")
                return
            if not HTML_PATH.exists():
                self._text_response("dashboard.html not found", status=500)
                return
            html = HTML_PATH.read_text(encoding="utf-8")
            self._text_response(html, content_type="text/html; charset=utf-8")
            return

        if path == "/article":
            if not self._is_authenticated():
                self._redirect("/login")
                return
            if not ARTICLE_HTML_PATH.exists():
                self._text_response("article.html not found", status=500)
                return
            html = ARTICLE_HTML_PATH.read_text(encoding="utf-8")
            self._text_response(html, content_type="text/html; charset=utf-8")
            return

        if path == "/admin":
            if not self._is_authenticated():
                self._redirect("/login")
                return
            if not ADMIN_HTML_PATH.exists():
                self._text_response("admin.html not found", status=500)
                return
            html = ADMIN_HTML_PATH.read_text(encoding="utf-8")
            self._text_response(html, content_type="text/html; charset=utf-8")
            return

        if path == "/api/today-summary":
            if not self._require_auth_api():
                return
            self.handle_today_summary(qs)
            return

        if path == "/api/recipients":
            if not self._require_auth_api():
                return
            self.handle_get_recipients()
            return

        if path == "/cgin_logo.png":
            if not self._is_authenticated():
                self._redirect("/login")
                return
            if not CGIN_LOGO_PATH.exists():
                self._text_response("cgin_logo.png not found", status=404)
                return
            data = CGIN_LOGO_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path == "/api/filters":
            if not self._require_auth_api():
                return
            self.handle_filters()
            return

        if path == "/api/articles":
            if not self._require_auth_api():
                return
            self.handle_articles(qs)
            return

        if path == "/api/attachments":
            if not self._require_auth_api():
                return
            self.handle_attachments(qs)
            return

        if path == "/api/article":
            if not self._require_auth_api():
                return
            self.handle_article(qs)
            return
        if path == "/api/article-report":
            if not self._require_auth_api():
                return
            self.handle_article_report(qs)
            return

        if path == "/api/similar":
            if not self._require_auth_api():
                return
            self.handle_similar(qs)
            return

        if path == "/api/suggest":
            if not self._require_auth_api():
                return
            self.handle_suggest(qs)
            return
        if path == "/api/stats":
            if not self._require_auth_api():
                return
            self.handle_stats(qs)
            return
        if path == "/api/type-report":
            if not self._require_auth_api():
                return
        if path == "/api/notifications":
            if not self._require_auth_api():
                return
            self.handle_notifications(qs)
            return

        if path == "/api/settings":
            if not self._require_auth_api():
                return
            self.handle_get_settings()
            return

        # ── 키워드 분석 API ────────────────────────────
        if path == "/api/kw/extract":
            if not self._require_auth_api():
                return
            self.handle_kw_extract(qs)
            return

        if path == "/api/kw/cooccurrence":
            if not self._require_auth_api():
                return
            self.handle_kw_cooccurrence(qs)
            return

        if path == "/api/kw/trend":
            if not self._require_auth_api():
                return
            self.handle_kw_trend(qs)
            return

        if path == "/api/kw/stopwords":
            if not self._require_auth_api():
                return
            self.handle_kw_stopwords_get()
            return

        if path == "/api/kw/synonyms":
            if not self._require_auth_api():
                return
            self.handle_kw_synonyms_get()
            return
        # ──────────────────────────────────────────────

        if qs:
            self.handle_notifications(qs)
            return

        self._json_response({"error": "Not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/settings":
            if not self._require_auth_api():
                return
            self.handle_post_settings()
            return

        if path == "/api/login":
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception:
                self._json_response({"error": "Invalid JSON"}, status=400)
                return

            user_id = (payload.get("id") or "").strip()
            user_pw = (payload.get("password") or "").strip()
            if user_id == self.login_id and user_pw == self.login_pw:
                data = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Set-Cookie", "press_auth=1; Path=/; HttpOnly; SameSite=Lax")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return

            self._json_response({"error": "?꾩씠???먮뒗 鍮꾨?踰덊샇媛 ?щ컮瑜댁? ?딆뒿?덈떎."}, status=401)
            return

        if path == "/api/recipients":
            if not self._require_auth_api():
                return
            self.handle_add_recipient()
            return

        if path == "/api/recipients/delete":
            if not self._require_auth_api():
                return
            self.handle_delete_recipient()
            return

        if path == "/api/send-email":
            if not self._require_auth_api():
                return
            self.handle_send_email()
            return

        # ── 키워드 사전 POST API ───────────────────────
        if path == "/api/kw/stopwords":
            if not self._require_auth_api():
                return
            self.handle_kw_stopwords_post()
            return

        if path == "/api/kw/stopwords/delete":
            if not self._require_auth_api():
                return
            self.handle_kw_stopwords_delete()
            return

        if path == "/api/kw/synonyms":
            if not self._require_auth_api():
                return
            self.handle_kw_synonyms_post()
            return

        if path == "/api/kw/synonyms/delete":
            if not self._require_auth_api():
                return
            self.handle_kw_synonyms_delete()
            return
        # ──────────────────────────────────────────────

        if path == "/api/logout":
            data = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", "press_auth=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self._json_response({"error": "Not found"}, status=404)

    def handle_filters(self):
        conn = self._db()
        try:
            payload = {
                "press_types": [
                    {"value": "press_release", "label": "\ubcf4\ub3c4\uc790\ub8cc"},
                    {"value": "press_explainer", "label": "\ubcf4\ub3c4\uc124\uba85\uc790\ub8cc"},
                    {"value": "rule_change_notice", "label": "\uaddc\uc815 \uc81c\uac1c\uc815 \uc608\uace0"},
                    {"value": "recent_rule_change_info", "label": "\ucd5c\uc2e0 \uc81c\u00b7\uac1c\uc815 \uc815\ubcf4"},
                    {"value": "admin_guidance_notice", "label": "\ud589\uc815\uc9c0\ub3c4 \uc608\uace0"},
                    {"value": "admin_guidance_enforcement", "label": "\ud589\uc815\uc9c0\ub3c4 \uc2dc\ud589"},
                    {"value": "law_interpretation", "label": "\ubc95\ub839\ud574\uc11d"},
                    {"value": "no_action_opinion", "label": "\ube44\uc870\uce58\uc758\uacac\uc11c"},
                    {"value": "other_data", "label": "\uae30\ud0c0\uc790\ub8cc"},
                ],
                "organizations": [r[0] for r in conn.execute("SELECT DISTINCT organization FROM articles ORDER BY 1").fetchall() if r[0]],
            }
            self._json_response(payload)
        finally:
            conn.close()

    def handle_articles(self, qs):
        page = max(1, to_int(qs.get("page", ["1"])[0], 1))
        page_size = min(100, max(1, to_int(qs.get("page_size", ["20"])[0], 20)))
        offset = (page - 1) * page_size

        title_q = (qs.get("q", [""])[0] or "").strip()
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        organization = (qs.get("organization", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()
        sort_order = (qs.get("sort_order", ["desc"])[0] or "desc").strip().lower()

        from_expr, where_sql, params = self._build_query_parts(
            title_q=title_q,
            press_type=press_type,
            organization=organization,
            from_date=from_date,
            to_date=to_date,
        )

        conn = self._db()
        try:
            total = conn.execute(f"SELECT COUNT(*) {from_expr} {where_sql}", params).fetchone()[0]

            order_dir = "ASC" if sort_order == "asc" else "DESC"
            # Sort by date strictly since BM25 breaks with the IN(...) subquery and LIKE fallback
            order_clause = f"ORDER BY a.published_at {order_dir}, a.id {order_dir}"

            data_sql = f"""
                SELECT
                    a.id, a.source_system, a.source_channel, a.source_item_id,
                    a.title, a.published_at, a.organization, a.department,
                    a.original_url, a.detail_url,
                    (SELECT COUNT(*) FROM attachments at WHERE at.article_id = a.id) AS attachment_count
                {from_expr}
                {where_sql}
                {order_clause}
                LIMIT ? OFFSET ?
            """
            rows = conn.execute(data_sql, params + [page_size, offset]).fetchall()
            self._json_response({"page": page, "page_size": page_size, "total": total, "items": [dict(r) for r in rows]})
        finally:
            conn.close()

    @staticmethod
    def _type_label(type_key: str) -> str:
        return {
            "press_release": "\ubcf4\ub3c4\uc790\ub8cc",
            "press_explainer": "\ubcf4\ub3c4\uc124\uba85\uc790\ub8cc",
            "rule_change_notice": "\uaddc\uc815 \uc81c\uac1c\uc815 \uc608\uace0",
            "recent_rule_change_info": "\ucd5c\uc2e0 \uc81c\u00b7\uac1c\uc815 \uc815\ubcf4",
            "admin_guidance_notice": "\ud589\uc815\uc9c0\ub3c4 \uc608\uace0",
            "admin_guidance_enforcement": "\ud589\uc815\uc9c0\ub3c4 \uc2dc\ud589",
            "law_interpretation": "\ubc95\ub839\ud574\uc11d",
            "no_action_opinion": "\ube44\uc870\uce58\uc758\uacac\uc11c",
            "other_data": "\uae30\ud0c0\uc790\ub8cc",
        }.get(type_key, type_key)

    @staticmethod
    def _simple_tokenize(text: str):
        """Lowercase token split for BM25."""
        return re.findall(r"[\uac00-\ud7a3a-zA-Z0-9]+", (text or "").lower())


    def _build_query_parts(self, title_q="", press_type="", organization="", from_date="", to_date=""):
        where = []
        params = []
        join_sql = ""

        if title_q:
            clean_q = re.sub(r'[^\w\s]', ' ', title_q).strip()
            if clean_q:
                words = clean_q.split()
                if len(words) > 1:
                    joined_word = "".join(words)
                    and_words = " AND ".join(words)
                    match_query = f'"{clean_q}" OR "{joined_word}" OR ({and_words})'
                else:
                    joined_word = clean_q
                    match_query = f'"{clean_q}"*'
                    
                where.append("(a.id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?) OR replace(a.title, ' ', '') LIKE ?)")
                params.extend([match_query, f"%{joined_word}%"])
                
        channels = self.PRESS_TYPE_CHANNELS.get(press_type, [])
        if channels:
            placeholders = ", ".join(["?"] * len(channels))
            where.append(f"a.source_channel IN ({placeholders})")
            params.extend(channels)
        if organization:
            where.append("a.organization = ?")
            params.append(organization)
        normalized_date_expr = "date(substr(a.published_at, 1, 10))"
        if from_date and to_date:
            where.append(f"{normalized_date_expr} BETWEEN date(?) AND date(?)")
            params.extend([from_date, to_date])
        elif from_date:
            where.append(f"{normalized_date_expr} = date(?)")
            params.append(from_date)
        elif to_date:
            where.append(f"{normalized_date_expr} <= date(?)")
            params.append(to_date)

        from_expr = "FROM articles a " + join_sql
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        return from_expr, where_sql, params

    def handle_type_report(self, qs):
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()

        if not press_type:
            self._json_response({"item": None})
            return

        channels = self.PRESS_TYPE_CHANNELS.get(press_type, [])
        if not channels:
            self._json_response({"item": None})
            return

        conn = self._db()
        try:
            channel_placeholders = ", ".join(["?"] * len(channels))
            date_sql = ""
            params = list(channels)
            if from_date:
                date_sql += " AND date(substr(a.published_at, 1, 10)) >= date(?)"
                params.append(from_date)
            if to_date:
                date_sql += " AND date(substr(a.published_at, 1, 10)) <= date(?)"
                params.append(to_date)

            row = conn.execute(
                f"""
                SELECT
                    ro.id,
                    ro.title,
                    ro.summary_text,
                    ro.report_markdown,
                    ro.llm_status,
                    ro.llm_completed_at,
                    ro.created_at
                FROM report_outputs ro
                WHERE COALESCE(ro.llm_status, 'pending') = 'completed'
                  AND EXISTS (
                    SELECT 1
                    FROM report_output_sources rs
                    JOIN articles a ON a.id = rs.article_id
                    WHERE rs.report_output_id = ro.id
                      AND a.source_channel IN ({channel_placeholders})
                      {date_sql}
                  )
                ORDER BY COALESCE(ro.llm_completed_at, ro.created_at) DESC, ro.id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
            self._json_response({"item": dict(row) if row else None})
        except sqlite3.OperationalError:
            self._json_response({"item": None})
        finally:
            conn.close()

    def handle_stats(self, qs):
        now = time.time()
        title_q = (qs.get("q", [""])[0] or "").strip()
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        organization = (qs.get("organization", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()
        top_n = min(20, max(3, to_int(qs.get("top_n", ["8"])[0], 8)))

        from_expr, where_sql, params = self._build_query_parts(
            title_q=title_q,
            press_type=press_type,
            organization=organization,
            from_date=from_date,
            to_date=to_date,
        )

        type_case = """
            CASE
                WHEN a.source_channel IN ('fsc_admin_guidance_notice', 'fss_admin_guidance_notice') THEN 'admin_guidance_notice'
                WHEN a.source_channel IN ('fsc_admin_guidance_enforcement', 'fss_admin_guidance_enforcement') THEN 'admin_guidance_enforcement'
                WHEN a.source_channel = 'fsc_law_interpretation' THEN 'law_interpretation'
                WHEN a.source_channel = 'fsc_no_action_opinion' THEN 'no_action_opinion'
                WHEN a.source_channel IN ('fss_press_explainer', 'fsc_press_explainer') THEN 'press_explainer'
                WHEN a.source_channel IN ('fsc_rule_change_notice', 'ksd_rule_change_notice', 'krx_rule_change_notice', 'kofia_rule_change_notice') THEN 'rule_change_notice'
                WHEN a.source_channel IN ('fsc_regulation_notice', 'krx_recent_rule_change', 'kofia_recent_rule_change') THEN 'recent_rule_change_info'
                WHEN a.source_channel IN ('kfb_publicdata_other', 'fsec_bbs_222') THEN 'other_data'
                ELSE 'press_release'
            END
        """

        conn = self._db()
        try:
            total = conn.execute(f"SELECT COUNT(*) {from_expr} {where_sql}", params).fetchone()[0]

            org_rows = conn.execute(
                f"""
                SELECT COALESCE(a.organization, '(미상)') AS organization, COUNT(*) AS cnt
                {from_expr}
                {where_sql}
                GROUP BY COALESCE(a.organization, '(미상)')
                ORDER BY cnt DESC, organization ASC
                LIMIT ?
                """,
                params + [top_n],
            ).fetchall()

            type_rows = conn.execute(
                f"""
                SELECT {type_case} AS press_type, COUNT(*) AS cnt
                {from_expr}
                {where_sql}
                GROUP BY {type_case}
                ORDER BY
                  CASE press_type
                    WHEN 'press_release' THEN 1
                    WHEN 'press_explainer' THEN 2
                    WHEN 'rule_change_notice' THEN 3
                    WHEN 'recent_rule_change_info' THEN 4
                    WHEN 'admin_guidance_notice' THEN 5
                    WHEN 'admin_guidance_enforcement' THEN 6
                    WHEN 'law_interpretation' THEN 7
                    WHEN 'no_action_opinion' THEN 8
                    WHEN 'other_data' THEN 9
                    ELSE 99
                  END,
                  cnt DESC,
                  press_type ASC
                """,
                params,
            ).fetchall()

            payload = {
                "total": total,
                "by_organization": [{"name": r["organization"], "count": r["cnt"]} for r in org_rows],
                "by_type": [
                    {
                        "key": r["press_type"],
                        "label": self._type_label(r["press_type"]),
                        "count": r["cnt"],
                    }
                    for r in type_rows
                ],
            }
            # Cache key definition was missing as well.
            cache_key = f"stats_{title_q}_{press_type}_{organization}_{from_date}_{to_date}_{top_n}"
            self._stats_cache[cache_key] = (now, payload)
            self._json_response(payload)
        finally:
            conn.close()

    def handle_attachments(self, qs):
        article_id = to_int(qs.get("article_id", ["0"])[0], 0)
        if article_id <= 0:
            self._json_response({"error": "article_id is required"}, status=400)
            return

        conn = self._db()
        try:
            rows = conn.execute(
                """
                SELECT id, article_id, file_name, file_url, file_ext
                FROM attachments
                WHERE article_id = ?
                ORDER BY id ASC
                """,
                (article_id,),
            ).fetchall()
            self._json_response({"article_id": article_id, "items": [dict(r) for r in rows]})
        finally:
            conn.close()

    def handle_article(self, qs):
        article_id = to_int(qs.get("id", ["0"])[0], 0)
        if article_id <= 0:
            self._json_response({"error": "id is required"}, status=400)
            return

        conn = self._db()
        try:
            row = conn.execute(
                """
                SELECT
                    id, source_system, source_channel, source_item_id, title, published_at,
                    organization, department, original_url, detail_url,
                    effective_date, amendment_type, content_html, content_text, raw_json
                FROM articles
                WHERE id = ?
                """,
                (article_id,),
            ).fetchone()
            if not row:
                self._json_response({"error": "article not found"}, status=404)
                return

            atts = conn.execute(
                """
                SELECT id, file_name, file_url, file_ext
                FROM attachments
                WHERE article_id = ?
                ORDER BY id ASC
                """,
                (article_id,),
            ).fetchall()

            payload = dict(row)
            raw = {}
            try:
                raw = json.loads(payload.get("raw_json") or "{}")
            except Exception:
                raw = {}
            payload["notice_start"] = raw.get("notice_start")
            payload["notice_end"] = raw.get("notice_end")
            payload["final_extension_date"] = raw.get("final_extension_date")
            payload["validity_period"] = raw.get("validity_period")
            payload["enforcement_status"] = raw.get("enforcement_status")
            payload["processing_status"] = raw.get("processing_status")
            payload["public_yn"] = raw.get("public_yn")
            payload["registrant"] = raw.get("registrant")
            payload["reply_date"] = raw.get("reply_date")
            payload["query_summary"] = raw.get("query_summary")
            payload["reply_text"] = raw.get("reply_text")
            payload["reason_text"] = raw.get("reason_text")
            payload["case_number"] = raw.get("case_number")
            payload.pop("raw_json", None)
            
            # Collapse excessive newlines for better readability
            if payload.get("content_text"):
                payload["content_text"] = re.sub(r'\n(?:\s*\n){2,}', '\n\n', payload["content_text"])
                
            payload["attachments"] = [dict(a) for a in atts]
            self._json_response(payload)
        finally:
            conn.close()

    def handle_article_report(self, qs):
        article_id = to_int(qs.get("id", ["0"])[0], 0)
        if article_id <= 0:
            self._json_response({"error": "id is required"}, status=400)
            return

        conn = self._db()
        try:
            row = conn.execute(
                """
                SELECT
                    ro.id,
                    ro.title,
                    ro.summary_text,
                    ro.report_markdown,
                    ro.llm_status,
                    ro.llm_provider,
                    ro.llm_model,
                    ro.llm_completed_at,
                    ro.created_at
                FROM report_outputs ro
                JOIN report_output_sources rs ON rs.report_output_id = ro.id
                WHERE rs.article_id = ?
                  AND COALESCE(ro.llm_status, 'pending') = 'completed'
                ORDER BY COALESCE(ro.llm_completed_at, ro.created_at) DESC, ro.id DESC
                LIMIT 1
                """,
                (article_id,),
            ).fetchone()

            if not row:
                self._json_response({"item": None})
                return

            source_count = conn.execute(
                "SELECT COUNT(*) FROM report_output_sources WHERE report_output_id = ?",
                (row["id"],),
            ).fetchone()[0]

            payload = dict(row)
            payload["source_count"] = int(source_count or 0)
            self._json_response({"item": payload})
        except sqlite3.OperationalError:
            self._json_response({"item": None})
        finally:
            conn.close()

    def handle_similar(self, qs):
        article_id = to_int(qs.get("id", ["0"])[0], 0)
        limit = max(3, min(20, to_int((qs.get("limit", ["8"])[0] or "8"), 8)))
        if article_id <= 0:
            self._json_response({"error": "id is required"}, status=400)
            return

        conn = self._db()
        try:
            target = conn.execute(
                "SELECT id, title, source_channel, organization FROM articles WHERE id = ?",
                (article_id,),
            ).fetchone()
            if not target:
                self._json_response({"error": "article not found"}, status=404)
                return

            target_title = target["title"] or ""
            target_tokens = self._title_tokens(target_title)
            
            if not target_tokens:
                self._json_response({"id": article_id, "items": []})
                return
            
            # Construct FTS MATCH OR query
            match_query = " OR ".join([f'"{tok}"' for tok in set(target_tokens)])
            
            rows = conn.execute(
                """
                SELECT a.id, a.title, a.published_at, a.organization, a.source_channel, bm25(f.articles_fts) as rank
                FROM articles a
                JOIN articles_fts f ON a.id = f.rowid
                WHERE f.articles_fts MATCH ?
                  AND a.id <> ?
                  AND (a.organization = ? OR a.source_channel = ?)
                ORDER BY rank
                LIMIT ?
                """,
                (match_query, article_id, target["organization"], target["source_channel"], limit),
            ).fetchall()

            items = []
            if rows:
                for row in rows:
                    items.append(
                        {
                            "id": row["id"],
                            "title": row["title"],
                            "published_at": row["published_at"],
                            "organization": row["organization"],
                            "source_channel": row["source_channel"],
                        }
                    )

            self._json_response({
                "id": article_id,
                "items": items
            })
        finally:
            conn.close()

    @staticmethod
    def _title_tokens(text):
        if not text:
            return set()
        tokens = re.findall(r"[\uac00-\ud7a3A-Za-z0-9]{2,}", text)
        stop = {
            "\ubcf4\ub3c4\uc790\ub8cc",
            "\ubcf4\ub3c4",
            "\uc790\ub8cc",
            "\uad00\ub828",
            "\uc548\ub0b4",
            "\uacf5\uc9c0",
            "\ubc0f",
            "\ub4f1",
        }
        out = set()
        for tok in tokens:
            if tok in stop:
                continue
            out.add(tok.lower())
        return out

    @staticmethod
    def _title_similarity(a_title, a_tokens, b_title, b_tokens, apply_threshold=True):
        if not a_title or not b_title:
            return 0.0

        # token overlap (title semantics)
        union = a_tokens | b_tokens
        token_score = (len(a_tokens & b_tokens) / len(union)) if union else 0.0

        # surface similarity (title string shape)
        seq_score = difflib.SequenceMatcher(None, a_title, b_title).ratio()

        # title-first weighted score
        score = token_score * 0.65 + seq_score * 0.35
        if not apply_threshold:
            return score
        return score if score >= 0.16 else 0.0

    def handle_notifications(self, qs):
        """Return today's new articles grouped by organization and type."""
        since_date = (qs.get("since", [""])[0] or "").strip()
        conn = self._db()
        try:
            date_expr = "date(a.published_at)"
            if since_date:
                where = f"{date_expr} > date(?)"
                params = [since_date]
            else:
                # Use fixed KST date baseline to avoid server timezone drift.
                where = f"{date_expr} = date('now', '+9 hours')"
                params = []

            type_case = """
                CASE
                    WHEN source_channel IN ('fss_press_explainer','fsc_press_explainer') THEN '보도설명자료'
                    WHEN source_channel IN ('fsc_admin_guidance_notice','fss_admin_guidance_notice') THEN '행정지도 예고'
                    WHEN source_channel IN ('fsc_admin_guidance_enforcement','fss_admin_guidance_enforcement') THEN '행정지도 시행'
                    WHEN source_channel = 'fsc_law_interpretation' THEN '법령해석'
                    WHEN source_channel = 'fsc_no_action_opinion' THEN '비조치의견서'
                    WHEN source_channel IN ('fsc_rule_change_notice','ksd_rule_change_notice','krx_rule_change_notice','kofia_rule_change_notice') THEN '규정 제개정 예고'
                    WHEN source_channel IN ('fsc_regulation_notice','krx_recent_rule_change','kofia_recent_rule_change') THEN '최신 제·개정 정보'
                    WHEN source_channel IN ('kfb_publicdata_other','fsec_bbs_222') THEN '기타자료'
                    ELSE '보도자료'
                END
            """

            total = conn.execute(f"SELECT COUNT(*) FROM articles a WHERE {where}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT
                    COALESCE(a.organization, '(기관 없음)') AS org,
                    {type_case} AS type_label,
                    COUNT(*) AS cnt
                FROM articles a
                WHERE {where}
                GROUP BY org, type_label
                ORDER BY cnt DESC, org ASC
                """,
                params,
            ).fetchall()

            entry_rows = conn.execute(
                f"""
                SELECT
                    a.id AS id,
                    COALESCE(a.organization, '(기관 없음)') AS org,
                    {type_case} AS type_label,
                    COALESCE(a.title, '(제목 없음)') AS title,
                    COALESCE(a.detail_url, a.original_url, '') AS url,
                    a.published_at AS published_at
                FROM articles a
                WHERE {where}
                ORDER BY COALESCE(a.published_at, '') DESC, a.id DESC
                LIMIT 200
                """,
                params,
            ).fetchall()

            from collections import defaultdict
            grouped = defaultdict(list)
            for row in rows:
                grouped[row["org"]].append({"type": row["type_label"], "count": row["cnt"]})
            result = [
                {"organization": org, "items": items}
                for org, items in sorted(grouped.items(), key=lambda x: -sum(i["count"] for i in x[1]))
            ]
            entries = [
                {
                    "id": row["id"],
                    "organization": row["org"],
                    "type": row["type_label"],
                    "title": row["title"],
                    "url": row["url"],
                    "published_at": row["published_at"],
                }
                for row in entry_rows
            ]
            self._json_response({"total": total, "groups": result, "entries": entries})
        finally:
            conn.close()

    def handle_suggest(self, qs):
        q = (qs.get("q", [""])[0] or "").strip()
        if len(q) < 2:
            self._json_response({"items": []})
            return
        clean_q = re.sub(r'[^\w\s]', ' ', q).strip()
        if not clean_q:
            self._json_response({"items": []})
            return
        words = clean_q.split()
        match_query = " AND ".join(f'"{w}"*' for w in words)
        conn = self._db()
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT a.title
                FROM articles a
                JOIN articles_fts f ON a.id = f.rowid
                WHERE f.articles_fts MATCH ?
                ORDER BY a.published_at DESC
                LIMIT 8
                """,
                (match_query,),
            ).fetchall()
            self._json_response({"items": [r["title"] for r in rows]})
        except Exception:
            self._json_response({"items": []})
        finally:
            conn.close()

    def _ensure_admin_tables(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS email_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now','+9 hours'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key_name TEXT PRIMARY KEY,
                key_value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kw_stopwords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now','+9 hours'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kw_synonyms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical TEXT NOT NULL,
                synonym TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now','+9 hours'))
            )
        """)
        conn.commit()

    def handle_get_settings(self):
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            row = conn.execute("SELECT key_value FROM settings WHERE key_name = 'email_schedule_time'").fetchone()
            schedule_time = row["key_value"] if row else ""
            self._json_response({"email_schedule_time": schedule_time})
        finally:
            conn.close()

    def handle_post_settings(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
            
        schedule_time = (payload.get("email_schedule_time") or "").strip()
        # Basic validation for HH:MM format
        if schedule_time and not re.match(r"^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", schedule_time):
            self._json_response({"error": "올바른 시간 형식이 아닙니다 (HH:MM)."}, status=400)
            return
            
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            conn.execute(
                "INSERT INTO settings (key_name, key_value) VALUES ('email_schedule_time', ?) "
                "ON CONFLICT(key_name) DO UPDATE SET key_value = ?",
                (schedule_time, schedule_time)
            )
            conn.commit()
            self._json_response({"ok": True, "email_schedule_time": schedule_time})
        finally:
            conn.close()

    def handle_today_summary(self, qs):
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            date_str = (qs.get("date", [""])[0] or "").strip()
            if date_str:
                where = "date(a.published_at) = ?"
                params = [date_str]
            else:
                where = "date(a.published_at) = date('now', '+9 hours')"
                params = []

            type_case = """
                CASE
                    WHEN source_channel IN ('fss_press_explainer','fsc_press_explainer') THEN '보도설명자료'
                    WHEN source_channel IN ('fsc_admin_guidance_notice','fss_admin_guidance_notice') THEN '행정지도 예고'
                    WHEN source_channel IN ('fsc_admin_guidance_enforcement','fss_admin_guidance_enforcement') THEN '행정지도 시행'
                    WHEN source_channel = 'fsc_law_interpretation' THEN '법령해석'
                    WHEN source_channel = 'fsc_no_action_opinion' THEN '비조치의견서'
                    WHEN source_channel IN ('fsc_rule_change_notice','ksd_rule_change_notice','krx_rule_change_notice','kofia_rule_change_notice') THEN '규정 제개정 예고'
                    WHEN source_channel IN ('fsc_regulation_notice','krx_recent_rule_change','kofia_recent_rule_change') THEN '최신 제·개정 정보'
                    WHEN source_channel IN ('kfb_publicdata_other','fsec_bbs_222') THEN '기타자료'
                    ELSE '보도자료'
                END
            """

            rows = conn.execute(
                f"""
                SELECT
                    a.id,
                    COALESCE(a.organization, '(기관 없음)') AS organization,
                    {type_case} AS type_label,
                    COALESCE(a.title, '(제목 없음)') AS title,
                    COALESCE(a.detail_url, a.original_url, '') AS url,
                    a.published_at
                FROM articles a
                WHERE {where}
                ORDER BY organization ASC, type_label ASC, COALESCE(a.published_at,'') DESC
                LIMIT 200
                """,
                params,
            ).fetchall()

            items = [
                {
                    "id": r["id"],
                    "organization": r["organization"],
                    "type": r["type_label"],
                    "title": r["title"],
                    "url": r["url"],
                    "published_at": r["published_at"],
                }
                for r in rows
            ]
            self._json_response({"total": len(items), "items": items})
        finally:
            conn.close()

    def handle_get_recipients(self):
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            rows = conn.execute(
                "SELECT id, name, email, created_at FROM email_recipients ORDER BY created_at DESC"
            ).fetchall()
            self._json_response({"recipients": [dict(r) for r in rows]})
        finally:
            conn.close()

    def handle_add_recipient(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        email = (payload.get("email") or "").strip()
        name = (payload.get("name") or "").strip()
        if not email or "@" not in email:
            self._json_response({"error": "유효한 이메일 주소를 입력하세요."}, status=400)
            return
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            conn.execute(
                "INSERT OR IGNORE INTO email_recipients (name, email) VALUES (?, ?)",
                (name, email),
            )
            conn.commit()
            self._json_response({"ok": True})
        finally:
            conn.close()

    def handle_delete_recipient(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        rid = payload.get("id")
        if not rid:
            self._json_response({"error": "id required"}, status=400)
            return
        conn = self._db()
        try:
            conn.execute("DELETE FROM email_recipients WHERE id = ?", (rid,))
            conn.commit()
            self._json_response({"ok": True})
        finally:
            conn.close()

    def handle_send_email(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return

        mode = payload.get("mode", "summary")  # "summary" | "article"
        recipient_ids = payload.get("recipient_ids", [])

        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            if recipient_ids:
                placeholders = ",".join("?" * len(recipient_ids))
                recipients = conn.execute(
                    f"SELECT name, email FROM email_recipients WHERE id IN ({placeholders})",
                    recipient_ids,
                ).fetchall()
            else:
                recipients = conn.execute(
                    "SELECT name, email FROM email_recipients"
                ).fetchall()

            if not recipients:
                self._json_response({"error": "수신자가 없습니다."}, status=400)
                return

            if mode == "article":
                article_id = payload.get("article_id")
                if not article_id:
                    self._json_response({"error": "article_id required"}, status=400)
                    return
                row = conn.execute(
                    """SELECT id, COALESCE(organization,'') AS org,
                              COALESCE(title,'(제목 없음)') AS title,
                              COALESCE(detail_url, original_url,'') AS url,
                              published_at
                       FROM articles WHERE id = ?""",
                    (article_id,),
                ).fetchone()
                if not row:
                    self._json_response({"error": "기사를 찾을 수 없습니다."}, status=404)
                    return
                subject = f"[보도자료] {row['title']}"
                html_body = f"""
<html><body style="font-family:sans-serif;color:#222;max-width:680px;margin:auto;">
<h2 style="color:#1a56db;">📋 보도자료 공유</h2>
<table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
  <tr style="background:#f1f5f9;"><th style="padding:8px 12px;text-align:left;border:1px solid #e2e8f0;">기관</th><td style="padding:8px 12px;border:1px solid #e2e8f0;">{row['org']}</td></tr>
  <tr><th style="padding:8px 12px;text-align:left;border:1px solid #e2e8f0;">제목</th><td style="padding:8px 12px;border:1px solid #e2e8f0;">{row['title']}</td></tr>
  <tr style="background:#f1f5f9;"><th style="padding:8px 12px;text-align:left;border:1px solid #e2e8f0;">발행일</th><td style="padding:8px 12px;border:1px solid #e2e8f0;">{row['published_at'] or '-'}</td></tr>
</table>
<div style="margin: 24px 0; text-align: center;">
  <a href="{row['url']}" style="display:inline-block;background:#fff;color:#1a56db;padding:10px 20px;border:1px solid #1a56db;border-radius:6px;text-decoration:none;font-weight:bold;margin-right:10px;">원문 보기</a>
  <a href="http://34.30.218.173/article?id={row['id']}" style="display:inline-block;background:#1a56db;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:bold;">씨지인사이드에서 보기 →</a>
</div>
<hr style="border:none;border-top:1px solid #e2e8f0;margin-top:32px;margin-bottom:16px;">
<div style="font-size:12px;color:#64748b;line-height:1.6;text-align:center;">
  <p style="margin:0;font-weight:bold;">Copyright © (주) 씨지인사이드</p>
  <p style="margin:0;">서울특별시 마포구 백범로 31길 21 서울창업허브 본관 714호</p>
  <p style="margin:0;">All rights reserved.</p>
</div>
</body></html>"""
            else:
                # summary mode
                date_str = payload.get("date", "")
                where = "date(a.published_at) = ?" if date_str else "date(a.published_at) = date('now', '+9 hours')"
                params2 = [date_str] if date_str else []
                type_case = """
                    CASE
                        WHEN source_channel IN ('fss_press_explainer','fsc_press_explainer') THEN '보도설명자료'
                        WHEN source_channel IN ('fsc_admin_guidance_notice','fss_admin_guidance_notice') THEN '행정지도 예고'
                        WHEN source_channel IN ('fsc_admin_guidance_enforcement','fss_admin_guidance_enforcement') THEN '행정지도 시행'
                        WHEN source_channel = 'fsc_law_interpretation' THEN '법령해석'
                        WHEN source_channel = 'fsc_no_action_opinion' THEN '비조치의견서'
                        WHEN source_channel IN ('fsc_rule_change_notice','ksd_rule_change_notice','krx_rule_change_notice','kofia_rule_change_notice') THEN '규정 제개정 예고'
                        WHEN source_channel IN ('fsc_regulation_notice','krx_recent_rule_change','kofia_recent_rule_change') THEN '최신 제·개정 정보'
                        WHEN source_channel IN ('kfb_publicdata_other','fsec_bbs_222') THEN '기타자료'
                        ELSE '보도자료'
                    END
                """
                rows2 = conn.execute(
                    f"""
                    SELECT COALESCE(a.organization,'(기관 없음)') AS org,
                           {type_case} AS type_label,
                           COALESCE(a.title,'(제목 없음)') AS title,
                           COALESCE(a.detail_url, a.original_url,'') AS url
                    FROM articles a
                    WHERE {where}
                    ORDER BY org ASC, type_label ASC
                    LIMIT 200
                    """,
                    params2,
                ).fetchall()

                from datetime import datetime, timezone, timedelta
                kst = timezone(timedelta(hours=9))
                display_date = date_str or datetime.now(kst).strftime("%Y-%m-%d")
                subject = f"[보도자료 요약] {display_date} 오늘자 금융규제 보도자료"

                rows_html = "".join(
                    f"""<tr style="{'background:#f8fafc' if i%2==0 else ''}">
                      <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['org']}</td>
                      <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['type_label']}</td>
                      <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['title']}</td>
                      <td style="padding:8px 12px;border:1px solid #e2e8f0;text-align:center;">
                        {'<a href="' + r['url'] + '" style="color:#1a56db;">원문↗</a>' if r['url'] else '-'}
                      </td>
                    </tr>"""
                    for i, r in enumerate(rows2)
                )
                html_body = f"""
<html><body style="font-family:sans-serif;color:#222;max-width:900px;margin:auto;">
<h2 style="color:#1a56db;">📋 오늘자 보도자료 요약 ({display_date})</h2>
<p style="color:#475569;">총 {len(rows2)}건</p>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#1a56db;color:#fff;">
      <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">기관</th>
      <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">유형</th>
      <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">제목</th>
      <th style="padding:10px 12px;text-align:center;border:1px solid #1e40af;width:60px;">링크</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
<div style="margin: 24px 0; text-align: center;">
  <a href="http://34.30.218.173/" style="display:inline-block;background:#1a56db;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:bold;font-size:15px;">씨지인사이드 대시보드 바로가기 →</a>
</div>
<hr style="border:none;border-top:1px solid #e2e8f0;margin-top:32px;margin-bottom:16px;">
<div style="font-size:12px;color:#64748b;line-height:1.6;text-align:center;">
  <p style="margin:0;font-weight:bold;">Copyright © (주) 씨지인사이드</p>
  <p style="margin:0;">서울특별시 마포구 백범로 31길 21 서울창업허브 본관 714호</p>
  <p style="margin:0;">All rights reserved.</p>
</div>
</body></html>"""

        finally:
            conn.close()

        # Send emails
        try:
            sent = 0
            for r in recipients:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = SMTP_USER
                msg["To"] = r["email"]
                msg.attach(MIMEText(html_body, "html", "utf-8"))
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                    server.ehlo()
                    server.starttls()
                    server.login(SMTP_USER, SMTP_PASS)
                    server.sendmail(SMTP_USER, r["email"], msg.as_string())
                sent += 1
            self._json_response({"ok": True, "sent": sent})
        except Exception as e:
            self._json_response({"error": f"이메일 발송 실패: {e}"}, status=500)


    # ──────────────────────────────────────────────────────────────────────────
    # 키워드 분석 — 사전 캐시
    # ──────────────────────────────────────────────────────────────────────────
    _dict_cache_lock = threading.Lock()
    _stopwords_cache = None          # set | None
    _synonym_map_cache = None        # {synonym: canonical} | None
    _kw_result_cache = {}            # {cache_key: (ts, data)}
    KW_CACHE_TTL = 300               # 5분

    def _invalidate_dict_cache(self):
        with self._dict_cache_lock:
            self._stopwords_cache = None
            self._synonym_map_cache = None

    def _get_stopwords_set(self, conn):
        with self._dict_cache_lock:
            if self._stopwords_cache is not None:
                return self._stopwords_cache
        rows = conn.execute("SELECT word FROM kw_stopwords").fetchall()
        sw = {r["word"] for r in rows}
        with self._dict_cache_lock:
            self._stopwords_cache = sw
        return sw

    def _get_synonym_map(self, conn):
        """Returns {synonym: canonical} mapping."""
        with self._dict_cache_lock:
            if self._synonym_map_cache is not None:
                return self._synonym_map_cache
        rows = conn.execute("SELECT canonical, synonym FROM kw_synonyms").fetchall()
        m = {r["synonym"]: r["canonical"] for r in rows}
        with self._dict_cache_lock:
            self._synonym_map_cache = m
        return m

    def _extract_keywords_from_texts(self, texts, stopwords, synonym_map, min_len=2):
        """
        texts: list of str
        Returns Counter {word: count}
        """
        counter = Counter()
        kiwi = self.__class__._kiwi
        use_kiwi = kiwi is not None

        for text in texts:
            if not text:
                continue
            if use_kiwi:
                try:
                    # kiwi.analyze() → list of (Result, score); Result.tokens → list of Token
                    result = kiwi.analyze(text, top_n=1)
                    tokens = [
                        tok.form
                        for tok in result[0][0].tokens
                        if tok.tag in ("NNG", "NNP", "SL")  # 일반명사, 고유명사, 외국어
                        and len(tok.form) >= min_len
                    ]
                except Exception:
                    tokens = re.findall(r"[\uac00-\ud7a3a-zA-Z0-9]{" + str(min_len) + r",}", text)
            else:
                tokens = re.findall(r"[\uac00-\ud7a3a-zA-Z0-9]{" + str(min_len) + r",}", text)

            for tok in tokens:
                word = synonym_map.get(tok, tok)  # 동의어 → 대표어
                if word in stopwords:
                    continue
                counter[word] += 1

        return counter

    def _kw_query_articles(self, conn, press_type, organization, from_date, to_date, target, limit=2000):
        """DB에서 텍스트 목록을 가져온다."""
        channels = self.PRESS_TYPE_CHANNELS.get(press_type, [])
        where = []
        params = []
        if channels:
            placeholders = ",".join("?" * len(channels))
            where.append(f"source_channel IN ({placeholders})")
            params.extend(channels)
        if organization:
            where.append("organization = ?")
            params.append(organization)
        if from_date:
            where.append("date(substr(published_at,1,10)) >= date(?)")
            params.append(from_date)
        if to_date:
            where.append("date(substr(published_at,1,10)) <= date(?)")
            params.append(to_date)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        if target == "title":
            col = "COALESCE(title,'')"
        elif target == "content":
            col = "COALESCE(content_text,'')"
        else:  # both
            col = "COALESCE(title,'') || ' ' || COALESCE(content_text,'')"

        rows = conn.execute(
            f"SELECT id, published_at, {col} AS text FROM articles {where_sql} ORDER BY published_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        return rows

    def handle_kw_extract(self, qs):
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        organization = (qs.get("organization", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()
        target = (qs.get("target", ["title"])[0] or "title").strip()
        top_n = min(200, max(5, to_int(qs.get("top_n", ["50"])[0], 50)))

        cache_key = f"extract_{press_type}_{organization}_{from_date}_{to_date}_{target}_{top_n}"
        now = time.time()
        cached = self._kw_result_cache.get(cache_key)
        if cached and now - cached[0] < self.KW_CACHE_TTL:
            self._json_response(cached[1])
            return

        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            stopwords = self._get_stopwords_set(conn)
            synonym_map = self._get_synonym_map(conn)
            rows = self._kw_query_articles(conn, press_type, organization, from_date, to_date, target)
        finally:
            conn.close()

        texts = [r["text"] for r in rows]
        counter = self._extract_keywords_from_texts(texts, stopwords, synonym_map)
        top = counter.most_common(top_n)
        payload = {
            "total_docs": len(rows),
            "target": target,
            "engine": "kiwi" if self.__class__._kiwi is not None else "regex",
            "keywords": [{"word": w, "count": c} for w, c in top],
        }
        self._kw_result_cache[cache_key] = (now, payload)
        self._json_response(payload)

    def handle_kw_cooccurrence(self, qs):
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        organization = (qs.get("organization", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()
        target = (qs.get("target", ["title"])[0] or "title").strip()
        top_n = min(100, max(5, to_int(qs.get("top_n", ["40"])[0], 40)))
        min_cooc = max(1, to_int(qs.get("min_cooc", ["2"])[0], 2))

        cache_key = f"cooc_{press_type}_{organization}_{from_date}_{to_date}_{target}_{top_n}_{min_cooc}"
        now = time.time()
        cached = self._kw_result_cache.get(cache_key)
        if cached and now - cached[0] < self.KW_CACHE_TTL:
            self._json_response(cached[1])
            return

        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            stopwords = self._get_stopwords_set(conn)
            synonym_map = self._get_synonym_map(conn)
            rows = self._kw_query_articles(conn, press_type, organization, from_date, to_date, target, limit=1000)
        finally:
            conn.close()

        kiwi = self.__class__._kiwi
        # 문서별 키워드 집합 수집
        doc_keywords = []
        for row in rows:
            text = row["text"] or ""
            if kiwi:
                try:
                    result = kiwi.analyze(text, top_n=1)
                    tokens = [
                        synonym_map.get(tok.form, tok.form)
                        for sent in result[0][0].tokens
                        for tok in [sent]
                        if tok.tag in ("NNG", "NNP", "SL") and len(tok.form) >= 2
                    ]
                except Exception:
                    tokens = [
                        synonym_map.get(t, t)
                        for t in re.findall(r"[\uac00-\ud7a3a-zA-Z0-9]{2,}", text)
                    ]
            else:
                tokens = [
                    synonym_map.get(t, t)
                    for t in re.findall(r"[\uac00-\ud7a3a-zA-Z0-9]{2,}", text)
                ]
            kw_set = {t for t in tokens if t not in stopwords}
            doc_keywords.append(kw_set)

        # 전체 빈도 집계 → 상위 top_n 키워드만 네트워크에 포함
        freq = Counter(w for kws in doc_keywords for w in kws)
        top_words = {w for w, _ in freq.most_common(top_n)}

        # 동시출현 간선 계산
        edge_counter = Counter()
        for kws in doc_keywords:
            kws_filtered = [w for w in kws if w in top_words]
            kws_sorted = sorted(kws_filtered)
            for i in range(len(kws_sorted)):
                for j in range(i + 1, len(kws_sorted)):
                    edge_counter[(kws_sorted[i], kws_sorted[j])] += 1

        nodes = [{"id": w, "word": w, "freq": freq[w]} for w in top_words]
        edges = [
            {"source": a, "target": b, "weight": cnt}
            for (a, b), cnt in edge_counter.items()
            if cnt >= min_cooc
        ]

        payload = {"nodes": nodes, "edges": edges, "total_docs": len(rows)}
        self._kw_result_cache[cache_key] = (now, payload)
        self._json_response(payload)

    def handle_kw_trend(self, qs):
        press_type = (qs.get("press_type", [""])[0] or "").strip()
        organization = (qs.get("organization", [""])[0] or "").strip()
        from_date = (qs.get("from_date", [""])[0] or "").strip()
        to_date = (qs.get("to_date", [""])[0] or "").strip()
        target = (qs.get("target", ["title"])[0] or "title").strip()
        top_n = min(20, max(3, to_int(qs.get("top_n", ["10"])[0], 10)))
        granularity = (qs.get("granularity", ["month"])[0] or "month").strip()  # day/week/month

        # 사용자가 직접 지정한 키워드 목록 (쉼표 구분)
        custom_kw_raw = (qs.get("keywords", [""])[0] or "").strip()
        custom_keywords = [k.strip() for k in custom_kw_raw.split(",") if k.strip()] if custom_kw_raw else []

        cache_key = f"trend_{press_type}_{organization}_{from_date}_{to_date}_{target}_{top_n}_{granularity}_{custom_kw_raw}"
        now = time.time()
        cached = self._kw_result_cache.get(cache_key)
        if cached and now - cached[0] < self.KW_CACHE_TTL:
            self._json_response(cached[1])
            return

        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            stopwords = self._get_stopwords_set(conn)
            synonym_map = self._get_synonym_map(conn)
            rows = self._kw_query_articles(conn, press_type, organization, from_date, to_date, target, limit=3000)
        finally:
            conn.close()

        kiwi = self.__class__._kiwi

        def get_period_label(pub_at):
            if not pub_at:
                return "미상"
            d = str(pub_at)[:10]  # YYYY-MM-DD
            if granularity == "day":
                return d
            elif granularity == "week":
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    # ISO 주 시작일(월요일)
                    week_start = dt - timedelta(days=dt.weekday())
                    return week_start.strftime("%Y-%m-%d")
                except Exception:
                    return d[:7]
            else:  # month
                return d[:7]  # YYYY-MM

        # 기간별 문서 분류
        period_docs = defaultdict(list)  # {period: [text, ...]}
        for row in rows:
            label = get_period_label(row["published_at"])
            period_docs[label].append(row["text"] or "")

        # 전체 텍스트로 상위 키워드 결정
        all_texts = [row["text"] or "" for row in rows]
        global_counter = self._extract_keywords_from_texts(all_texts, stopwords, synonym_map)
        top_keywords = [w for w, _ in global_counter.most_common(top_n)]

        # 기간별 빈도 계산
        periods = sorted(period_docs.keys())
        series = []
        for kw in top_keywords:
            counts = []
            for p in periods:
                texts = period_docs[p]
                c = self._extract_keywords_from_texts(texts, stopwords, synonym_map)
                counts.append(c.get(kw, 0))
            series.append({"keyword": kw, "counts": counts})

        payload = {
            "periods": periods,
            "granularity": granularity,
            "top_keywords": top_keywords,
            "series": series,
            "total_docs": len(rows),
        }
        self._kw_result_cache[cache_key] = (now, payload)
        self._json_response(payload)

    # ── 불용어 사전 CRUD ───────────────────────────────────────────────────────
    def handle_kw_stopwords_get(self):
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            rows = conn.execute("SELECT id, word, created_at FROM kw_stopwords ORDER BY word ASC").fetchall()
            self._json_response({"items": [dict(r) for r in rows]})
        finally:
            conn.close()

    def handle_kw_stopwords_post(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        word = (payload.get("word") or "").strip()
        if not word:
            self._json_response({"error": "word 필드가 필요합니다."}, status=400)
            return
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            conn.execute("INSERT OR IGNORE INTO kw_stopwords (word) VALUES (?)", (word,))
            conn.commit()
            self._invalidate_dict_cache()
            self._json_response({"ok": True})
        finally:
            conn.close()

    def handle_kw_stopwords_delete(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        sid = payload.get("id")
        if not sid:
            self._json_response({"error": "id 필드가 필요합니다."}, status=400)
            return
        conn = self._db()
        try:
            conn.execute("DELETE FROM kw_stopwords WHERE id = ?", (sid,))
            conn.commit()
            self._invalidate_dict_cache()
            self._json_response({"ok": True})
        finally:
            conn.close()

    # ── 동의어 사전 CRUD ───────────────────────────────────────────────────────
    def handle_kw_synonyms_get(self):
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            rows = conn.execute(
                "SELECT id, canonical, synonym, created_at FROM kw_synonyms ORDER BY canonical ASC, synonym ASC"
            ).fetchall()
            # 대표어별로 그룹핑
            groups = defaultdict(list)
            for r in rows:
                groups[r["canonical"]].append({"id": r["id"], "synonym": r["synonym"]})
            result = [
                {"canonical": canonical, "synonyms": syns}
                for canonical, syns in sorted(groups.items())
            ]
            self._json_response({"groups": result})
        finally:
            conn.close()

    def handle_kw_synonyms_post(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        canonical = (payload.get("canonical") or "").strip()
        synonym = (payload.get("synonym") or "").strip()
        if not canonical or not synonym:
            self._json_response({"error": "canonical과 synonym 필드가 필요합니다."}, status=400)
            return
        if canonical == synonym:
            self._json_response({"error": "대표어와 동의어가 같습니다."}, status=400)
            return
        conn = self._db()
        try:
            self._ensure_admin_tables(conn)
            conn.execute(
                "INSERT OR REPLACE INTO kw_synonyms (canonical, synonym) VALUES (?, ?)",
                (canonical, synonym),
            )
            conn.commit()
            self._invalidate_dict_cache()
            self._json_response({"ok": True})
        finally:
            conn.close()

    def handle_kw_synonyms_delete(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self._json_response({"error": "Invalid JSON"}, status=400)
            return
        sid = payload.get("id")
        if not sid:
            self._json_response({"error": "id 필드가 필요합니다."}, status=400)
            return
        conn = self._db()
        try:
            conn.execute("DELETE FROM kw_synonyms WHERE id = ?", (sid,))
            conn.commit()
            self._invalidate_dict_cache()
            self._json_response({"ok": True})
        finally:
            conn.close()
    # ──────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="SQLite press dashboard")
    parser.add_argument("--db-path", default="press_unified.db", help="SQLite DB path")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    DashboardHandler.db_path = str(db_path)
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    
    # Start background email scheduler thread
    def run_email_scheduler():
        import time
        from datetime import datetime, timezone, timedelta
        import traceback
        
        kst = timezone(timedelta(hours=9))
        
        while True:
            try:
                # Wait until the start of the next minute
                now_kst = datetime.now(kst)
                sleep_seconds = 60 - now_kst.second
                time.sleep(sleep_seconds)
                
                now_kst = datetime.now(kst)
                current_time_str = now_kst.strftime("%H:%M")
                today_str = now_kst.strftime("%Y-%m-%d")
                
                # Check schedule directly from DB
                conn = sqlite3.connect(DashboardHandler.db_path)
                conn.row_factory = sqlite3.Row
                try:
                    # Make sure tables exist first
                    conn.execute("CREATE TABLE IF NOT EXISTS settings (key_name TEXT PRIMARY KEY, key_value TEXT NOT NULL)")
                    
                    row = conn.execute("SELECT key_value FROM settings WHERE key_name = 'email_schedule_time'").fetchone()
                    schedule_time = row["key_value"] if row else ""
                    
                    if not schedule_time:
                        continue # No schedule set
                        
                    if current_time_str == schedule_time:
                        sent_row = conn.execute("SELECT key_value FROM settings WHERE key_name = 'last_email_sent_date'").fetchone()
                        last_sent = sent_row["key_value"] if sent_row else ""
                        
                        if last_sent != today_str:
                            print(f"[Scheduler] Time matched ({schedule_time}). Sending automated summary emails for {today_str}...")
                            
                            # Fetch recipients
                            recipients = conn.execute("SELECT id, name, email FROM email_recipients ORDER BY created_at DESC").fetchall()
                            if not recipients:
                                print("[Scheduler] No recipients found. Skipping.")
                                continue
                                
                            # Fetch today's summary
                            # (Re-using logic similar to handle_send_email / handle_today_summary but raw DB execution)
                            type_case = """
                                CASE
                                    WHEN source_channel IN ('fss_press_explainer','fsc_press_explainer') THEN '보도설명자료'
                                    WHEN source_channel IN ('fsc_admin_guidance_notice','fss_admin_guidance_notice') THEN '행정지도 예고'
                                    WHEN source_channel IN ('fsc_admin_guidance_enforcement','fss_admin_guidance_enforcement') THEN '행정지도 시행'
                                    WHEN source_channel = 'fsc_law_interpretation' THEN '법령해석'
                                    WHEN source_channel = 'fsc_no_action_opinion' THEN '비조치의견서'
                                    WHEN source_channel IN ('fsc_rule_change_notice','ksd_rule_change_notice','krx_rule_change_notice','kofia_rule_change_notice') THEN '규정 제개정 예고'
                                    WHEN source_channel IN ('fsc_regulation_notice','krx_recent_rule_change','kofia_recent_rule_change') THEN '최신 제·개정 정보'
                                    WHEN source_channel IN ('kfb_publicdata_other','fsec_bbs_222') THEN '기타자료'
                                    ELSE '보도자료'
                                END
                            """
                            articles = conn.execute(
                                f"""
                                SELECT COALESCE(a.organization,'(기관 없음)') AS org,
                                       {type_case} AS type_label,
                                       COALESCE(a.title,'(제목 없음)') AS title,
                                       COALESCE(a.detail_url, a.original_url,'') AS url
                                FROM articles a
                                WHERE date(a.published_at) = date('now', '+9 hours')
                                ORDER BY org ASC, type_label ASC
                                LIMIT 200
                                """
                            ).fetchall()
                            
                            subject = f"[보도자료 요약] {today_str} 오늘자 금융규제 보도자료 자동발송"
                            
                            rows_html = "".join(
                                f'''<tr style="{'background:#f8fafc' if i%2==0 else ''}">
                                  <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['org']}</td>
                                  <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['type_label']}</td>
                                  <td style="padding:8px 12px;border:1px solid #e2e8f0;">{r['title']}</td>
                                  <td style="padding:8px 12px;border:1px solid #e2e8f0;text-align:center;">
                                    {'<a href="' + r['url'] + '" style="color:#1a56db;">원문↗</a>' if r['url'] else '-'}
                                  </td>
                                </tr>'''
                                for i, r in enumerate(articles)
                            )
                            html_body = f"""
                            <html><body style="font-family:sans-serif;color:#222;max-width:900px;margin:auto;">
                            <h2 style="color:#1a56db;">📋 오늘자 보도자료 요약 자동발송 ({today_str})</h2>
                            <p style="color:#475569;">총 {len(articles)}건</p>
                            <table style="width:100%;border-collapse:collapse;font-size:13px;">
                              <thead>
                                <tr style="background:#1a56db;color:#fff;">
                                  <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">기관</th>
                                  <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">유형</th>
                                  <th style="padding:10px 12px;text-align:left;border:1px solid #1e40af;">제목</th>
                                  <th style="padding:10px 12px;text-align:center;border:1px solid #1e40af;width:60px;">링크</th>
                                </tr>
                              </thead>
                              <tbody>{rows_html if articles else '<tr><td colspan="4" style="text-align:center;padding:20px;">오늘 수집된 보도자료가 없습니다.</td></tr>'}</tbody>
                            </table>
                            <div style="margin: 24px 0; text-align: center;">
                              <a href="http://34.30.218.173/" style="display:inline-block;background:#1a56db;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:bold;font-size:15px;">씨지인사이드 대시보드 바로가기 →</a>
                            </div>
                            <hr style="border:none;border-top:1px solid #e2e8f0;margin-top:32px;margin-bottom:16px;">
                            <div style="font-size:12px;color:#64748b;line-height:1.6;text-align:center;">
                              <p style="margin:0;font-weight:bold;">Copyright © (주) 씨지인사이드</p>
                              <p style="margin:0;">서울특별시 마포구 백범로 31길 21 서울창업허브 본관 714호</p>
                              <p style="margin:0;">All rights reserved.</p>
                            </div>
                            </body></html>"""

                            # Send emails
                            sent = 0
                            for r in recipients:
                                msg = MIMEMultipart("alternative")
                                msg["Subject"] = subject
                                msg["From"] = SMTP_USER
                                msg["To"] = r["email"]
                                msg.attach(MIMEText(html_body, "html", "utf-8"))
                                try:
                                    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp_srv:
                                        smtp_srv.ehlo()
                                        smtp_srv.starttls()
                                        smtp_srv.login(SMTP_USER, SMTP_PASS)
                                        smtp_srv.sendmail(SMTP_USER, r["email"], msg.as_string())
                                    sent += 1
                                except Exception as e:
                                    print(f"[Scheduler] Failed to send to {r['email']}: {e}")
                                    
                            print(f"[Scheduler] Sent {sent}/{len(recipients)} emails.")
                            
                            # Only mark as sent if at least one email succeeded
                            if sent > 0:
                                conn.execute(
                                    "INSERT INTO settings (key_name, key_value) VALUES ('last_email_sent_date', ?) "
                                    "ON CONFLICT(key_name) DO UPDATE SET key_value = ?",
                                    (today_str, today_str)
                                )
                                conn.commit()
                except sqlite3.OperationalError as e:
                    # Ignore table not found errors before the very first request hits admin page
                    pass
                finally:
                    conn.close()
                    
            except Exception as outer_e:
                print(f"[Scheduler Error] {outer_e}")
                traceback.print_exc()
                time.sleep(10) # wait a bit before retrying on crash
                
    import threading
    scheduler_thread = threading.Thread(target=run_email_scheduler, daemon=True)
    scheduler_thread.start()

    print(f"Dashboard running: http://{args.host}:{args.port}")
    print(f"DB: {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()


