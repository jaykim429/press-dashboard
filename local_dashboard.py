import argparse
import difflib
import json
import math
import re
import sqlite3
import time
from collections import Counter
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
    _keywords_cache = {} # {(top_n): (timestamp, data)}
    CACHE_TTL = 300      # 5 minutes

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

        if path == "/api/similar":
            if not self._require_auth_api():
                return
            self.handle_similar(qs)
            return

        if path == "/api/keywords":
            if not self._require_auth_api():
                return
            self.handle_keywords(qs)
            return
        if path == "/api/stats":
            if not self._require_auth_api():
                return
            self.handle_stats(qs)
            return

        if path == "/api/notifications":
            if not self._require_auth_api():
                return
            self.handle_notifications(qs)
            return

        self._json_response({"error": "Not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

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
                
        if press_type == "press_release":
            where.append("a.source_channel IN (?, ?, ?, ?)")
            params.extend(["korea_policy_briefing_press_release", "fss_press_release", "ksd_press_release", "bok_press_release"])
        elif press_type == "press_explainer":
            where.append("a.source_channel IN (?, ?)")
            params.extend(["fss_press_explainer", "fsc_press_explainer"])
        elif press_type == "rule_change_notice":
            where.append("a.source_channel IN (?, ?, ?, ?)")
            params.extend(
                [
                    "fsc_rule_change_notice",
                    "ksd_rule_change_notice",
                    "krx_rule_change_notice",
                    "kofia_rule_change_notice",
                ]
            )
        elif press_type == "recent_rule_change_info":
            where.append("a.source_channel IN (?, ?, ?)")
            params.extend(
                [
                    "fsc_regulation_notice",
                    "krx_recent_rule_change",
                    "kofia_recent_rule_change",
                ]
            )
        elif press_type == "admin_guidance_notice":
            where.append("a.source_channel IN (?, ?)")
            params.extend(["fsc_admin_guidance_notice", "fss_admin_guidance_notice"])
        elif press_type == "admin_guidance_enforcement":
            where.append("a.source_channel IN (?, ?)")
            params.extend(["fsc_admin_guidance_enforcement", "fss_admin_guidance_enforcement"])
        elif press_type == "law_interpretation":
            where.append("a.source_channel = ?")
            params.append("fsc_law_interpretation")
        elif press_type == "no_action_opinion":
            where.append("a.source_channel = ?")
            params.append("fsc_no_action_opinion")
        elif press_type == "other_data":
            where.append("a.source_channel IN (?, ?)")
            params.extend(["kfb_publicdata_other", "fsec_bbs_222"])
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
                payload["content_text"] = re.sub(r'\n{3,}', '\n\n', payload["content_text"])
                
            payload["attachments"] = [dict(a) for a in atts]
            self._json_response(payload)
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

    def handle_keywords(self, qs):
        now = time.time()
        if "keywords" in self._keywords_cache:
            ts, cached = self._keywords_cache["keywords"]
            if now - ts < self.CACHE_TTL:
                self._json_response(cached)
                return

        conn = self._db()
        try:
            # Check if precomputed table exists and has data
            try:
                rows = conn.execute("SELECT keyword, score FROM precomputed_keywords ORDER BY score DESC LIMIT 12").fetchall()
                if rows:
                    payload = [{"keyword": r["keyword"], "score": r["score"]} for r in rows]
                    self._keywords_cache["keywords"] = (now, payload)
                    self._json_response(payload)
                    return
            except sqlite3.OperationalError:
                pass  # Table might not exist yet if ingest hasn't run
            
            # Fallback to empty if not yet precomputed
            payload = []
            self._keywords_cache["keywords"] = (now, payload)
            self._json_response(payload)
        finally:
            conn.close()


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
                where = f"{date_expr} = date('now', 'localtime')"
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

            from collections import defaultdict
            grouped = defaultdict(list)
            for row in rows:
                grouped[row["org"]].append({"type": row["type_label"], "count": row["cnt"]})
            result = [
                {"organization": org, "items": items}
                for org, items in sorted(grouped.items(), key=lambda x: -sum(i["count"] for i in x[1]))
            ]
            self._json_response({"total": total, "groups": result})
        finally:
            conn.close()

    @classmethod
    def _get_kiwi(cls):
        if Kiwi is None:
            return None
        if cls._kiwi is None:
            try:
                cls._kiwi = Kiwi()
            except Exception:
                cls._kiwi = None
        return cls._kiwi

    @staticmethod
    def _stopwords():
        return {
            "\ubcf4\ub3c4\uc790\ub8cc",
            "\ubcf4\ub3c4",
            "\uc790\ub8cc",
            "\uae08\uc735",
            "\uc704\uc6d0\ud68c",
            "\uae08\uc735\uc704\uc6d0\ud68c",
            "\uae08\uc735\uac10\ub3c5\uc6d0",
            "\ud55c\uad6d\uc740\ud589",
            "\ud55c\uad6d\uc608\ud0c1\uacb0\uc81c\uc6d0",
            "\uad00\ub828",
            "\uac1c\ucd5c",
            "\ubc1c\ud45c",
            "\uc548\ub0b4",
            "\uc2dc\ud589",
            "\ub300\ud55c",
            "\uc704\ud55c",
            "\uc5d0\uc11c",
            "\uc73c\ub85c",
            "\uae30\uc900",
            "\uc624\ub298",
            "\ucd5c\uadfc",
            "\ub2e4\uc6b4\ub85c\ub4dc",
            "\ubc14\ub78d\ub2c8\ub2e4",
            "\ud569\ub2c8\ub2e4",
        }

    @classmethod
    def _extract_keywords_with_kiwi_bm25(cls, rows, top_n=12):
        kiwi = cls._get_kiwi()
        if kiwi is None:
            return None

        stopwords = cls._stopwords()
        hangul_only = re.compile(r"^[\uAC00-\uD7A3]{2,}$")
        docs = []

        def tokenize(text: str):
            if not text:
                return []
            try:
                tokens = kiwi.tokenize(text)
            except Exception:
                return []
            out = []
            for tok in tokens:
                if not str(tok.tag).startswith("N"):
                    continue
                word = (tok.form or "").strip()
                if len(word) < 2:
                    continue
                if any(ch.isdigit() for ch in word):
                    continue
                if not hangul_only.match(word):
                    continue
                if word in stopwords:
                    continue
                out.append(word)
            return out

        for row in rows:
            tf = Counter()
            for token in tokenize(row["title"] or ""):
                tf[token] += 3  # title-first weighting
            for token in tokenize(row["content_text"] or ""):
                tf[token] += 1
            if not tf:
                continue
            docs.append(tf)

        return cls._rank_keywords_bm25(docs, top_n=top_n)

    @classmethod
    def _extract_keywords_with_regex_bm25(cls, rows, top_n=12):
        token_pattern = re.compile(r"[\uAC00-\uD7A3A-Za-z]{2,}")
        stopwords = cls._stopwords()
        docs = []

        for row in rows:
            title = row["title"] or ""
            body = row["content_text"] or ""
            tf = Counter()

            for token in token_pattern.findall(title):
                if token in stopwords:
                    continue
                if any(ch.isdigit() for ch in token):
                    continue
                tf[token] += 3

            for token in token_pattern.findall(body):
                if token in stopwords:
                    continue
                if any(ch.isdigit() for ch in token):
                    continue
                tf[token] += 1

            if tf:
                docs.append(tf)

        return cls._rank_keywords_bm25(docs, top_n=top_n)

    @staticmethod
    def _rank_keywords_bm25(docs, top_n=12, k1=1.5, b=0.75):
        if not docs:
            return []

        n_docs = len(docs)
        doc_lengths = [sum(tf.values()) for tf in docs]
        avgdl = (sum(doc_lengths) / n_docs) if n_docs else 0.0
        if avgdl <= 0:
            return []

        df = Counter()
        for tf in docs:
            for term in tf.keys():
                df[term] += 1

        scores = Counter()
        for term, df_t in df.items():
            # Robertson/Sparck Jones IDF (BM25)
            idf = math.log(1.0 + (n_docs - df_t + 0.5) / (df_t + 0.5))
            for i, tf in enumerate(docs):
                f = tf.get(term, 0)
                if f <= 0:
                    continue
                dl = doc_lengths[i]
                denom = f + k1 * (1.0 - b + b * (dl / avgdl))
                if denom <= 0:
                    continue
                scores[term] += idf * (f * (k1 + 1.0) / denom)

        ranked = scores.most_common(top_n)
        return [{"keyword": k, "score": round(v, 4)} for k, v in ranked]

    @classmethod
    def _extract_keywords(cls, rows, top_n=12):
        items = cls._extract_keywords_with_kiwi_bm25(rows, top_n=top_n)
        if items is not None:
            return items
        return cls._extract_keywords_with_regex_bm25(rows, top_n=top_n)


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
    server = HTTPServer((args.host, args.port), DashboardHandler)
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


