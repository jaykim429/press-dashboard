import re

filepath = r'C:\Users\admin1\Documents\보도자료 테스트\local_dashboard.py'

with open(filepath, 'rb') as f:
    content = f.read().decode('utf-8')

# Normalize newlines
content = content.replace('\r\n', '\n')

# 1. Remove _keywords_cache
content = content.replace(
    '    _keywords_cache = {} # {(top_n): (timestamp, data)}\n',
    ''
)

# 2. Replace route
content = content.replace(
    '''        if path == "/api/keywords":
            if not self._require_auth_api():
                return
            self.handle_keywords(qs)
            return''',
    '''        if path == "/api/suggest":
            if not self._require_auth_api():
                return
            self.handle_suggest(qs)
            return'''
)

# 3. Add id to handle_notifications 
# This is tricky without exact string replace. Let's find the exact string to replace.
old_query = '''            entry_rows = conn.execute(
                f"""
                SELECT
                    COALESCE(a.organization, '(기관 없음)') AS org,
                    {type_case} AS type_label,
                    COALESCE(a.title, '(제목 없음)') AS title,
                    COALESCE(a.detail_url, a.original_url, '') AS url,
                    a.published_at AS published_at
                FROM articles a'''

new_query = '''            entry_rows = conn.execute(
                f"""
                SELECT
                    a.id AS id,
                    COALESCE(a.organization, '(기관 없음)') AS org,
                    {type_case} AS type_label,
                    COALESCE(a.title, '(제목 없음)') AS title,
                    COALESCE(a.detail_url, a.original_url, '') AS url,
                    a.published_at AS published_at
                FROM articles a'''
content = content.replace(old_query, new_query)

old_entries = '''            entries = [
                {
                    "organization": row["org"],
                    "type": row["type_label"],
                    "title": row["title"],
                    "url": row["url"],
                    "published_at": row["published_at"],
                }
                for row in entry_rows
            ]'''

new_entries = '''            entries = [
                {
                    "id": row["id"],
                    "organization": row["org"],
                    "type": row["type_label"],
                    "title": row["title"],
                    "url": row["url"],
                    "published_at": row["published_at"],
                }
                for row in entry_rows
            ]'''
content = content.replace(old_entries, new_entries)


# 4. Remove handle_keywords
idx_start = content.find('    def handle_keywords(self, qs):')
idx_end = content.find('    def handle_notifications(self, qs):')
if idx_start != -1 and idx_end != -1:
    content = content[:idx_start] + content[idx_end:]
else:
    print("FAILED TO REMOVE handle_keywords")

# 5. Remove _get_kiwi until def main(), and insert handle_suggest
idx_kiwi = content.find('    @classmethod\n    def _get_kiwi(cls):')
if idx_kiwi == -1: idx_kiwi = content.find('    def _get_kiwi(cls):')

idx_main = content.find('\ndef main():')

handle_suggest = '''    def handle_suggest(self, qs):
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

'''

if idx_kiwi != -1 and idx_main != -1:
    content = content[:idx_kiwi] + handle_suggest + content[idx_main:]
else:
    print("FAILED TO REMOVE kiwi AND INSERT suggest")


with open(filepath, 'wb') as f:
    f.write(content.encode('utf-8'))

print("Done python script")
