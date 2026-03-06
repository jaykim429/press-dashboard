import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), "press_unified.db")
conn = sqlite3.connect(db_path)
rows = conn.execute("SELECT source_channel, title, published_at FROM articles WHERE published_at >= '2026-03-06' ORDER BY published_at DESC").fetchall()
print(f"Found {len(rows)} articles for today (local):")
for r in rows:
    print(f"  [{r[0]}] {r[1]} ({r[2]})")
conn.close()
