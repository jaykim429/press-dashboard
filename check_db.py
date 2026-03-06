import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), "press_unified.db")
conn = sqlite3.connect(db_path)
rows = conn.execute("SELECT source_channel, COUNT(*) FROM articles WHERE published_at LIKE '2026-03-06%' GROUP BY source_channel").fetchall()
print("Articles for 2026-03-06:")
for r in rows:
    print(f"  {r[0]}: {r[1]} items")
print(f"Total: {sum((r[1] for r in rows))} items")
conn.close()
