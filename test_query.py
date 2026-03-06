import sqlite3

conn = sqlite3.connect('press_unified.db')
def run_query(q):
    words = q.split()
    if len(words) > 1:
        joined_word = "".join(words)
        and_words = " AND ".join(words)
        match_query = f'"{q}" OR "{joined_word}" OR ({and_words})'
    else:
        joined_word = q
        match_query = f'"{q}"'
    
    print("Match query:", match_query)
    sql = """
    SELECT a.id, a.title 
    FROM articles a 
    WHERE a.id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?)
       OR replace(a.title, ' ', '') LIKE ?
    LIMIT 5
    """
    res = conn.execute(sql, (match_query, f"%{joined_word}%")).fetchall()
    print(f"Results for '{q}':", len(res), [r[1] for r in res])

run_query("소비자물가")
run_query("소비자 물가")
conn.close()
