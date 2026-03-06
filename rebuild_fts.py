import sqlite3
conn = sqlite3.connect('press_unified.db')
print('Before:', conn.execute('SELECT COUNT(*) FROM articles_fts WHERE articles_fts MATCH ?', ('±ÝÀ¶',)).fetchone()[0])
conn.execute('INSERT INTO articles_fts(articles_fts) VALUES(''rebuild'')')
conn.commit()
print('After:', conn.execute('SELECT COUNT(*) FROM articles_fts WHERE articles_fts MATCH ?', ('±ÝÀ¶',)).fetchone()[0])
conn.close()
