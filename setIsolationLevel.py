import sys
import MySQLdb

if len(sys.argv)>1:
    isolation_level = int(sys.argv[1])

    query_text = [
    "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
    ]
    
    print(query_text)

    conn = MySQLdb.connect(host='localhost',user='root',passwd='test',db='t')
    cur = conn.cursor()
    cur.execute(query_text[isolation_level])
    conn.commit()
