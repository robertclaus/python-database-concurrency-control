import sys
import MySQLdb
from config import config

if len(sys.argv)>1:
    isolation_level = sys.argv[1]
    if isolation_level == 'ru':
      isolation_level = 0
    if isolation_level == 's':
      isolation_level = 1

    query_text = [
    "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
    ]

    conn = MySQLdb.connect(host=config.MYSQL_HOST,user=config.MYSQL_USER,passwd=config.MYSQL_PASSWORD,db=config.MYSQL_DB_NAME)
    cur = conn.cursor()
    cur.execute(query_text[isolation_level])
    conn.commit()
