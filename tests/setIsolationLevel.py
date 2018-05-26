import sys
import MySQLdb
from runners.config import config

if len(sys.argv)>1:
    isolation_level = sys.argv[1]
    if isolation_level == 'ru':
      isolation_level = 0
    if isolation_level == 's':
      isolation_level = 1
    if isolation_level == 'rc':
      isolation_level = 2
    if isolation_level == 'rr':
      isolation_level = 3
    if isolation_level == 'ru-exi':
      isolation_level = 0
    if isolation_level == 'd':
      isolation_level = 4

    query_text = [
    "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED;",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
    "DELETE FROM t.a;",
    ]

    conn = MySQLdb.connect(host=config.MYSQL_HOST,user=config.MYSQL_USER,passwd=config.MYSQL_PASSWORD,db=config.MYSQL_DB_NAME)
    cur = conn.cursor()
    cur.execute(query_text[isolation_level])
    conn.commit()
