import os
import shutil
from lib.simpledb.database import SimpleDatabase
from lib.simpledb.constants import DATA_DIR

def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('simpledb_sql_manual start')
    dataDir = os.path.abspath(DATA_DIR)
    if os.path.isdir(dataDir):
        for f in os.listdir(dataDir):
            p = os.path.join(dataDir, f)
            try:
                os.remove(p)
            except Exception:
                pass
    db = SimpleDatabase(dataDir)
    db.execute('CREATE TABLE t1 (id INT, name VARCHAR(16))')
    db.execute('INSERT INTO t1 (id,name) VALUES (1,"Alice")')
    db.execute('INSERT INTO t1 (id,name) VALUES (2,"Bob")')
    rowsAll = db.execute('SELECT * FROM t1')
    check(len(rowsAll) == 2, 'select * count')
    rowsOne = db.execute('SELECT id,name FROM t1 WHERE id=2')
    check(len(rowsOne) == 1 and rowsOne[0][0] == 2, 'where id=2')
    db.execute('CREATE INDEX ON t1(id)')
    db.execute('DELETE * FROM t1')
    rowsEmpty = db.execute('SELECT * FROM t1')
    check(len(rowsEmpty) == 0, 'delete * cleared')
    db.closeAll()
    print('simpledb_sql_manual ok')

if __name__ == '__main__':
    run()

