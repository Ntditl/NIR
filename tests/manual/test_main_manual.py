from lib.db.models import recreateAllTables
from lib.db.connection import getDbConnection
from lib.main import generateSampleData, recreateTables

def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('main_manual start')
    recreateTables()
    generateSampleData()
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        c = cur.fetchone()[0]
        check(c > 0, 'viewer count after sample data')
    print('main_manual ok')

if __name__ == '__main__':
    run()

