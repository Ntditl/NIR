from lib.db.models import recreateAllTables, getTableNames, dropAllTables, createAllTables
from lib.db.connection import getDbConnection


def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('db_models_manual start')
    recreateAllTables(True)
    names = getTableNames()
    check(len(names) > 0, 'tables present after recreate')
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT 1')
        cur.fetchone()
    dropAllTables()
    createAllTables(True)
    names2 = getTableNames()
    check(len(names2) > 0, 'tables present after drop/create')
    print('db_models_manual ok')

if __name__ == '__main__':
    run()
