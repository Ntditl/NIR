from lib.db.models import recreateAllTables
from lib.db.connection import getDbConnection
from lib.managers.dataManager import DataManager


def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def _viewer_gen(cur, rowCount):
    i = 0
    while i < rowCount:
        cur.execute("INSERT INTO viewer (first_name,last_name,email,phone_number) VALUES (%s,%s,%s,%s)", (
            'f'+str(i), 'l'+str(i), 'u'+str(i)+'@ex', '+7'+str(i)
        ))
        i = i + 1

def run():
    print('data_manager_manual start')
    recreateAllTables(True)
    dm = DataManager()
    dm.replaceData('viewer', _viewer_gen, 7)
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        c = cur.fetchone()[0]
        check(c == 7, 'replaceData row count')
    dm.replaceData('viewer', _viewer_gen, 3)
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        c2 = cur.fetchone()[0]
        check(c2 == 3, 'replaceData second run row count')
    print('data_manager_manual ok')

if __name__ == '__main__':
    run()

