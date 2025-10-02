import os
import shutil
from lib.db.models import recreateAllTables
from lib.data.generators import RandomDataGenerator
from lib.managers.sandboxManager import SandboxManager
from lib.managers.backupManager import BackupManager
from lib.db.connection import getDbConnection

BACKUP_DIR = 'tmp_manual_backup_2'


def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('sandbox_backup_manual start')
    recreateAllTables(True)
    gen = RandomDataGenerator()
    gen.generateData(3, 2, 1, 1, 1, 0.2, 0.2, 0.2)
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        before = cur.fetchone()[0]
    sm = SandboxManager()
    sm.createSandboxSchema()
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='sandbox'")
        row = cur.fetchone()
        check(row is not None, 'sandbox schema exists')
    if os.path.isdir(BACKUP_DIR):
        shutil.rmtree(BACKUP_DIR)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    bm = BackupManager(BACKUP_DIR)
    bm.backupAllTables()
    recreateAllTables(True)
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        empty = cur.fetchone()[0]
        check(empty == 0, 'after recreate empty')
    bm.restoreAllTables()
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM viewer')
        after = cur.fetchone()[0]
        check(after == before, 'restore viewer count match')
    print('sandbox_backup_manual ok')

if __name__ == '__main__':
    run()

