# tests/conftest.py

import sys
import pathlib


projectRoot = pathlib.Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(projectRoot))

def dropAndCreateAll():
    from lib.databaseConnection import getDbConnection
    from lib.tableModels import getCreateTablesSql, getDropTablesSql
    with getDbConnection() as (connection, cursor):
        drops = getDropTablesSql()
        i = 0
        while i < len(drops):
            cursor.execute(drops[i])
            i = i + 1
        creates = getCreateTablesSql()
        i = 0
        while i < len(creates):
            cursor.execute(creates[i])
            i = i + 1

def truncateAll():
    from lib.databaseConnection import getDbConnection
    from lib.tableModels import getTableNames
    with getDbConnection() as (connection, cursor):
        names = getTableNames()
        i = len(names) - 1
        while i >= 0:
            cursor.execute(f"TRUNCATE TABLE {names[i]} RESTART IDENTITY CASCADE;")
            i = i - 1
