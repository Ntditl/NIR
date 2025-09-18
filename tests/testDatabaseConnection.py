import psycopg2

from lib.databaseConnection import getDbConnection
from lib.tableModels import getTableNames, getCreateTablesSql, getDropTablesSql


def dropAndCreateSchema():
    with getDbConnection() as (conn, cur):
        commands = getDropTablesSql()
        i = 0
        while i < len(commands):
            cur.execute(commands[i])
            i = i + 1
        commands = getCreateTablesSql()
        i = 0
        while i < len(commands):
            cur.execute(commands[i])
            i = i + 1


def truncateAllTables():
    with getDbConnection() as (conn, cur):
        names = getTableNames()
        i = len(names) - 1
        while i >= 0:
            cur.execute(f"TRUNCATE TABLE {names[i]} RESTART IDENTITY CASCADE;")
            i = i - 1


def checkConnection():
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT 1;")
        row = cur.fetchone()
        if row is None or row[0] != 1:
            raise RuntimeError("SELECT 1 failed")


def checkCommitAndRollback():
    truncateAllTables()
    with getDbConnection() as (conn, cur):
        cur.execute(
            "INSERT INTO viewer (first_name, last_name, email, phone_number) VALUES (%s, %s, %s, %s)",
            ("CommitTest", "User", "commit@test.com", "+0012345678"),
        )
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT first_name FROM viewer WHERE email = 'commit@test.com'")
        row = cur.fetchone()
        if row is None or row[0] != "CommitTest":
            raise RuntimeError("Commit did not persist")
    try:
        with getDbConnection() as (conn, cur):
            cur.execute("INSERT INTO viewer(nonexistent_column) VALUES (1);")
    except psycopg2.Error:
        pass
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer;")
        count = cur.fetchone()[0]
        if count != 1:
            raise RuntimeError("Rollback check failed")


def main():
    dropAndCreateSchema()
    checkConnection()
    checkCommitAndRollback()
    print("databaseConnection checks passed")


if __name__ == "__main__":
    main()
