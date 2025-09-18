from lib.tableModels import getCreateTablesSql, getDropTablesSql, getTableNames
from lib.databaseConnection import getDbConnection


def checkTableNames():
    names = getTableNames()
    if not isinstance(names, list):
        raise RuntimeError("getTableNames should return list")
    i = 0
    while i < len(names):
        if not isinstance(names[i], str):
            raise RuntimeError("table name should be string")
        i = i + 1


def checkSqlExecutable():
    with getDbConnection() as (conn, cur):
        drops = getDropTablesSql()
        i = 0
        while i < len(drops):
            cur.execute(drops[i])
            i = i + 1
        creates = getCreateTablesSql()
        i = 0
        while i < len(creates):
            cur.execute(creates[i])
            i = i + 1


def main():
    checkTableNames()
    checkSqlExecutable()
    print("tableModels checks passed")


if __name__ == "__main__":
    main()
