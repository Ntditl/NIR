from lib.managers.dataManager import DataManager
from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql, getDropTablesSql


def recreateSchema():
    with getDbConnection() as (conn, cur):
        drops = getDropTablesSql()
        for i in range(len(drops)):
            cur.execute(drops[i])
        creates = getCreateTablesSql()
        for i in range(len(creates)):
            cur.execute(creates[i])


def generatorCinema(cur, rowCount):
    rows = []
    for i in range(rowCount):
        rows.append((f'name_{i}', f'addr_{i}', f'+{i:04d}', 'City'))
    cur.executemany("INSERT INTO cinema (name,address,phone_number,city) VALUES (%s,%s,%s,%s)", rows)


def testDataManagerReplace():
    recreateSchema()
    mgr = DataManager()
    mgr.replaceData('cinema', generatorCinema, 5)
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM cinema")
        c = cur.fetchone()[0]
        if c != 5:
            raise RuntimeError('replaceData failed')


def main():
    testDataManagerReplace()
    print('Проверка DataManager пройдена')

if __name__ == '__main__':
    main()

