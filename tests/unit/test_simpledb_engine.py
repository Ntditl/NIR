import os
import shutil
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine

dataDir = 'test_simpledb_data'


def resetDir():
    if os.path.isdir(dataDir):
        shutil.rmtree(dataDir)
    os.makedirs(dataDir, exist_ok=True)


def buildSchema():
    return Schema('people', [
        {"name": "id", "type": "INT", "index": True},
        {"name": "name", "type": "VARCHAR", "max": 32, "index": True},
        {"name": "age", "type": "INT"}
    ])


def testSimpleDbBasic():
    resetDir()
    schema = buildSchema()
    files = TableFiles(dataDir, schema.tableName)
    eng = TableEngine(files, schema)
    eng.create()
    eng.open()
    eng.insertRow({"id": 1, "name": "Alice", "age": 30})
    eng.insertRow({"id": 2, "name": "Bob", "age": 25})
    eng.insertRow({"id": 3, "name": "Carol", "age": 40})
    allRows = eng.select(['*'], None)
    if len(allRows) != 3:
        raise RuntimeError('select * size mismatch')
    r = eng.select(['id','name'], ('id', 2))
    if len(r) != 1 or r[0][0] != 2:
        raise RuntimeError('select by id failed')
    r2 = eng.select(['id'], ('name', 'Bob'))
    if len(r2) != 1 or r2[0][0] != 2:
        raise RuntimeError('select by string index failed')
    deleted = eng.deleteWhere('age', 30)
    if deleted != 1:
        raise RuntimeError('deleteWhere count mismatch')
    remain = eng.select(['*'], None)
    if len(remain) != 2:
        raise RuntimeError('remaining rows mismatch')
    eng.close()
    eng2 = TableEngine(files, schema)
    eng2.open()
    r3 = eng2.select(['id'], ('id', 3))
    if len(r3) != 1:
        raise RuntimeError('reopen index select failed')
    eng2.close()


def main():
    testSimpleDbBasic()
    print('Проверка SimpleDB engine пройдена')

if __name__ == '__main__':
    main()

