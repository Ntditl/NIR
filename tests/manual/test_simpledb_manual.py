import os
import shutil
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine
from lib.simpledb.rowcodec import packValue, unpackValue

BASE_DIR = 'tmp_simpledb_cases'

def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def build_schema():
    return Schema('tcase', [
        {"name": "id", "type": "INT", "index": True},
        {"name": "name", "type": "VARCHAR", "max": 16},
        {"name": "note", "type": "VARCHAR", "max": 32}
    ])

def setup_dir():
    if os.path.isdir(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    os.makedirs(BASE_DIR, exist_ok=True)

def test_rowcodec():
    b = packValue('INT', None, 42)
    v, off = unpackValue('INT', b, 0)
    check(v == 42 and off == 8, 'int pack/unpack')
    s = 'Тест'
    b2 = packValue('VARCHAR', 10, s)
    v2, o2 = unpackValue('VARCHAR', b2, 0)
    check(v2 == s, 'str pack/unpack')

def test_engine_basic():
    setup_dir()
    schema = build_schema()
    files = TableFiles(BASE_DIR, schema.tableName)
    eng = TableEngine(files, schema)
    eng.create(); eng.open()
    for i in range(1, 11):
        eng.insertRow({"id": i, "name": 'n'+str(i), "note": 'val_'+str(i)})
    all_rows = eng.select(['*'], None)
    check(len(all_rows) == 10, 'insert/select count')
    one = eng.select(['id','name'], ('id', 5))
    check(len(one) == 1 and one[0][0] == 5, 'select where')
    deleted = eng.deleteWhere('id', 3)
    check(deleted == 1, 'delete where count')
    left = eng.select(['id'], None)
    check(len(left) == 9, 'count after delete')
    eng._rebuildSingleIndex('id')
    q = eng.select(['id'], ('id', 5))
    check(len(q) == 1 and q[0][0] == 5, 'select after rebuild index')
    eng.close()

def run():
    print('simpledb_manual start')
    test_rowcodec()
    test_engine_basic()
    print('simpledb_manual ok')

if __name__ == '__main__':
    run()

