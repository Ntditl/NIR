import os
import time
import timeit
import matplotlib.pyplot as plt
from lib.simpledb import SimpleDatabase


def _ensureEmpty(db):
    db.dropDataDir()


def _createBaseTable(db, name, withIndex=False):
    db.execute("CREATE TABLE " + name + " (id INT, name VARCHAR(32))")
    if withIndex:
        db.execute("CREATE INDEX ON " + name + "(id)")


def _fillTable(db, name, rows):
    i = 1
    while i <= rows:
        s = "INSERT INTO " + name + " (id,name) VALUES (" + str(i) + ",\"name_" + str(i) + "\")"
        db.execute(s)
        i = i + 1


def _timeSelectWhereEq(db, name, col, val, repeats):
    def run():
        if isinstance(val, int):
            sql = "SELECT * FROM " + name + " WHERE " + col + " = " + str(val)
        else:
            sql = "SELECT * FROM " + name + " WHERE " + col + " = \"" + str(val) + "\""
        db.execute(sql)
    timer = timeit.Timer(run)
    arr = timer.repeat(repeat=repeats, number=1)
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def _timeDeleteWhereEq(db, name, col, val, repeats):
    def run():
        if isinstance(val, int):
            sql = "DELETE FROM " + name + " WHERE " + col + " = " + str(val)
        else:
            sql = "DELETE FROM " + name + " WHERE " + col + " = \"" + str(val) + "\""
        db.execute(sql)
    timer = timeit.Timer(run)
    arr = timer.repeat(repeat=repeats, number=1)
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def _timeInsertBulk(db, name, count, repeats):
    def run():
        _ensureEmpty(db)
        _createBaseTable(db, name, withIndex=False)
        i = 1
        while i <= count:
            db.execute("INSERT INTO " + name + " (id,name) VALUES (" + str(i) + ",\"x\")")
            i = i + 1
    timer = timeit.Timer(run)
    arr = timer.repeat(repeat=repeats, number=1)
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def _timeInsertBulkWithIndex(db, name, count, repeats):
    def run():
        _ensureEmpty(db)
        _createBaseTable(db, name, withIndex=True)
        i = 1
        while i <= count:
            db.execute("INSERT INTO " + name + " (id,name) VALUES (" + str(i) + ",\"x\")")
            i = i + 1
    timer = timeit.Timer(run)
    arr = timer.repeat(repeat=repeats, number=1)
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def runSimpleDbIndexBench(resultsDir, rowCounts=None, repeats=3):
    if rowCounts is None:
        rowCounts = [1000, 5000, 10000]
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)
    db = SimpleDatabase()
    xs = []
    ysStrSel = []
    ysIntSelNoIdx = []
    ysIntSelIdx = []
    ysStrDel = []
    ysIntDelNoIdx = []
    ysIntDelIdx = []
    i = 0
    while i < len(rowCounts):
        n = int(rowCounts[i])
        _ensureEmpty(db)
        _createBaseTable(db, 't_noidx', withIndex=False)
        _fillTable(db, 't_noidx', n)
        xs.append(n)
        t = _timeSelectWhereEq(db, 't_noidx', 'name', "name_" + str(n//2), repeats)
        ysStrSel.append(t)
        t = _timeSelectWhereEq(db, 't_noidx', 'id', n//2, repeats)
        ysIntSelNoIdx.append(t)
        db.dropDataDir()
        _createBaseTable(db, 't_idx', withIndex=True)
        _fillTable(db, 't_idx', n)
        t = _timeSelectWhereEq(db, 't_idx', 'id', n//2, repeats)
        ysIntSelIdx.append(t)
        _ensureEmpty(db)
        _createBaseTable(db, 't_noidx', withIndex=False)
        _fillTable(db, 't_noidx', n)
        t = _timeDeleteWhereEq(db, 't_noidx', 'name', "name_" + str(n//2), repeats)
        ysStrDel.append(t)
        _ensureEmpty(db)
        _createBaseTable(db, 't_noidx', withIndex=False)
        _fillTable(db, 't_noidx', n)
        t = _timeDeleteWhereEq(db, 't_noidx', 'id', n//2, repeats)
        ysIntDelNoIdx.append(t)
        _ensureEmpty(db)
        _createBaseTable(db, 't_idx', withIndex=True)
        _fillTable(db, 't_idx', n)
        t = _timeDeleteWhereEq(db, 't_idx', 'id', n//2, repeats)
        ysIntDelIdx.append(t)
        i = i + 1
    plt.figure(figsize=(10, 6))
    plt.plot(xs, ysStrSel, marker='o', label='str WHERE =')
    plt.plot(xs, ysIntSelNoIdx, marker='s', label='int WHERE = (no index)')
    plt.plot(xs, ysIntSelIdx, marker='^', label='int WHERE = (index)')
    plt.xlabel('Rows')
    plt.ylabel('Time (s)')
    plt.title('SELECT WHERE equality')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(resultsDir, 'simpledb_select_where.png'))
    plt.close()
    plt.figure(figsize=(10, 6))
    plt.plot(xs, ysStrDel, marker='o', label='str WHERE =')
    plt.plot(xs, ysIntDelNoIdx, marker='s', label='int WHERE = (no index)')
    plt.plot(xs, ysIntDelIdx, marker='^', label='int WHERE = (index)')
    plt.xlabel('Rows')
    plt.ylabel('Time (s)')
    plt.title('DELETE WHERE equality')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(resultsDir, 'simpledb_delete_where.png'))
    plt.close()
    xsI = []
    ysInsNoIdx = []
    ysInsIdx = []
    i = 0
    while i < len(rowCounts):
        n = int(rowCounts[i])
        t1 = _timeInsertBulk(db, 'ti', n, repeats)
        t2 = _timeInsertBulkWithIndex(db, 'ti', n, repeats)
        xsI.append(n)
        ysInsNoIdx.append(t1)
        ysInsIdx.append(t2)
        i = i + 1
    plt.figure(figsize=(10, 6))
    plt.plot(xsI, ysInsNoIdx, marker='o', label='no index')
    plt.plot(xsI, ysInsIdx, marker='s', label='index')
    plt.xlabel('Rows inserted')
    plt.ylabel('Time (s)')
    plt.title('INSERT performance')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(resultsDir, 'simpledb_insert.png'))
    plt.close()

