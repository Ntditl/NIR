import os
from lib.simpledb import SimpleDatabase
from lib.visualization.plots import PlotBuilder
from lib.utils.timing import measureExecutionTime

DEFAULT_ROW_COUNTS = [1000, 5000, 10000]
CSV_FILENAME = 'simpledb_bench.csv'
SELECT_EQ_GRAPH = 'simpledb_select_where'
DELETE_EQ_GRAPH = 'simpledb_delete_where'
INSERT_GRAPH = 'simpledb_insert'
SELECT_STRING_GRAPH = 'simpledb_select_where_string'
SELECT_INT_GRAPH = 'simpledb_select_where_int'
DELETE_STRING_GRAPH = 'simpledb_delete_where_string'
DELETE_INT_GRAPH = 'simpledb_delete_where_int'
INSERT_NO_INDEX_GRAPH = 'simpledb_insert_no_index'


def _ensureEmpty(database):
    database.dropDataDir()


def _createBaseTable(database, tableName, withIndex=False):
    database.execute('CREATE TABLE ' + tableName + ' (id INT, name VARCHAR(32))')
    if withIndex:
        database.execute('CREATE INDEX ON ' + tableName + '(id)')


def _fillTable(database, tableName, rowCount):
    current = 1
    while current <= rowCount:
        database.execute('INSERT INTO ' + tableName + ' (id,name) VALUES (' + str(current) + ',"name_' + str(current) + '")')
        current = current + 1


def _measureCallable(repeats, fn):
    times = []
    index = 0
    while index < repeats:
        executionTime = measureExecutionTime(fn)
        times.append(executionTime)
        index = index + 1
    best = times[0]
    i = 1
    while i < len(times):
        if times[i] < best:
            best = times[i]
        i = i + 1
    total = 0.0
    i = 0
    while i < len(times):
        total = total + times[i]
        i = i + 1
    avg = total / len(times)
    return best, avg


def _timeSelect(database, tableName, columnName, value, repeats):
    def run():
        if isinstance(value, int):
            sql = 'SELECT * FROM ' + tableName + ' WHERE ' + columnName + ' = ' + str(value)
        else:
            sql = 'SELECT * FROM ' + tableName + ' WHERE ' + columnName + ' = "' + str(value) + '"'
        database.execute(sql)
    return _measureCallable(repeats, run)


def _timeDelete(database, tableName, columnName, value, repeats):
    def run():
        if isinstance(value, int):
            sql = 'DELETE FROM ' + tableName + ' WHERE ' + columnName + ' = ' + str(value)
        else:
            sql = 'DELETE FROM ' + tableName + ' WHERE ' + columnName + ' = "' + str(value) + '"'
        database.execute(sql)
    return _measureCallable(repeats, run)


def _timeInsertBulk(database, tableName, countRows, withIndex, repeats):
    def run():
        _ensureEmpty(database)
        _createBaseTable(database, tableName, withIndex=withIndex)
        current = 1
        while current <= countRows:
            database.execute('INSERT INTO ' + tableName + ' (id,name) VALUES (' + str(current) + ',"x")')
            current = current + 1
    return _measureCallable(repeats, run)


def _writeCsv(resultsDir, rowsData):
    path = os.path.join(resultsDir, CSV_FILENAME)
    with open(path, 'w', encoding='utf-8') as f:
        f.write('operation,variant,rows,repeats,best_time_seconds,avg_time_seconds\n')
        i = 0
        while i < len(rowsData):
            r = rowsData[i]
            f.write(r[0] + ',' + r[1] + ',' + str(r[2]) + ',' + str(r[3]) + ',' + f"{r[4]:.6f}" + ',' + f"{r[5]:.6f}" + '\n')
            i = i + 1
    return path


def runSimpleDbIndexBench(resultsDir, rowCounts=None, repeats=3):
    if rowCounts is None:
        rowCounts = list(DEFAULT_ROW_COUNTS)
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)
    database = SimpleDatabase()
    selectStringNoIndex = []
    selectIntNoIndex = []
    selectIntIndex = []
    deleteStringNoIndex = []
    deleteIntNoIndex = []
    deleteIntIndex = []
    insertNoIndex = []
    insertIndex = []
    csvRows = []
    for countValue in rowCounts:
        _ensureEmpty(database)
        _createBaseTable(database, 't_noidx', withIndex=False)
        _fillTable(database, 't_noidx', countValue)
        midVal = countValue // 2
        if midVal < 1:
            midVal = 1
        bestSelStr, avgSelStr = _timeSelect(database, 't_noidx', 'name', 'name_' + str(midVal), repeats)
        selectStringNoIndex.append((countValue, bestSelStr))
        csvRows.append(('select', 'string_no_index', countValue, repeats, bestSelStr, avgSelStr))
        bestSelIntNo, avgSelIntNo = _timeSelect(database, 't_noidx', 'id', midVal, repeats)
        selectIntNoIndex.append((countValue, bestSelIntNo))
        csvRows.append(('select', 'int_no_index', countValue, repeats, bestSelIntNo, avgSelIntNo))
        database.dropDataDir()
        _createBaseTable(database, 't_idx', withIndex=True)
        _fillTable(database, 't_idx', countValue)
        bestSelIntIdx, avgSelIntIdx = _timeSelect(database, 't_idx', 'id', midVal, repeats)
        selectIntIndex.append((countValue, bestSelIntIdx))
        csvRows.append(('select', 'int_index', countValue, repeats, bestSelIntIdx, avgSelIntIdx))
        _ensureEmpty(database)
        _createBaseTable(database, 't_noidx', withIndex=False)
        _fillTable(database, 't_noidx', countValue)
        bestDelStr, avgDelStr = _timeDelete(database, 't_noidx', 'name', 'name_' + str(midVal), repeats)
        deleteStringNoIndex.append((countValue, bestDelStr))
        csvRows.append(('delete', 'string_no_index', countValue, repeats, bestDelStr, avgDelStr))
        _ensureEmpty(database)
        _createBaseTable(database, 't_noidx', withIndex=False)
        _fillTable(database, 't_noidx', countValue)
        bestDelIntNo, avgDelIntNo = _timeDelete(database, 't_noidx', 'id', midVal, repeats)
        deleteIntNoIndex.append((countValue, bestDelIntNo))
        csvRows.append(('delete', 'int_no_index', countValue, repeats, bestDelIntNo, avgDelIntNo))
        _ensureEmpty(database)
        _createBaseTable(database, 't_idx', withIndex=True)
        _fillTable(database, 't_idx', countValue)
        bestDelIntIdx, avgDelIntIdx = _timeDelete(database, 't_idx', 'id', midVal, repeats)
        deleteIntIndex.append((countValue, bestDelIntIdx))
        csvRows.append(('delete', 'int_index', countValue, repeats, bestDelIntIdx, avgDelIntIdx))
        bestInsNo, avgInsNo = _timeInsertBulk(database, 'ti', countValue, withIndex=False, repeats=repeats)
        insertNoIndex.append((countValue, bestInsNo))
        csvRows.append(('insert', 'no_index', countValue, repeats, bestInsNo, avgInsNo))
        bestInsIdx, avgInsIdx = _timeInsertBulk(database, 'ti', countValue, withIndex=True, repeats=repeats)
        insertIndex.append((countValue, bestInsIdx))
        csvRows.append(('insert', 'index', countValue, repeats, bestInsIdx, avgInsIdx))
    _writeCsv(resultsDir, csvRows)
    builder = PlotBuilder(resultsDir)
    xsSel = []
    ysSelStr = []
    ysSelIntNo = []
    ysSelIntIdx = []
    for pair in selectStringNoIndex:
        xsSel.append(pair[0])
        ysSelStr.append(pair[1])
    for pair in selectIntNoIndex:
        ysSelIntNo.append(pair[1])
    for pair in selectIntIndex:
        ysSelIntIdx.append(pair[1])
    builder.buildChart({
        'str_no_index': (xsSel, ysSelStr),
        'int_no_index': (xsSel, ysSelIntNo),
        'int_index': (xsSel, ysSelIntIdx)
    }, 'SELECT equality', 'Rows', 'Time (s)', SELECT_EQ_GRAPH, True)
    xsDel = []
    ysDelStr = []
    ysDelIntNo = []
    ysDelIntIdx = []
    for pair in deleteStringNoIndex:
        xsDel.append(pair[0])
        ysDelStr.append(pair[1])
    for pair in deleteIntNoIndex:
        ysDelIntNo.append(pair[1])
    for pair in deleteIntIndex:
        ysDelIntIdx.append(pair[1])
    builder.buildChart({
        'str_no_index': (xsDel, ysDelStr),
        'int_no_index': (xsDel, ysDelIntNo),
        'int_index': (xsDel, ysDelIntIdx)
    }, 'DELETE equality', 'Rows', 'Time (s)', DELETE_EQ_GRAPH, True)
    xsIns = []
    ysInsNo = []
    ysInsIdx = []
    for pair in insertNoIndex:
        xsIns.append(pair[0])
        ysInsNo.append(pair[1])
    for pair in insertIndex:
        ysInsIdx.append(pair[1])
    builder.buildChart({
        'no_index': (xsIns, ysInsNo),
        'index': (xsIns, ysInsIdx)
    }, 'INSERT performance', 'Rows inserted', 'Time (s)', INSERT_GRAPH, True)
    builder.buildChart({'string_select': (xsSel, ysSelStr)}, 'SELECT WHERE string', 'Rows', 'Time (s)', SELECT_STRING_GRAPH, True)
    builder.buildChart({'int_select_no_index': (xsSel, ysSelIntNo)}, 'SELECT WHERE int (no index)', 'Rows', 'Time (s)', SELECT_INT_GRAPH, True)
    builder.buildChart({'string_delete': (xsDel, ysDelStr)}, 'DELETE WHERE string', 'Rows', 'Time (s)', DELETE_STRING_GRAPH, True)
    builder.buildChart({'int_delete_no_index': (xsDel, ysDelIntNo)}, 'DELETE WHERE int (no index)', 'Rows', 'Time (s)', DELETE_INT_GRAPH, True)
    builder.buildChart({'insert_no_index': (xsIns, ysInsNo)}, 'INSERT no index', 'Rows inserted', 'Time (s)', INSERT_NO_INDEX_GRAPH, True)
    return True
