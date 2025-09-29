import datetime
import random
import timeit
import os
from lib.db.connection import getDbConnection
from lib.visualization.plots import PlotBuilder

REPEATS = 3


def _getColumnsInfo(tableName):
    with getDbConnection() as (conn, cur):
        cur.execute(
            "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = %s",
            (tableName,)
        )
        rows = cur.fetchall()
    info = []
    for row in rows:
        info.append((row[0], row[1], row[2]))
    return info


def _generateValue(dtype, maxLen, columnName, rowIndex):
    if columnName.endswith('_id'):
        return rowIndex + 1
    if 'char' in dtype or 'text' in dtype:
        base = 'val_' + columnName + '_' + str(rowIndex)
        if maxLen is not None:
            if len(base) > int(maxLen):
                base = base[:int(maxLen)]
        return base
    if 'int' in dtype:
        return random.randint(1, 1000)
    if 'numeric' in dtype:
        cents = random.randint(0, 100000)
        return cents / 100.0
    if dtype == 'date':
        return datetime.date.today()
    if dtype.startswith('timestamp'):
        return datetime.datetime.now(datetime.timezone.utc)
    if 'boolean' in dtype:
        if random.randint(0, 1) == 0:
            return False
        return True
    return None


def _generateTableRows(tableName, rowCount):
    columns = _getColumnsInfo(tableName)
    rows = []
    for i in range(rowCount):
        record = []
        for col in columns:
            cname = col[0]
            dtype = col[1]
            mlen = col[2]
            value = _generateValue(dtype, mlen, cname, i)
            record.append(value)
        rows.append(record)
    return rows


def _generateRelatedGroup(tablesSequence, rowCount):
    generated = {}
    for t in tablesSequence:
        if t == 'hall':
            generated['cinema'] = _generateTableRows('cinema', rowCount)
            generated['hall'] = _generateTableRows('hall', rowCount)
        elif t == 'viewer_profile':
            generated['viewer'] = _generateTableRows('viewer', rowCount)
            generated['viewer_profile'] = _generateTableRows('viewer_profile', rowCount)
        elif t == 'favorite_movies':
            generated['viewer'] = _generateTableRows('viewer', rowCount)
            generated['movie'] = _generateTableRows('movie', rowCount)
            generated['favorite_movies'] = _generateTableRows('favorite_movies', rowCount)
        elif t == 'movie_review':
            generated['viewer'] = _generateTableRows('viewer', rowCount)
            generated['movie'] = _generateTableRows('movie', rowCount)
            generated['movie_review'] = _generateTableRows('movie_review', rowCount)
        elif t == 'session':
            generated['movie'] = _generateTableRows('movie', rowCount)
            generated['hall'] = _generateTableRows('hall', rowCount)
            generated['session'] = _generateTableRows('session', rowCount)
    return generated


def _measureGenerateSingle(table, n):
    def run():
        _generateTableRows(table, n)
    arr = timeit.repeat('run()', repeat=REPEATS, number=1, globals=locals())
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def _measureGenerateGroup(sequence, n):
    def run():
        _generateRelatedGroup(sequence, n)
    arr = timeit.repeat('run()', repeat=REPEATS, number=1, globals=locals())
    best = arr[0]
    i = 1
    while i < len(arr):
        if arr[i] < best:
            best = arr[i]
        i = i + 1
    return best


def _saveSingleCharts(resultsSingle, resultsGroups, baseDir, isRaster):
    builder = PlotBuilder(baseDir)
    for table, series in resultsSingle.items():
        xs = []
        ys = []
        for point in series:
            xs.append(point[0])
            ys.append(point[1])
        builder.buildChart({table: (xs, ys)}, 'Generation ' + table, 'Rows', 'Time (s)', 'generation_' + table, isRaster)
    for group, series in resultsGroups.items():
        xs = []
        ys = []
        for point in series:
            xs.append(point[0])
            ys.append(point[1])
        safeName = group.replace(' ', '_')
        builder.buildChart({group: (xs, ys)}, 'Generation group ' + group, 'Rows', 'Time (s)', 'generation_group_' + safeName, isRaster)


def measureGenerationSpeed(
    tablesConfig,
    outputCsvPath,
    outputImagePath=None
):
    selectedTables = []
    if isinstance(tablesConfig, dict):
        for k in tablesConfig:
            if tablesConfig.get(k):
                selectedTables.append(k)
    else:
        for name in tablesConfig:
            selectedTables.append(name)
    rowCounts = [100, 500, 1000, 5000, 10000]
    if isinstance(tablesConfig, dict):
        if 'rowCounts' in tablesConfig:
            rowCounts = tablesConfig['rowCounts']
    fkGroups = []
    if isinstance(tablesConfig, dict):
        if 'fkGroups' in tablesConfig:
            fkGroups = tablesConfig['fkGroups']

    resultsSingle = {}
    for table in selectedTables:
        times = []
        for n in rowCounts:
            elapsed = _measureGenerateSingle(table, n)
            times.append((n, elapsed))
        resultsSingle[table] = times

    resultsGroups = {}
    for idx, groupDef in enumerate(fkGroups):
        name = groupDef.get('name', 'group_' + str(idx))
        sequence = groupDef.get('tables', [])
        times = []
        for n in rowCounts:
            elapsed = _measureGenerateGroup(sequence, n)
            times.append((n, elapsed))
        resultsGroups[name] = times

    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('subject,rows,time_seconds\n')
        for table, series in resultsSingle.items():
            for point in series:
                csvf.write(f"{table},{point[0]},{point[1]:.6f}\n")
        for group, series in resultsGroups.items():
            for point in series:
                csvf.write(f"{group},{point[0]},{point[1]:.6f}\n")

    if outputImagePath:
        outDir = os.path.dirname(outputImagePath)
        if outDir != "" and not os.path.isdir(outDir):
            os.makedirs(outDir, exist_ok=True)
        isRaster = False
        ext = os.path.splitext(outputImagePath)[1].lower()
        if ext in ['.png', '.jpg', '.jpeg']:
            isRaster = True
        _saveSingleCharts(resultsSingle, resultsGroups, outDir, isRaster)
        builder = PlotBuilder(outDir)
        combined = {}
        for table, series in resultsSingle.items():
            xs = []
            ys = []
            for point in series:
                xs.append(point[0])
                ys.append(point[1])
            combined[table] = (xs, ys)
        for group, series in resultsGroups.items():
            xs = []
            ys = []
            for point in series:
                xs.append(point[0])
                ys.append(point[1])
            combined[group] = (xs, ys)
        baseName = os.path.splitext(os.path.basename(outputImagePath))[0]
        builder.buildChart(combined, 'Generation Time (in-memory)', 'Rows', 'Time (s)', baseName, isRaster)
