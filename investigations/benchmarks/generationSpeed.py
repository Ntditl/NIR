import datetime
import random
import os
from lib.db.connection import getDbConnection
from lib.visualization.plots import PlotBuilder
from lib.utils.timing import measureExecutionTime

REPEATS = 3
FAST_THRESHOLD = 0.01
MEDIUM_THRESHOLD = 0.1
CATEGORIES_CSV_NAME = 'generation_speed_categories.csv'


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
    for tableName in tablesSequence:
        generated[tableName] = _generateTableRows(tableName, rowCount)
    return generated


def _measureGenerateSingle(table, n):
    def run():
        _generateTableRows(table, n)
    times = []
    for _ in range(REPEATS):
        times.append(measureExecutionTime(run))
    total = 0.0
    for v in times:
        total = total + v
    avg = total / len(times)
    return avg


def _measureGenerateGroup(sequence, n):
    def run():
        _generateRelatedGroup(sequence, n)
    times = []
    for _ in range(REPEATS):
        times.append(measureExecutionTime(run))
    total = 0.0
    for v in times:
        total = total + v
    avg = total / len(times)
    return avg




def _categorizeSeries(allSeries):
    fast = {}
    medium = {}
    slow = {}
    for name in allSeries:
        series = allSeries[name]
        lastPoint = series[len(series) - 1]
        characteristicTime = lastPoint[1]
        if characteristicTime <= FAST_THRESHOLD:
            fast[name] = series
        elif characteristicTime <= MEDIUM_THRESHOLD:
            medium[name] = series
        else:
            slow[name] = series
    return fast, medium, slow


def _saveCategoryCombined(categories, outDir, isRaster):
    builder = PlotBuilder(outDir)
    fast = categories[0]
    medium = categories[1]
    slow = categories[2]
    if len(medium) > 0:
        builder.buildChart(_convertSeriesDict(medium), 'Графики времени генерации данных для всех таблиц', 'Строки', 'Время (с)', 'generation_speed_medium', isRaster)
    if len(slow) > 0:
        builder.buildChart(_convertSeriesDict(slow), 'Графики времени генерации данных для всех таблиц', 'Строки', 'Время (с)', 'generation_speed_slow', isRaster)


def _convertSeriesDict(seriesDict):
    converted = {}
    for name in seriesDict:
        series = seriesDict[name]
        xs = []
        ys = []
        for point in series:
            xs.append(point[0])
            ys.append(point[1])
        converted[name] = (xs, ys)
    return converted


def _writeCategoriesCsv(path, fast, medium, slow):
    with open(path, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('subject,category,rows,time_seconds\n')
        for name in fast:
            series = fast[name]
            for point in series:
                csvf.write(f"{name},fast,{point[0]},{point[1]:.6f}\n")
        for name in medium:
            series = medium[name]
            for point in series:
                csvf.write(f"{name},medium,{point[0]},{point[1]:.6f}\n")
        for name in slow:
            series = slow[name]
            for point in series:
                csvf.write(f"{name},slow,{point[0]},{point[1]:.6f}\n")


def measureGenerationSpeed(
    tablesConfig,
    outputSingleDir,
    outputFkGroupsDir
):
    selectedTables = []
    if isinstance(tablesConfig, dict):
        for k in tablesConfig:
            if k != 'rowCounts' and k != 'fkGroups':
                if tablesConfig.get(k):
                    selectedTables.append(k)
    else:
        for name in tablesConfig:
            selectedTables.append(name)

    rowCounts = tablesConfig['rowCounts']
    fkGroups = tablesConfig.get('fkGroups', [])

    print('Старт измерений генерации. Таблиц', len(selectedTables), 'групп', len(fkGroups), 'ряды', rowCounts, flush=True)

    resultsSingle = {}
    for table in selectedTables:
        print('Таблица', table, 'замер начинается', flush=True)
        times = []
        for n in rowCounts:
            print('  rows', n, '→', 'начало', flush=True)
            elapsed = _measureGenerateSingle(table, n)
            print('  rows', n, 'готово среднее время', f'{elapsed:.6f}', flush=True)
            times.append((n, elapsed))
        resultsSingle[table] = times
        print('Таблица', table, 'завершена', flush=True)

    resultsGroups = {}
    for idx, groupDef in enumerate(fkGroups):
        name = groupDef.get('name', 'group_' + str(idx))
        sequence = groupDef.get('tables', [])
        print('Группа', name, 'таблиц', sequence, 'замер начинается', flush=True)
        times = []
        for n in rowCounts:
            print('  rows', n, '→', 'начало', flush=True)
            elapsed = _measureGenerateGroup(sequence, n)
            print('  rows', n, 'готово среднее время', f'{elapsed:.6f}', flush=True)
            times.append((n, elapsed))
        resultsGroups[name] = times
        print('Группа', name, 'завершена', flush=True)

    if not os.path.isdir(outputSingleDir):
        os.makedirs(outputSingleDir, exist_ok=True)
    if not os.path.isdir(outputFkGroupsDir):
        os.makedirs(outputFkGroupsDir, exist_ok=True)

    singleCsvPath = os.path.join(outputSingleDir, 'generation_speed_single.csv')
    with open(singleCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('table,rows,time_seconds\n')
        for table in resultsSingle:
            series = resultsSingle[table]
            for point in series:
                csvf.write(f"{table},{point[0]},{point[1]:.6f}\n")

    fkGroupsCsvPath = os.path.join(outputFkGroupsDir, 'generation_speed_fk_groups.csv')
    with open(fkGroupsCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('group,rows,time_seconds\n')
        for group in resultsGroups:
            series = resultsGroups[group]
            for point in series:
                csvf.write(f"{group},{point[0]},{point[1]:.6f}\n")

    singleImagePath = os.path.join(outputSingleDir, 'generation_speed_single.png')
    allSingleSeries = {}
    for k in resultsSingle:
        allSingleSeries[k] = resultsSingle[k]
    fast, medium, slow = _categorizeSeries(allSingleSeries)
    _saveCategoryCombined((fast, medium, slow), outputSingleDir, True)
    categoriesCsvPath = os.path.join(outputSingleDir, CATEGORIES_CSV_NAME)
    _writeCategoriesCsv(categoriesCsvPath, fast, medium, slow)

    fkGroupsImagePath = os.path.join(outputFkGroupsDir, 'generation_speed_fk_groups.png')
    if len(resultsGroups) > 0:
        allFkSeries = {}
        for k in resultsGroups:
            allFkSeries[k] = resultsGroups[k]
        fast, medium, slow = _categorizeSeries(allFkSeries)
        _saveCategoryCombined((fast, medium, slow), outputFkGroupsDir, True)
        categoriesCsvPath = os.path.join(outputFkGroupsDir, CATEGORIES_CSV_NAME)
        _writeCategoriesCsv(categoriesCsvPath, fast, medium, slow)
