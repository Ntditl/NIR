import datetime
import random
import timeit
import os
from lib.db.connection import getDbConnection
from lib.visualization.plots import PlotBuilder

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
    for t in tablesSequence:
        if t == 'viewer_profile':
            generated['viewer'] = _generateTableRows('viewer', rowCount)
            generated['viewer_profile'] = _generateTableRows('viewer_profile', rowCount)
        elif t == 'favorite_movies':
            generated['viewer'] = _generateTableRows('viewer', rowCount)
            generated['movie'] = _generateTableRows('movie', rowCount)
            generated['favorite_movies'] = _generateTableRows('favorite_movies', rowCount)
    return generated


def _measureGenerateSingle(table, n):
    def run():
        _generateTableRows(table, n)
    arr = timeit.repeat('run()', repeat=REPEATS, number=1, globals=locals())
    total = 0.0
    for v in arr:
        total = total + v
    avg = total / len(arr)
    return avg


def _measureGenerateGroup(sequence, n):
    def run():
        _generateRelatedGroup(sequence, n)
    arr = timeit.repeat('run()', repeat=REPEATS, number=1, globals=locals())
    total = 0.0
    for v in arr:
        total = total + v
    avg = total / len(arr)
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
    outputCsvPath,
    outputImagePath=None
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

    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('subject,type,rows,time_seconds\n')
        for table in resultsSingle:
            series = resultsSingle[table]
            for point in series:
                csvf.write(f"{table},single,{point[0]},{point[1]:.6f}\n")
        for group in resultsGroups:
            series = resultsGroups[group]
            for point in series:
                csvf.write(f"{group},fk_group,{point[0]},{point[1]:.6f}\n")

    if outputImagePath:
        outDir = os.path.dirname(outputImagePath)
        if outDir != "" and not os.path.isdir(outDir):
            os.makedirs(outDir, exist_ok=True)
        isRaster = False
        ext = os.path.splitext(outputImagePath)[1].lower()
        if ext in ['.png', '.jpg', '.jpeg']:
            isRaster = True
        # индивидуальные графики отключены
        combined = {}
        for table in resultsSingle:
            series = resultsSingle[table]
            xs = []
            ys = []
            for point in series:
                xs.append(point[0])
                ys.append(point[1])
            combined[table] = (xs, ys)
        for group in resultsGroups:
            series = resultsGroups[group]
            xs = []
            ys = []
            for point in series:
                xs.append(point[0])
                ys.append(point[1])
            combined[group] = (xs, ys)
        allSeries = {}
        for k in resultsSingle:
            allSeries[k] = resultsSingle[k]
        for k in resultsGroups:
            allSeries[k] = resultsGroups[k]
        fast, medium, slow = _categorizeSeries(allSeries)
        _saveCategoryCombined((fast, medium, slow), outDir, isRaster)
        categoriesCsvPath = os.path.join(outDir, CATEGORIES_CSV_NAME)
        _writeCategoriesCsv(categoriesCsvPath, fast, medium, slow)
