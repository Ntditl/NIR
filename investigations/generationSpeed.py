import datetime
import random
import time
import os
import matplotlib.pyplot as plt
from lib.databaseConnection import getDbConnection


def _getColumnsInfo(tableName):
    with getDbConnection() as (conn, cur):
        cur.execute(
            "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = %s",
            (tableName,)
        )
        rows = cur.fetchall()
    info = []
    i = 0
    while i < len(rows):
        info.append((rows[i][0], rows[i][1], rows[i][2]))
        i = i + 1
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
    i = 0
    while i < rowCount:
        record = []
        j = 0
        while j < len(columns):
            cname = columns[j][0]
            dtype = columns[j][1]
            mlen = columns[j][2]
            value = _generateValue(dtype, mlen, cname, i)
            record.append(value)
            j = j + 1
        rows.append(record)
        i = i + 1
    return rows


def _generateRelatedGroup(tablesSequence, rowCount):
    generated = {}
    idx = 0
    while idx < len(tablesSequence):
        t = tablesSequence[idx]
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
        idx = idx + 1
    return generated


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
        i = 0
        while i < len(tablesConfig):
            selectedTables.append(tablesConfig[i])
            i = i + 1
    rowCounts = [100, 500, 1000, 5000, 10000]
    if isinstance(tablesConfig, dict):
        if 'rowCounts' in tablesConfig:
            rowCounts = tablesConfig['rowCounts']
    fkGroups = []
    if isinstance(tablesConfig, dict):
        if 'fkGroups' in tablesConfig:
            fkGroups = tablesConfig['fkGroups']

    resultsSingle = {}
    i = 0
    while i < len(selectedTables):
        table = selectedTables[i]
        times = []
        j = 0
        while j < len(rowCounts):
            n = rowCounts[j]
            start = time.time()
            _ = _generateTableRows(table, n)
            elapsed = time.time() - start
            times.append((n, elapsed))
            j = j + 1
        resultsSingle[table] = times
        i = i + 1

    resultsGroups = {}
    k = 0
    while k < len(fkGroups):
        group = fkGroups[k]
        name = group.get('name', 'group_' + str(k))
        sequence = group.get('tables', [])
        times = []
        j = 0
        while j < len(rowCounts):
            n = rowCounts[j]
            started = time.time()
            _ = _generateRelatedGroup(sequence, n)
            elapsed = time.time() - started
            times.append((n, elapsed))
            j = j + 1
        resultsGroups[name] = times
        k = k + 1

    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('subject,rows,time_seconds\n')
        for t in resultsSingle:
            series = resultsSingle[t]
            j = 0
            while j < len(series):
                csvf.write(f"{t},{series[j][0]},{series[j][1]:.6f}\n")
                j = j + 1
        for g in resultsGroups:
            series = resultsGroups[g]
            j = 0
            while j < len(series):
                csvf.write(f"{g},{series[j][0]},{series[j][1]:.6f}\n")
                j = j + 1

    if outputImagePath:
        outDir = os.path.dirname(outputImagePath)
        if outDir != "" and not os.path.isdir(outDir):
            os.makedirs(outDir, exist_ok=True)
        plt.figure(figsize=(12, 8))
        for t in resultsSingle:
            xs = []
            ys = []
            j = 0
            series = resultsSingle[t]
            while j < len(series):
                xs.append(series[j][0])
                ys.append(series[j][1])
                j = j + 1
            plt.plot(xs, ys, marker='o', label=t)
        for g in resultsGroups:
            xs = []
            ys = []
            j = 0
            series = resultsGroups[g]
            while j < len(series):
                xs.append(series[j][0])
                ys.append(series[j][1])
                j = j + 1
            plt.plot(xs, ys, marker='s', label=g)
        plt.title('Generation Time (in-memory)')
        plt.xlabel('Rows')
        plt.ylabel('Time (s)')
        plt.grid(True)
        plt.legend()
        plt.savefig(outputImagePath)
        plt.close()
