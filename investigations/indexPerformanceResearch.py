import os
import sys
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.managers.sandboxManager import SandboxManager
from lib.visualization.plots import PlotBuilder
from lib.utils.timing import measureAverageTime
from investigations.researchUtils import SANDBOX_SCHEMA_NAME

RUNS = 1
QUERIES_PER_RUN = 10


def _createTableCopyWithoutPk(sourceTable, targetTable):
    with getDbConnection() as (conn, cur):
        cur.execute(f"DROP TABLE IF EXISTS {SANDBOX_SCHEMA_NAME}.{targetTable} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {SANDBOX_SCHEMA_NAME}.{targetTable} AS 
            SELECT * FROM {SANDBOX_SCHEMA_NAME}.{sourceTable}
        """)
        print(f"Создана копия таблицы {targetTable} без первичного ключа", flush=True)


def _createTableCopyWithoutIndex(sourceTable, targetTable, indexName):
    with getDbConnection() as (conn, cur):
        cur.execute(f"DROP TABLE IF EXISTS {SANDBOX_SCHEMA_NAME}.{targetTable} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {SANDBOX_SCHEMA_NAME}.{targetTable} AS 
            SELECT * FROM {SANDBOX_SCHEMA_NAME}.{sourceTable}
        """)
        cur.execute(f"DROP INDEX IF EXISTS {SANDBOX_SCHEMA_NAME}.{indexName};")
        print(f"Создана копия таблицы {targetTable} без индекса {indexName}", flush=True)


def _fillTableWithData(tableName, rowCount):
    print(f'  [LOG] _fillTableWithData: начало для {tableName}, rowCount={rowCount}', flush=True)

    with getDbConnection() as (conn, cur):
        print(f'  [LOG] _fillTableWithData: проверка существования таблицы', flush=True)
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{SANDBOX_SCHEMA_NAME}' 
                AND table_name = '{tableName}'
            )
        """)
        tableExists = cur.fetchone()[0]
        print(f'  [LOG] _fillTableWithData: таблица {tableName} существует={tableExists}', flush=True)

        if tableExists:
            print(f'  [LOG] _fillTableWithData: выполняю TRUNCATE для {tableName}', flush=True)
            cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.{tableName} RESTART IDENTITY CASCADE;")
            print(f'  [LOG] _fillTableWithData: TRUNCATE выполнен и закоммичен', flush=True)

    print(f'  [LOG] _fillTableWithData: создаю генератор данных', flush=True)
    dataGenerator = RandomDataGenerator()
    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
    print(f'  [LOG] _fillTableWithData: генератор создан, начинаю генерацию данных', flush=True)

    if tableName == 'movie' or tableName.startswith('movie_'):
        print(f'  [LOG] _fillTableWithData: вызываю generateMovies({rowCount})', flush=True)
        dataGenerator.generateMovies(rowCount)
        print(f'  [LOG] _fillTableWithData: generateMovies завершен', flush=True)
    elif tableName == 'viewer':
        print(f'  [LOG] _fillTableWithData: вызываю generateViewers({rowCount})', flush=True)
        dataGenerator.generateViewers(rowCount)
        print(f'  [LOG] _fillTableWithData: generateViewers завершен', flush=True)
    elif tableName == 'viewer_profile':
        print(f'  [LOG] _fillTableWithData: вызываю generateViewers и generateViewerProfiles', flush=True)
        dataGenerator.generateViewers(rowCount)
        dataGenerator.generateViewerProfiles(rowCount)
        print(f'  [LOG] _fillTableWithData: генерация завершена', flush=True)

    print(f"Таблица {tableName} заполнена {rowCount} строками", flush=True)


def measurePkIndexEffect(rowCounts, resultsDir, savePlot):
    tableWithPk = 'movie'
    tableNoPk = 'movie_no_pk'

    resultsWithPk = []
    resultsNoPk = []

    for rowCount in rowCounts:
        print(f'PK индекс: тест для {rowCount} строк', flush=True)

        print(f'[LOG] measurePkIndexEffect: вызываю _fillTableWithData', flush=True)
        _fillTableWithData(tableWithPk, rowCount)
        print(f'[LOG] measurePkIndexEffect: _fillTableWithData завершен', flush=True)

        print(f'[LOG] measurePkIndexEffect: вызываю _createTableCopyWithoutPk', flush=True)
        _createTableCopyWithoutPk(tableWithPk, tableNoPk)
        print(f'[LOG] measurePkIndexEffect: _createTableCopyWithoutPk завершен', flush=True)

        totalTimeWithPk = 0.0
        totalTimeNoPk = 0.0

        totalQueries = RUNS * QUERIES_PER_RUN
        print(f'  Выполняем {totalQueries} запросов ({RUNS} раундов по {QUERIES_PER_RUN} запросов)...', flush=True)

        for runIndex in range(RUNS):
            print(f'  Раунд {runIndex + 1}/{RUNS}...', flush=True)

            for queryIndex in range(QUERIES_PER_RUN):
                if queryIndex % 20 == 0:
                    print(f'    [LOG] запрос {queryIndex + 1}/{QUERIES_PER_RUN}', flush=True)

                def queryWithPk():
                    searchId = random.randint(1, rowCount)
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithPk} WHERE movie_id = {searchId}")
                        rows = cur.fetchall()

                timeWithPk = measureAverageTime(queryWithPk, repeats=1)
                totalTimeWithPk += timeWithPk

                def queryNoPk():
                    searchId = random.randint(1, rowCount)
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoPk} WHERE movie_id = {searchId}")
                        rows = cur.fetchall()

                timeNoPk = measureAverageTime(queryNoPk, repeats=1)
                totalTimeNoPk += timeNoPk

        avgTimeWithPk = totalTimeWithPk / totalQueries
        avgTimeNoPk = totalTimeNoPk / totalQueries

        resultsWithPk.append({'count': rowCount, 'time': avgTimeWithPk})
        resultsNoPk.append({'count': rowCount, 'time': avgTimeNoPk})

        print(f'  С PK: {avgTimeWithPk:.6f} сек, Без PK: {avgTimeNoPk:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'pk_equality_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_pk,without_pk\n')
            for i in range(len(resultsWithPk)):
                f.write(f"{resultsWithPk[i]['count']},{resultsWithPk[i]['time']},{resultsNoPk[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithPk]
        yWithPk = [r['time'] for r in resultsWithPk]
        yNoPk = [r['time'] for r in resultsNoPk]
        builder.buildChart(
            {'С первичным ключом': (xValues, yWithPk), 'Без первичного ключа': (xValues, yNoPk)},
            'SELECT WHERE по PK (равенство)',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'pk_equality_comparison',
            True
        )

    return resultsWithPk


def measurePkInequalityEffect(rowCounts, resultsDir, savePlot):
    tableWithPk = 'movie'
    tableNoPk = 'movie_no_pk'

    resultsWithPk = []
    resultsNoPk = []

    for rowCount in rowCounts:
        print(f'PK индекс (неравенство): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithPk, rowCount)
        _createTableCopyWithoutPk(tableWithPk, tableNoPk)

        totalTimeWithPk = 0.0
        totalTimeNoPk = 0.0

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                threshold = random.randint(1, rowCount)

                def queryWithPk():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithPk} WHERE movie_id < {threshold}")
                        rows = cur.fetchall()

                timeWithPk = measureAverageTime(queryWithPk, repeats=1)
                totalTimeWithPk += timeWithPk

                def queryNoPk():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoPk} WHERE movie_id < {threshold}")
                        rows = cur.fetchall()

                timeNoPk = measureAverageTime(queryNoPk, repeats=1)
                totalTimeNoPk += timeNoPk

        avgTimeWithPk = totalTimeWithPk / (RUNS * QUERIES_PER_RUN)
        avgTimeNoPk = totalTimeNoPk / (RUNS * QUERIES_PER_RUN)

        resultsWithPk.append({'count': rowCount, 'time': avgTimeWithPk})
        resultsNoPk.append({'count': rowCount, 'time': avgTimeNoPk})

        print(f'  С PK: {avgTimeWithPk:.6f} сек, Без PK: {avgTimeNoPk:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'pk_inequality_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_pk,without_pk\n')
            for i in range(len(resultsWithPk)):
                f.write(f"{resultsWithPk[i]['count']},{resultsWithPk[i]['time']},{resultsNoPk[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithPk]
        yWithPk = [r['time'] for r in resultsWithPk]
        yNoPk = [r['time'] for r in resultsNoPk]
        builder.buildChart(
            {'С первичным ключом': (xValues, yWithPk), 'Без первичного ключа': (xValues, yNoPk)},
            'SELECT WHERE по PK (неравенство)',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'pk_inequality_comparison',
            True
        )

    return resultsWithPk


def measurePkInsertEffect(rowCounts, resultsDir, savePlot):
    tableWithPk = 'movie'
    tableNoPk = 'movie_no_pk'

    resultsWithPk = []
    resultsNoPk = []

    insertCount = 100

    for rowCount in rowCounts:
        print(f'PK индекс (INSERT): тест для {rowCount} строк в таблице', flush=True)

        _fillTableWithData(tableWithPk, rowCount)
        _createTableCopyWithoutPk(tableWithPk, tableNoPk)

        totalTimeWithPk = 0.0
        totalTimeNoPk = 0.0

        for runIndex in range(RUNS):
            def insertWithPk():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableWithPk} (title, genre, duration_minutes, release_date) VALUES ('Test Movie', 'Action', 120, '2024-01-01')")

            timeWithPk = measureAverageTime(insertWithPk, repeats=1)
            totalTimeWithPk += timeWithPk

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableWithPk} WHERE title = 'Test Movie'")

            def insertNoPk():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableNoPk} (title, genre, duration_minutes, release_date) VALUES ('Test Movie', 'Action', 120, '2024-01-01')")

            timeNoPk = measureAverageTime(insertNoPk, repeats=1)
            totalTimeNoPk += timeNoPk

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableNoPk} WHERE title = 'Test Movie'")

        avgTimeWithPk = totalTimeWithPk / RUNS
        avgTimeNoPk = totalTimeNoPk / RUNS

        resultsWithPk.append({'count': rowCount, 'time': avgTimeWithPk})
        resultsNoPk.append({'count': rowCount, 'time': avgTimeNoPk})

        print(f'  С PK: {avgTimeWithPk:.6f} сек, Без PK: {avgTimeNoPk:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'pk_insert_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_pk,without_pk\n')
            for i in range(len(resultsWithPk)):
                f.write(f"{resultsWithPk[i]['count']},{resultsWithPk[i]['time']},{resultsNoPk[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithPk]
        yWithPk = [r['time'] for r in resultsWithPk]
        yNoPk = [r['time'] for r in resultsNoPk]
        builder.buildChart(
            {'С первичным ключом': (xValues, yWithPk), 'Без первичного ключа': (xValues, yNoPk)},
            f'INSERT {insertCount} строк',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'pk_insert_comparison',
            True
        )

    return resultsWithPk


def measureStringIndexExperiment(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_string_idx'
    indexName = 'movie_title_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    for rowCount in rowCounts:
        print(f'Строковый индекс (равенство): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx}(title);")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        sampleTitles = []
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT title FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} LIMIT 10")
            for row in cur.fetchall():
                sampleTitles.append(row[0])

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                titleIndex = queryIndex % len(sampleTitles)

                def queryWithIdx():
                    searchTitle = sampleTitles[titleIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE title = %s", (searchTitle,))
                        rows = cur.fetchall()

                timeWithIdx = measureAverageTime(queryWithIdx, repeats=1)
                totalTimeWithIdx += timeWithIdx

                def queryNoIdx():
                    searchTitle = sampleTitles[titleIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE title = %s", (searchTitle,))
                        rows = cur.fetchall()

                timeNoIdx = measureAverageTime(queryNoIdx, repeats=1)
                totalTimeNoIdx += timeNoIdx

        avgTimeWithIdx = totalTimeWithIdx / (RUNS * QUERIES_PER_RUN)
        avgTimeNoIdx = totalTimeNoIdx / (RUNS * QUERIES_PER_RUN)

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С индексом: {avgTimeWithIdx:.6f} сек, Без индекса: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'string_equality_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_index,without_index\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'Со строковым индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            'SELECT WHERE по строке (равенство)',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'string_equality_comparison',
            True
        )

    return resultsWithIdx


def measureStringLikePrefix(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_string_idx'
    indexName = 'movie_title_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    for rowCount in rowCounts:
        print(f'Строковый индекс (LIKE prefix%): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx}(title);")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        prefixes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                prefix = prefixes[queryIndex % len(prefixes)]

                def queryWithIdx():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE title LIKE %s", (prefix + '%',))
                        rows = cur.fetchall()

                timeWithIdx = measureAverageTime(queryWithIdx, repeats=1)
                totalTimeWithIdx += timeWithIdx

                def queryNoIdx():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE title LIKE %s", (prefix + '%',))
                        rows = cur.fetchall()

                timeNoIdx = measureAverageTime(queryNoIdx, repeats=1)
                totalTimeNoIdx += timeNoIdx

        avgTimeWithIdx = totalTimeWithIdx / (RUNS * QUERIES_PER_RUN)
        avgTimeNoIdx = totalTimeNoIdx / (RUNS * QUERIES_PER_RUN)

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С индексом: {avgTimeWithIdx:.6f} сек, Без индекса: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'string_like_prefix_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_index,without_index\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'Со строковым индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            'SELECT WHERE LIKE prefix%',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'string_like_prefix_comparison',
            True
        )

    return resultsWithIdx


def measureStringLikeContains(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_string_idx'
    indexName = 'movie_title_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    for rowCount in rowCounts:
        print(f'Строковый индекс (LIKE %substring%): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx}(title);")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        substrings = ['test', 'movie', 'data', 'film', 'story']

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                substringIndex = queryIndex % len(substrings)

                def queryWithIdx():
                    substring = substrings[substringIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE title LIKE %s", ('%' + substring + '%',))
                        rows = cur.fetchall()

                timeWithIdx = measureAverageTime(queryWithIdx, repeats=1)
                totalTimeWithIdx += timeWithIdx

                def queryNoIdx():
                    substring = substrings[substringIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE title LIKE %s", ('%' + substring + '%',))
                        rows = cur.fetchall()

                timeNoIdx = measureAverageTime(queryNoIdx, repeats=1)
                totalTimeNoIdx += timeNoIdx

        avgTimeWithIdx = totalTimeWithIdx / (RUNS * QUERIES_PER_RUN)
        avgTimeNoIdx = totalTimeNoIdx / (RUNS * QUERIES_PER_RUN)

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С индексом: {avgTimeWithIdx:.6f} сек, Без индекса: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'string_like_contains_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_index,without_index\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'Со строковым индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            'SELECT WHERE LIKE %substring%',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'string_like_contains_comparison',
            True
        )

    return resultsWithIdx


def measureStringInsertExperiment(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_string_idx'
    indexName = 'movie_title_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    insertCount = 100

    for rowCount in rowCounts:
        print(f'Строковый индекс (INSERT): тест для {rowCount} строк в таблице', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx}(title);")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        for runIndex in range(RUNS):
            def insertWithIdx():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableWithIdx} (title, genre, duration_minutes, release_date) VALUES ('String Test Movie', 'Drama', 95, '2024-01-01')")

            timeWithIdx = measureAverageTime(insertWithIdx, repeats=1)
            totalTimeWithIdx += timeWithIdx

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE title = 'String Test Movie'")

            def insertNoIdx():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableNoIdx} (title, genre, duration_minutes, release_date) VALUES ('String Test Movie', 'Drama', 95, '2024-01-01')")

            timeNoIdx = measureAverageTime(insertNoIdx, repeats=1)
            totalTimeNoIdx += timeNoIdx

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE title = 'String Test Movie'")

        avgTimeWithIdx = totalTimeWithIdx / RUNS
        avgTimeNoIdx = totalTimeNoIdx / RUNS

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С индексом: {avgTimeWithIdx:.6f} сек, Без индекса: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'string_insert_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_index,without_index\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'Со строковым индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            f'INSERT {insertCount} строк',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'string_insert_comparison',
            True
        )

    return resultsWithIdx


def measureFtsSingleWordExperiment(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_fts_idx'
    indexName = 'movie_description_fts_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    for rowCount in rowCounts:
        print(f'FTS индекс (одно слово): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx} USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')));")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        searchWords = ['action', 'drama', 'story', 'hero', 'adventure']

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                word = searchWords[queryIndex % len(searchWords)]

                def queryWithIdx():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')) @@ to_tsquery(%s)", (word,))
                        rows = cur.fetchall()

                timeWithIdx = measureAverageTime(queryWithIdx, repeats=1)
                totalTimeWithIdx += timeWithIdx

                def queryNoIdx():
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')) @@ to_tsquery(%s)", (word,))
                        rows = cur.fetchall()

                timeNoIdx = measureAverageTime(queryNoIdx, repeats=1)
                totalTimeNoIdx += timeNoIdx

        avgTimeWithIdx = totalTimeWithIdx / (RUNS * QUERIES_PER_RUN)
        avgTimeNoIdx = totalTimeNoIdx / (RUNS * QUERIES_PER_RUN)

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С FTS: {avgTimeWithIdx:.6f} сек, Без FTS: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'fts_single_word_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_fts,without_fts\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'С FTS индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            'Полнотекстовый поиск (одно слово)',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'fts_single_word_comparison',
            True
        )

    return resultsWithIdx


def measureFtsMultiWordExperiment(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_fts_idx'
    indexName = 'movie_description_fts_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    for rowCount in rowCounts:
        print(f'FTS индекс (несколько слов): тест для {rowCount} строк', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx} USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')));")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        searchPhrases = ['action & hero', 'drama & story', 'adventure & journey', 'love & story', 'crime & detective']

        for runIndex in range(RUNS):
            for queryIndex in range(QUERIES_PER_RUN):
                phraseIndex = queryIndex % len(searchPhrases)

                def queryWithIdx():
                    phrase = searchPhrases[phraseIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')) @@ to_tsquery(%s)", (phrase,))
                        rows = cur.fetchall()

                timeWithIdx = measureAverageTime(queryWithIdx, repeats=1)
                totalTimeWithIdx += timeWithIdx

                def queryNoIdx():
                    phrase = searchPhrases[phraseIndex]
                    with getDbConnection() as (conn, cur):
                        cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')) @@ to_tsquery(%s)", (phrase,))
                        rows = cur.fetchall()

                timeNoIdx = measureAverageTime(queryNoIdx, repeats=1)
                totalTimeNoIdx += timeNoIdx

        avgTimeWithIdx = totalTimeWithIdx / (RUNS * QUERIES_PER_RUN)
        avgTimeNoIdx = totalTimeNoIdx / (RUNS * QUERIES_PER_RUN)

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С FTS: {avgTimeWithIdx:.6f} сек, Без FTS: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'fts_multi_word_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_fts,without_fts\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'С FTS индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            'Полнотекстовый поиск (несколько слов)',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'fts_multi_word_comparison',
            True
        )

    return resultsWithIdx


def measureFtsInsertExperiment(rowCounts, resultsDir, savePlot):
    tableWithIdx = 'movie'
    tableNoIdx = 'movie_no_fts_idx'
    indexName = 'movie_description_fts_idx'

    resultsWithIdx = []
    resultsNoIdx = []

    insertCount = 100

    for rowCount in rowCounts:
        print(f'FTS индекс (INSERT): тест для {rowCount} строк в таблице', flush=True)

        _fillTableWithData(tableWithIdx, rowCount)

        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {indexName} ON {SANDBOX_SCHEMA_NAME}.{tableWithIdx} USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(genre, '')));")

        _createTableCopyWithoutIndex(tableWithIdx, tableNoIdx, indexName)

        totalTimeWithIdx = 0.0
        totalTimeNoIdx = 0.0

        for runIndex in range(RUNS):
            def insertWithIdx():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableWithIdx} (title, genre, duration_minutes, release_date) VALUES ('FTS Test', 'Sci-Fi', 110, '2024-01-01')")

            timeWithIdx = measureAverageTime(insertWithIdx, repeats=1)
            totalTimeWithIdx += timeWithIdx

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableWithIdx} WHERE title = 'FTS Test'")

            def insertNoIdx():
                with getDbConnection() as (conn, cur):
                    for i in range(insertCount):
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.{tableNoIdx} (title, genre, duration_minutes, release_date) VALUES ('FTS Test', 'Sci-Fi', 110, '2024-01-01')")

            timeNoIdx = measureAverageTime(insertNoIdx, repeats=1)
            totalTimeNoIdx += timeNoIdx

            with getDbConnection() as (conn, cur):
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.{tableNoIdx} WHERE title = 'FTS Test'")

        avgTimeWithIdx = totalTimeWithIdx / RUNS
        avgTimeNoIdx = totalTimeNoIdx / RUNS

        resultsWithIdx.append({'count': rowCount, 'time': avgTimeWithIdx})
        resultsNoIdx.append({'count': rowCount, 'time': avgTimeNoIdx})

        print(f'  С FTS: {avgTimeWithIdx:.6f} сек, Без FTS: {avgTimeNoIdx:.6f} сек', flush=True)

    if savePlot:
        csvPath = os.path.join(resultsDir, 'fts_insert_comparison.csv')
        with open(csvPath, 'w', encoding='utf-8') as f:
            f.write('row_count,with_fts,without_fts\n')
            for i in range(len(resultsWithIdx)):
                f.write(f"{resultsWithIdx[i]['count']},{resultsWithIdx[i]['time']},{resultsNoIdx[i]['time']}\n")

        builder = PlotBuilder(resultsDir)
        xValues = [r['count'] for r in resultsWithIdx]
        yWithIdx = [r['time'] for r in resultsWithIdx]
        yNoIdx = [r['time'] for r in resultsNoIdx]
        builder.buildChart(
            {'С FTS индексом': (xValues, yWithIdx), 'Без индекса': (xValues, yNoIdx)},
            f'INSERT {insertCount} строк',
            'Количество строк в таблице',
            'Время выполнения (сек)',
            'fts_insert_comparison',
            True
        )

    return resultsWithIdx
