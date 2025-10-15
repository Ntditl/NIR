import os
import sys
import time
import csv
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.managers.sandboxManager import SandboxManager
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME

def ensureBaseDataMinimalInSandbox():
    sandboxManager = SandboxManager(SANDBOX_SCHEMA_NAME)
    sandboxManager.ensureMinimalData()

def _executeIndexExperiment(tableName, columnName, indexSql, testFunction, rowCounts, csvFileName, plotTitle):
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print(f'{plotTitle} rowCount {rowCount}', flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.{tableName} RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            if tableName == 'viewer':
                dataGenerator.generateViewers(rowCount)
            elif tableName == 'movie':
                dataGenerator.generateMovies(rowCount)
            elif tableName == 'viewer_profile':
                dataGenerator.generateViewers(rowCount)
                dataGenerator.generateViewerProfiles(rowCount)
            elif tableName == 'favorite_movies':
                dataGenerator.generateViewers(rowCount)
                dataGenerator.generateMovies(rowCount)
                dataGenerator.generateFavoriteMovies(rowCount)

            if indexSql:
                cur.execute(indexSql)

            totalTimeWithIndex = 0.0
            for runIndex in range(RUNS):
                for queryIndex in range(QUERIES_PER_RUN):
                    timeWithIndex = measureAverageTime(lambda: testFunction(cur, rowCount), 1)
                    totalTimeWithIndex += timeWithIndex

            avgTimeWithIndex = totalTimeWithIndex / (RUNS * QUERIES_PER_RUN)

            if indexSql:
                indexNameMatch = columnName + '_idx'
                cur.execute(f"DROP INDEX IF EXISTS {SANDBOX_SCHEMA_NAME}.{indexNameMatch};")

            totalTimeWithoutIndex = 0.0
            for runIndex in range(RUNS):
                for queryIndex in range(QUERIES_PER_RUN):
                    timeWithoutIndex = measureAverageTime(lambda: testFunction(cur, rowCount), 1)
                    totalTimeWithoutIndex += timeWithoutIndex

            avgTimeWithoutIndex = totalTimeWithoutIndex / (RUNS * QUERIES_PER_RUN)

            resultsWithIndex.append([rowCount, avgTimeWithIndex])
            resultsWithoutIndex.append([rowCount, avgTimeWithoutIndex])

    return resultsWithIndex, resultsWithoutIndex

def _saveResults(results1, results2, csvPath, plotPath, title, label1, label2, xLabel):
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write('row_count,with_index,without_index\n')
        for i in range(len(results1)):
            f.write(f"{results1[i][0]},{results1[i][1]},{results2[i][1]}\n")

    if plotPath:
        builder = PlotBuilder()
        builder.setTitle(title)
        builder.setXLabel(xLabel)
        builder.setYLabel('Время выполнения (сек)')

        xValues = [r[0] for r in results1]
        yWith = [r[1] for r in results1]
        yWithout = [r[1] for r in results2]

        builder.addSeries(xValues, yWith, label1)
        builder.addSeries(xValues, yWithout, label2)
        builder.savePlot(plotPath)

def measurePkIndexEffect(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id = {count // 2}")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measurePkInequalityEffect(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < {count // 2}")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measurePkInsertEffect(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            for i in range(100):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) VALUES ('Test', 'Action', 120, '2024-01-01')")
            conn.commit()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Test'")
            conn.commit()
    return results


def measureStringIndexExperiment(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Test Movie'")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measureStringLikePrefix(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title LIKE 'Test%'")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measureStringLikeContains(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title LIKE '%Test%'")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measureStringInsertExperiment(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            for i in range(100):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) VALUES ('String Test', 'Drama', 95, '2024-01-01')")
            conn.commit()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'String Test'")
            conn.commit()
    return results


def measureFtsSingleWordExperiment(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title @@ to_tsquery('test')")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measureFtsMultiWordExperiment(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title @@ to_tsquery('test & movie')")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
    return results


def measureFtsInsertExperiment(rowCounts, resultsDir, savePlot):
    results = []
    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            for i in range(100):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) VALUES ('FTS Test Movie', 'Sci-Fi', 110, '2024-01-01')")
            conn.commit()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'FTS Test Movie'")
            conn.commit()
    return results
