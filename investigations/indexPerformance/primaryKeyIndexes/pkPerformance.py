import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME, PK_ROW_COUNTS, PK_RUNS, PK_QUERIES_PER_RUN
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

def measurePkIndexEffect(rowCounts, outputDir, rasterFormat):
    print('Исследование эффекта первичного ключа (равенство)', flush=True)
    resultsWithPk = []
    resultsWithoutPk = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('PK INDEX rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)

            totalTimeWithPk = 0.0
            for runIndex in range(PK_RUNS):
                for queryIndex in range(PK_QUERIES_PER_RUN):
                    targetId = random.randint(1, rowCount)

                    def selectWithPk():
                        cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie WHERE movie_id = %s", (targetId,))
                        return cur.fetchall()

                    queryTime, result = measureAverageTime(selectWithPk)
                    totalTimeWithPk += queryTime

            avgTimeWithPk = totalTimeWithPk / (PK_RUNS * PK_QUERIES_PER_RUN)
            resultsWithPk.append((rowCount, avgTimeWithPk))

            cur.execute("CREATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk AS SELECT * FROM " + SANDBOX_SCHEMA_NAME + ".movie")

            totalTimeWithoutPk = 0.0
            for runIndex in range(PK_RUNS):
                for queryIndex in range(PK_QUERIES_PER_RUN):
                    targetId = random.randint(1, rowCount)

                    def selectWithoutPk():
                        cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie_no_pk WHERE movie_id = %s", (targetId,))
                        return cur.fetchall()

                    queryTime, result = measureAverageTime(selectWithoutPk)
                    totalTimeWithoutPk += queryTime

            avgTimeWithoutPk = totalTimeWithoutPk / (PK_RUNS * PK_QUERIES_PER_RUN)
            resultsWithoutPk.append((rowCount, avgTimeWithoutPk))

            cur.execute("DROP TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk")

            print('  с PK:', f'{avgTimeWithPk:.6f}', 'без PK:', f'{avgTimeWithoutPk:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithPk = [result[0] for result in resultsWithPk]
    yValuesWithPk = [result[1] for result in resultsWithPk]
    xValuesWithoutPk = [result[0] for result in resultsWithoutPk]
    yValuesWithoutPk = [result[1] for result in resultsWithoutPk]

    seriesDict = {
        'С первичным ключом': (xValuesWithPk, yValuesWithPk),
        'Без первичного ключа': (xValuesWithoutPk, yValuesWithoutPk)
    }

    plotBuilder.buildChart(seriesDict, 'SELECT по первичному ключу (равенство)', 'Строки', 'Время (с)', 'select_pk', rasterFormat)

    return {
        'with_pk': resultsWithPk,
        'without_pk': resultsWithoutPk
    }

def measurePkInequalityEffect(rowCounts, outputDir, rasterFormat):
    print('Исследование эффекта первичного ключа (неравенство)', flush=True)
    resultsWithPk = []
    resultsWithoutPk = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('PK INEQUALITY rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)

            totalTimeWithPk = 0.0
            for runIndex in range(PK_RUNS):
                for queryIndex in range(PK_QUERIES_PER_RUN):
                    maxId = rowCount // 2

                    def selectInequalityWithPk():
                        cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie WHERE movie_id < %s", (maxId,))
                        return cur.fetchall()

                    queryTime, result = measureAverageTime(selectInequalityWithPk)
                    totalTimeWithPk += queryTime

            avgTimeWithPk = totalTimeWithPk / (PK_RUNS * PK_QUERIES_PER_RUN)
            resultsWithPk.append((rowCount, avgTimeWithPk))

            cur.execute("CREATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk AS SELECT * FROM " + SANDBOX_SCHEMA_NAME + ".movie")

            totalTimeWithoutPk = 0.0
            for runIndex in range(PK_RUNS):
                for queryIndex in range(PK_QUERIES_PER_RUN):
                    maxId = rowCount // 2

                    def selectInequalityWithoutPk():
                        cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie_no_pk WHERE movie_id < %s", (maxId,))
                        return cur.fetchall()

                    queryTime, result = measureAverageTime(selectInequalityWithoutPk)
                    totalTimeWithoutPk += queryTime

            avgTimeWithoutPk = totalTimeWithoutPk / (PK_RUNS * PK_QUERIES_PER_RUN)
            resultsWithoutPk.append((rowCount, avgTimeWithoutPk))

            cur.execute("DROP TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk")

            print('  с PK:', f'{avgTimeWithPk:.6f}', 'без PK:', f'{avgTimeWithoutPk:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithPk = [result[0] for result in resultsWithPk]
    yValuesWithPk = [result[1] for result in resultsWithPk]
    xValuesWithoutPk = [result[0] for result in resultsWithoutPk]
    yValuesWithoutPk = [result[1] for result in resultsWithoutPk]

    seriesDict = {
        'С первичным ключом': (xValuesWithPk, yValuesWithPk),
        'Без первичного ключа': (xValuesWithoutPk, yValuesWithoutPk)
    }

    plotBuilder.buildChart(seriesDict, 'SELECT по первичному ключу (неравенство)', 'Строки', 'Время (с)', 'select_pk_ineq', rasterFormat)

    return {
        'with_pk': resultsWithPk,
        'without_pk': resultsWithoutPk
    }

def measurePkInsertEffect(rowCounts, outputDir, rasterFormat):
    print('Исследование эффекта первичного ключа на INSERT', flush=True)
    resultsWithPk = []
    resultsWithoutPk = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('PK INSERT rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            def insertWithPk():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateMovies(rowCount)
                return rowCount

            avgTimeWithPk, result = measureAverageTime(insertWithPk)
            resultsWithPk.append((rowCount, avgTimeWithPk / rowCount))

            cur.execute("CREATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk (LIKE " + SANDBOX_SCHEMA_NAME + ".movie INCLUDING ALL)")
            cur.execute("ALTER TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk DROP CONSTRAINT IF EXISTS movie_no_pk_pkey")

            def insertWithoutPk():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                for i in range(rowCount):
                    cur.execute("INSERT INTO " + SANDBOX_SCHEMA_NAME + ".movie_no_pk (title, genre, duration_minutes, release_date) VALUES (%s, %s, %s, %s)",
                              ("Movie " + str(i), "Action", 120, "2023-01-01"))
                return rowCount

            avgTimeWithoutPk, result = measureAverageTime(insertWithoutPk)
            resultsWithoutPk.append((rowCount, avgTimeWithoutPk / rowCount))

            cur.execute("DROP TABLE " + SANDBOX_SCHEMA_NAME + ".movie_no_pk")

            print('  с PK:', f'{avgTimeWithPk/rowCount:.6f}', 'без PK:', f'{avgTimeWithoutPk/rowCount:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithPk = [result[0] for result in resultsWithPk]
    yValuesWithPk = [result[1] for result in resultsWithPk]
    xValuesWithoutPk = [result[0] for result in resultsWithoutPk]
    yValuesWithoutPk = [result[1] for result in resultsWithoutPk]

    seriesDict = {
        'С первичным ключом': (xValuesWithPk, yValuesWithPk),
        'Без первичного ключа': (xValuesWithoutPk, yValuesWithoutPk)
    }

    plotBuilder.buildChart(seriesDict, 'INSERT с первичным ключом', 'Строки', 'Время на вставку (с)', 'insert_pk', rasterFormat)

    return {
        'with_pk': resultsWithPk,
        'without_pk': resultsWithoutPk
    }
