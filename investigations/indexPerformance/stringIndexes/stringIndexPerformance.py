import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME, STRING_INDEX_ROW_COUNTS, STRING_INDEX_SAMPLE_QUERIES
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

def measureStringIndexExperiment(rowCounts, outputDir, rasterFormat):
    print('Исследование строкового индекса (равенство)', flush=True)
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('STRING INDEX rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)

            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx ON " + SANDBOX_SCHEMA_NAME + ".viewer(email)")

            totalTimeWithIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                targetIndex = random.randint(1, rowCount)
                targetEmail = "user" + str(targetIndex) + "@example.com"

                def selectWithIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email = %s", (targetEmail,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectWithIndex)
                totalTimeWithIndex += queryTime

            avgTimeWithIndex = totalTimeWithIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithIndex.append((rowCount, avgTimeWithIndex))

            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx")

            totalTimeWithoutIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                targetIndex = random.randint(1, rowCount)
                targetEmail = "user" + str(targetIndex) + "@example.com"

                def selectWithoutIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email = %s", (targetEmail,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectWithoutIndex)
                totalTimeWithoutIndex += queryTime

            avgTimeWithoutIndex = totalTimeWithoutIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithoutIndex.append((rowCount, avgTimeWithoutIndex))

            print('  с индексом:', f'{avgTimeWithIndex:.6f}', 'без индекса:', f'{avgTimeWithoutIndex:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithIndex = [result[0] for result in resultsWithIndex]
    yValuesWithIndex = [result[1] for result in resultsWithIndex]
    xValuesWithoutIndex = [result[0] for result in resultsWithoutIndex]
    yValuesWithoutIndex = [result[1] for result in resultsWithoutIndex]

    seriesDict = {
        'С строковым индексом': (xValuesWithIndex, yValuesWithIndex),
        'Без строкового индекса': (xValuesWithoutIndex, yValuesWithoutIndex)
    }

    plotBuilder.buildChart(seriesDict, 'SELECT по строковому полю (равенство)', 'Строки', 'Время (с)', 'select_string_index', rasterFormat)

    return {
        'with_index': resultsWithIndex,
        'without_index': resultsWithoutIndex
    }

def measureStringLikePrefix(rowCounts, outputDir, rasterFormat):
    print('Исследование LIKE с префиксом', flush=True)
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('STRING LIKE PREFIX rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)

            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx ON " + SANDBOX_SCHEMA_NAME + ".viewer(email)")

            totalTimeWithIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                prefixLength = random.randint(3, 8)
                searchPrefix = "user" + str(random.randint(1, 100))[:prefixLength] + "%"

                def selectLikePrefixWithIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email LIKE %s", (searchPrefix,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectLikePrefixWithIndex)
                totalTimeWithIndex += queryTime

            avgTimeWithIndex = totalTimeWithIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithIndex.append((rowCount, avgTimeWithIndex))

            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx")

            totalTimeWithoutIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                prefixLength = random.randint(3, 8)
                searchPrefix = "user" + str(random.randint(1, 100))[:prefixLength] + "%"

                def selectLikePrefixWithoutIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email LIKE %s", (searchPrefix,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectLikePrefixWithoutIndex)
                totalTimeWithoutIndex += queryTime

            avgTimeWithoutIndex = totalTimeWithoutIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithoutIndex.append((rowCount, avgTimeWithoutIndex))

            print('  с индексом:', f'{avgTimeWithIndex:.6f}', 'без индекса:', f'{avgTimeWithoutIndex:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithIndex = [result[0] for result in resultsWithIndex]
    yValuesWithIndex = [result[1] for result in resultsWithIndex]
    xValuesWithoutIndex = [result[0] for result in resultsWithoutIndex]
    yValuesWithoutIndex = [result[1] for result in resultsWithoutIndex]

    seriesDict = {
        'С строковым индексом': (xValuesWithIndex, yValuesWithIndex),
        'Без строкового индекса': (xValuesWithoutIndex, yValuesWithoutIndex)
    }

    plotBuilder.buildChart(seriesDict, 'SELECT LIKE с префиксом', 'Строки', 'Время (с)', 'select_string_like_prefix', rasterFormat)

    return {
        'with_index': resultsWithIndex,
        'without_index': resultsWithoutIndex
    }

def measureStringLikeContains(rowCounts, outputDir, rasterFormat):
    print('Исследование LIKE с содержанием', flush=True)
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('STRING LIKE CONTAINS rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)

            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx ON " + SANDBOX_SCHEMA_NAME + ".viewer(email)")

            totalTimeWithIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                searchSubstring = "%" + str(random.randint(10, 99)) + "%"

                def selectLikeContainsWithIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email LIKE %s", (searchSubstring,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectLikeContainsWithIndex)
                totalTimeWithIndex += queryTime

            avgTimeWithIndex = totalTimeWithIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithIndex.append((rowCount, avgTimeWithIndex))

            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx")

            totalTimeWithoutIndex = 0.0
            for queryIndex in range(STRING_INDEX_SAMPLE_QUERIES):
                searchSubstring = "%" + str(random.randint(10, 99)) + "%"

                def selectLikeContainsWithoutIndex():
                    cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email LIKE %s", (searchSubstring,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectLikeContainsWithoutIndex)
                totalTimeWithoutIndex += queryTime

            avgTimeWithoutIndex = totalTimeWithoutIndex / STRING_INDEX_SAMPLE_QUERIES
            resultsWithoutIndex.append((rowCount, avgTimeWithoutIndex))

            print('  с индексом:', f'{avgTimeWithIndex:.6f}', 'без индекса:', f'{avgTimeWithoutIndex:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithIndex = [result[0] for result in resultsWithIndex]
    yValuesWithIndex = [result[1] for result in resultsWithIndex]
    xValuesWithoutIndex = [result[0] for result in resultsWithoutIndex]
    yValuesWithoutIndex = [result[1] for result in resultsWithoutIndex]

    seriesDict = {
        'С строковым индексом': (xValuesWithIndex, yValuesWithIndex),
        'Без строкового индекса': (xValuesWithoutIndex, yValuesWithoutIndex)
    }

    plotBuilder.buildChart(seriesDict, 'SELECT LIKE с содержанием', 'Строки', 'Время (с)', 'select_string_like_contains', rasterFormat)

    return {
        'with_index': resultsWithIndex,
        'without_index': resultsWithoutIndex
    }

def measureStringInsertExperiment(rowCounts, outputDir, rasterFormat):
    print('Исследование INSERT со строковым индексом', flush=True)
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('STRING INSERT rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx ON " + SANDBOX_SCHEMA_NAME + ".viewer(email)")

            def insertWithIndex():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateViewers(rowCount)
                return rowCount

            avgTimeWithIndex, result = measureAverageTime(insertWithIndex)
            resultsWithIndex.append((rowCount, avgTimeWithIndex / rowCount))

            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_viewer_email_idx")

            def insertWithoutIndex():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateViewers(rowCount)
                return rowCount

            avgTimeWithoutIndex, result = measureAverageTime(insertWithoutIndex)
            resultsWithoutIndex.append((rowCount, avgTimeWithoutIndex / rowCount))

            print('  с индексом:', f'{avgTimeWithIndex/rowCount:.6f}', 'без индекса:', f'{avgTimeWithoutIndex/rowCount:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithIndex = [result[0] for result in resultsWithIndex]
    yValuesWithIndex = [result[1] for result in resultsWithIndex]
    xValuesWithoutIndex = [result[0] for result in resultsWithoutIndex]
    yValuesWithoutIndex = [result[1] for result in resultsWithoutIndex]

    seriesDict = {
        'С строковым индексом': (xValuesWithIndex, yValuesWithIndex),
        'Без строкового индекса': (xValuesWithoutIndex, yValuesWithoutIndex)
    }

    plotBuilder.buildChart(seriesDict, 'INSERT со строковым индексом', 'Строки', 'Время на вставку (с)', 'insert_string_index', rasterFormat)

    return {
        'with_index': resultsWithIndex,
        'without_index': resultsWithoutIndex
    }
