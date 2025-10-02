import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME, FTS_ROW_COUNTS, FTS_SAMPLE_QUERIES, FTS_MULTI_SAMPLE_QUERIES, FTS_DICTIONARY
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

def measureFtsSingleWordExperiment(rowCounts, outputDir, rasterFormat):
    print('Исследование полнотекстового поиска (одно слово)', flush=True)
    resultsWithFts = []
    resultsWithoutFts = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('FTS SINGLE WORD rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovieReviews(rowCount)

            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx ON " + SANDBOX_SCHEMA_NAME + ".movie_review USING gin(to_tsvector('english', review_text))")

            totalTimeWithFts = 0.0
            for queryIndex in range(FTS_SAMPLE_QUERIES):
                searchWord = random.choice(FTS_DICTIONARY)

                def selectFtsSingleWord():
                    cur.execute("SELECT review_id, review_text FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE to_tsvector('english', review_text) @@ to_tsquery('english', %s)", (searchWord,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectFtsSingleWord)
                totalTimeWithFts += queryTime

            avgTimeWithFts = totalTimeWithFts / FTS_SAMPLE_QUERIES
            resultsWithFts.append((rowCount, avgTimeWithFts))

            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx")

            totalTimeWithoutFts = 0.0
            for queryIndex in range(FTS_SAMPLE_QUERIES):
                searchWord = random.choice(FTS_DICTIONARY)

                def selectWithoutFtsSingleWord():
                    cur.execute("SELECT review_id, review_text FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE review_text ILIKE %s", ('%' + searchWord + '%',))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectWithoutFtsSingleWord)
                totalTimeWithoutFts += queryTime

            avgTimeWithoutFts = totalTimeWithoutFts / FTS_SAMPLE_QUERIES
            resultsWithoutFts.append((rowCount, avgTimeWithoutFts))

            print('  с FTS:', f'{avgTimeWithFts:.6f}', 'без FTS:', f'{avgTimeWithoutFts:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithFts = [result[0] for result in resultsWithFts]
    yValuesWithFts = [result[1] for result in resultsWithFts]
    xValuesWithoutFts = [result[0] for result in resultsWithoutFts]
    yValuesWithoutFts = [result[1] for result in resultsWithoutFts]

    seriesDict = {
        'С полнотекстовым индексом': (xValuesWithFts, yValuesWithFts),
        'Без полнотекстового индекса': (xValuesWithoutFts, yValuesWithoutFts)
    }

    plotBuilder.buildChart(seriesDict, 'Полнотекстовый поиск (одно слово)', 'Строки', 'Время (с)', 'select_fts_single', rasterFormat)

    return {
        'with_fts': resultsWithFts,
        'without_fts': resultsWithoutFts
    }

def measureFtsMultiWordExperiment(rowCounts, outputDir, rasterFormat):
    print('Исследование полнотекстового поиска (несколько слов)', flush=True)
    resultsWithFts = []
    resultsWithoutFts = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('FTS MULTI WORD rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovieReviews(rowCount)

            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx ON " + SANDBOX_SCHEMA_NAME + ".movie_review USING gin(to_tsvector('english', review_text))")

            totalTimeWithFts = 0.0
            for queryIndex in range(FTS_MULTI_SAMPLE_QUERIES):
                wordCount = random.randint(2, 4)
                searchWords = []
                for i in range(wordCount):
                    searchWords.append(random.choice(FTS_DICTIONARY))
                searchQuery = ' & '.join(searchWords)

                def selectFtsMultiWord():
                    cur.execute("SELECT review_id, review_text FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE to_tsvector('english', review_text) @@ to_tsquery('english', %s)", (searchQuery,))
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectFtsMultiWord)
                totalTimeWithFts += queryTime

            avgTimeWithFts = totalTimeWithFts / FTS_MULTI_SAMPLE_QUERIES
            resultsWithFts.append((rowCount, avgTimeWithFts))

            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx")

            totalTimeWithoutFts = 0.0
            for queryIndex in range(FTS_MULTI_SAMPLE_QUERIES):
                wordCount = random.randint(2, 4)
                searchWords = []
                for i in range(wordCount):
                    searchWords.append(random.choice(FTS_DICTIONARY))

                def selectWithoutFtsMultiWord():
                    whereConditions = []
                    queryParams = []
                    for word in searchWords:
                        whereConditions.append("review_text ILIKE %s")
                        queryParams.append('%' + word + '%')
                    whereClause = ' AND '.join(whereConditions)
                    cur.execute("SELECT review_id, review_text FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE " + whereClause, queryParams)
                    return cur.fetchall()

                queryTime, result = measureAverageTime(selectWithoutFtsMultiWord)
                totalTimeWithoutFts += queryTime

            avgTimeWithoutFts = totalTimeWithoutFts / FTS_MULTI_SAMPLE_QUERIES
            resultsWithoutFts.append((rowCount, avgTimeWithoutFts))

            print('  с FTS:', f'{avgTimeWithFts:.6f}', 'без FTS:', f'{avgTimeWithoutFts:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithFts = [result[0] for result in resultsWithFts]
    yValuesWithFts = [result[1] for result in resultsWithFts]
    xValuesWithoutFts = [result[0] for result in resultsWithoutFts]
    yValuesWithoutFts = [result[1] for result in resultsWithoutFts]

    seriesDict = {
        'С полнотекстовым индексом': (xValuesWithFts, yValuesWithFts),
        'Без полнотекстового индекса': (xValuesWithoutFts, yValuesWithoutFts)
    }

    plotBuilder.buildChart(seriesDict, 'Полнотекстовый поиск (несколько слов)', 'Строки', 'Время (с)', 'select_fts_multi', rasterFormat)

    return {
        'with_fts': resultsWithFts,
        'without_fts': resultsWithoutFts
    }

def measureFtsInsertExperiment(rowCounts, outputDir, rasterFormat):
    print('Исследование INSERT с полнотекстовым индексом', flush=True)
    resultsWithFts = []
    resultsWithoutFts = []

    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('FTS INSERT rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")
            cur.execute("CREATE INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx ON " + SANDBOX_SCHEMA_NAME + ".movie_review USING gin(to_tsvector('english', review_text))")

            def insertWithFts():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateMovieReviews(rowCount)
                return rowCount

            avgTimeWithFts, result = measureAverageTime(insertWithFts)
            resultsWithFts.append((rowCount, avgTimeWithFts / rowCount))

            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")
            cur.execute("DROP INDEX " + SANDBOX_SCHEMA_NAME + "_review_fts_idx")

            def insertWithoutFts():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateMovieReviews(rowCount)
                return rowCount

            avgTimeWithoutFts, result = measureAverageTime(insertWithoutFts)
            resultsWithoutFts.append((rowCount, avgTimeWithoutFts / rowCount))

            print('  с FTS:', f'{avgTimeWithFts/rowCount:.6f}', 'без FTS:', f'{avgTimeWithoutFts/rowCount:.6f}', flush=True)

    plotBuilder = PlotBuilder(outputDir)
    xValuesWithFts = [result[0] for result in resultsWithFts]
    yValuesWithFts = [result[1] for result in resultsWithFts]
    xValuesWithoutFts = [result[0] for result in resultsWithoutFts]
    yValuesWithoutFts = [result[1] for result in resultsWithoutFts]

    seriesDict = {
        'С полнотекстовым индексом': (xValuesWithFts, yValuesWithFts),
        'Без полнотекстового индекса': (xValuesWithoutFts, yValuesWithoutFts)
    }

    plotBuilder.buildChart(seriesDict, 'INSERT с полнотекстовым индексом', 'Строки', 'Время на вставку (с)', 'fts_insert', rasterFormat)

    return {
        'with_fts': resultsWithFts,
        'without_fts': resultsWithoutFts
    }
