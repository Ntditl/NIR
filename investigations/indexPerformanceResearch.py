import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

PK_ROW_COUNTS = [100, 500, 1000, 2000, 3000]
STRING_INDEX_ROW_COUNTS = [100, 500, 1000, 2000, 3000]
FTS_ROW_COUNTS = [100, 500, 1000, 1500, 2000]
RUNS = 3
QUERIES_PER_RUN = 50

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
            getattr(dataGenerator, f'generate{tableName.capitalize()}s')(rowCount)

            if indexSql:
                cur.execute(indexSql)

            totalTimeWithIndex = 0.0
            for runIndex in range(RUNS):
                for queryIndex in range(QUERIES_PER_RUN):
                    timeWithIndex = measureAverageTime(lambda: testFunction(cur, rowCount), 1)
                    totalTimeWithIndex += timeWithIndex

            avgTimeWithIndex = totalTimeWithIndex / (RUNS * QUERIES_PER_RUN)

            if indexSql:
                cur.execute(f"DROP INDEX IF EXISTS {SANDBOX_SCHEMA_NAME}.{columnName}_idx;")

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

def measurePkIndexEffect(rowCounts, outputDir, rasterFormat):
    def pkEqualityTest(cur, rowCount):
        targetId = random.randint(1, rowCount)
        cur.execute(f"SELECT movie_id, title FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id = %s", (targetId,))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS movie_id_idx ON {SANDBOX_SCHEMA_NAME}.movie (movie_id);"
    results1, results2 = _executeIndexExperiment('movie', 'movie_id', indexSql, pkEqualityTest, rowCounts, 'pk_index_effect.csv', 'PK INDEX')

    csvPath = outputDir + '/pk_index_effect.csv'
    plotPath = outputDir + '/pk_index_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект первичного ключа (SELECT по равенству)', 'С первичным ключом', 'Без первичного ключа', 'Количество строк в таблице')

    return results1, results2

def measurePkInequalityEffect(rowCounts, outputDir, rasterFormat):
    def pkInequalityTest(cur, rowCount):
        targetId = random.randint(1, rowCount // 2)
        cur.execute(f"SELECT movie_id, title FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < %s", (targetId,))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS movie_id_idx ON {SANDBOX_SCHEMA_NAME}.movie (movie_id);"
    results1, results2 = _executeIndexExperiment('movie', 'movie_id', indexSql, pkInequalityTest, rowCounts, 'pk_inequality_effect.csv', 'PK INEQUALITY')

    csvPath = outputDir + '/pk_inequality_effect.csv'
    plotPath = outputDir + '/pk_inequality_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект первичного ключа (SELECT по неравенству)', 'С первичным ключом', 'Без первичного ключа', 'Количество строк в таблице')

    return results1, results2

def measurePkInsertEffect(rowCounts, outputDir, rasterFormat):
    resultsWithPk = []
    resultsWithoutPk = []

    ensureBaseDataMinimalInSandbox()

    for insertCount in rowCounts:
        print(f'PK INSERT insertCount {insertCount}', flush=True)

        totalTimeWithPk = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.movie RESTART IDENTITY CASCADE;")

                def insertWithPk():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    dataGenerator.generateMovies(insertCount)

                timeWithPk = measureAverageTime(insertWithPk, 1)
                totalTimeWithPk += timeWithPk

        avgTimeWithPk = totalTimeWithPk / RUNS

        totalTimeWithoutPk = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"DROP TABLE IF EXISTS {SANDBOX_SCHEMA_NAME}.movie_no_pk;")
                cur.execute(f"CREATE TABLE {SANDBOX_SCHEMA_NAME}.movie_no_pk AS SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE 1=0;")

                def insertWithoutPk():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    for i in range(insertCount):
                        movieData = dataGenerator._generateMovieData()
                        cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie_no_pk (title, description, release_date, duration_minutes, rating) VALUES (%s, %s, %s, %s, %s)",
                                   (movieData['title'], movieData['description'], movieData['release_date'], movieData['duration_minutes'], movieData['rating']))

                timeWithoutPk = measureAverageTime(insertWithoutPk, 1)
                totalTimeWithoutPk += timeWithoutPk

        avgTimeWithoutPk = totalTimeWithoutPk / RUNS

        resultsWithPk.append([insertCount, avgTimeWithPk])
        resultsWithoutPk.append([insertCount, avgTimeWithoutPk])

    csvPath = outputDir + '/pk_insert_effect.csv'
    plotPath = outputDir + '/pk_insert_effect' if rasterFormat else None
    _saveResults(resultsWithPk, resultsWithoutPk, csvPath, plotPath, 'Эффект первичного ключа (INSERT)', 'С первичным ключом', 'Без первичного ключа', 'Количество вставляемых строк')

    return resultsWithPk, resultsWithoutPk

def measureStringIndexExperiment(rowCounts, outputDir, rasterFormat):
    def stringEqualityTest(cur, rowCount):
        names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona"]
        targetName = random.choice(names)
        cur.execute(f"SELECT viewer_id, name FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE name = %s", (targetName,))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS viewer_name_idx ON {SANDBOX_SCHEMA_NAME}.viewer (name);"
    results1, results2 = _executeIndexExperiment('viewer', 'name', indexSql, stringEqualityTest, rowCounts, 'string_index_effect.csv', 'STRING INDEX')

    csvPath = outputDir + '/string_index_effect.csv'
    plotPath = outputDir + '/string_index_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект строкового индекса (SELECT по равенству)', 'С индексом', 'Без индекса', 'Количество строк в таблице')

    return results1, results2

def measureStringLikePrefix(rowCounts, outputDir, rasterFormat):
    def stringPrefixTest(cur, rowCount):
        prefixes = ["Al", "Bo", "Ch", "Di", "Ed", "Fi"]
        targetPrefix = random.choice(prefixes)
        cur.execute(f"SELECT viewer_id, name FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE name LIKE %s", (targetPrefix + '%',))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS viewer_name_idx ON {SANDBOX_SCHEMA_NAME}.viewer (name);"
    results1, results2 = _executeIndexExperiment('viewer', 'name', indexSql, stringPrefixTest, rowCounts, 'string_like_prefix_effect.csv', 'STRING LIKE PREFIX')

    csvPath = outputDir + '/string_like_prefix_effect.csv'
    plotPath = outputDir + '/string_like_prefix_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект строкового индекса (LIKE prefix)', 'С индексом', 'Без индекса', 'Количество строк в таблице')

    return results1, results2

def measureStringLikeContains(rowCounts, outputDir, rasterFormat):
    def stringContainsTest(cur, rowCount):
        substrings = ["ic", "ob", "ar", "an", "wa", "on"]
        targetSubstring = random.choice(substrings)
        cur.execute(f"SELECT viewer_id, name FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE name LIKE %s", ('%' + targetSubstring + '%',))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS viewer_name_idx ON {SANDBOX_SCHEMA_NAME}.viewer (name);"
    results1, results2 = _executeIndexExperiment('viewer', 'name', indexSql, stringContainsTest, rowCounts, 'string_like_contains_effect.csv', 'STRING LIKE CONTAINS')

    csvPath = outputDir + '/string_like_contains_effect.csv'
    plotPath = outputDir + '/string_like_contains_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект строкового индекса (LIKE contains)', 'С индексом', 'Без индекса', 'Количество строк в таблице')

    return results1, results2

def measureStringInsertExperiment(rowCounts, outputDir, rasterFormat):
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for insertCount in rowCounts:
        print(f'STRING INSERT insertCount {insertCount}', flush=True)

        totalTimeWithIndex = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.viewer RESTART IDENTITY CASCADE;")
                cur.execute(f"CREATE INDEX IF NOT EXISTS viewer_name_idx ON {SANDBOX_SCHEMA_NAME}.viewer (name);")

                def insertWithIndex():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    dataGenerator.generateViewers(insertCount)

                timeWithIndex = measureAverageTime(insertWithIndex, 1)
                totalTimeWithIndex += timeWithIndex

        avgTimeWithIndex = totalTimeWithIndex / RUNS

        totalTimeWithoutIndex = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.viewer RESTART IDENTITY CASCADE;")
                cur.execute(f"DROP INDEX IF EXISTS {SANDBOX_SCHEMA_NAME}.viewer_name_idx;")

                def insertWithoutIndex():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    dataGenerator.generateViewers(insertCount)

                timeWithoutIndex = measureAverageTime(insertWithoutIndex, 1)
                totalTimeWithoutIndex += timeWithoutIndex

        avgTimeWithoutIndex = totalTimeWithoutIndex / RUNS

        resultsWithIndex.append([insertCount, avgTimeWithIndex])
        resultsWithoutIndex.append([insertCount, avgTimeWithoutIndex])

    csvPath = outputDir + '/string_insert_effect.csv'
    plotPath = outputDir + '/string_insert_effect' if rasterFormat else None
    _saveResults(resultsWithIndex, resultsWithoutIndex, csvPath, plotPath, 'Эффект строкового индекса (INSERT)', 'С индексом', 'Без индекса', 'Количество вставляемых строк')

    return resultsWithIndex, resultsWithoutIndex

def measureFtsSingleWordExperiment(rowCounts, outputDir, rasterFormat):
    def ftsSingleTest(cur, rowCount):
        words = ["adventure", "action", "comedy", "drama", "thriller", "romance"]
        targetWord = random.choice(words)
        cur.execute(f"SELECT movie_id, title FROM {SANDBOX_SCHEMA_NAME}.movie WHERE to_tsvector('russian', description) @@ plainto_tsquery('russian', %s)", (targetWord,))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS movie_description_fts_idx ON {SANDBOX_SCHEMA_NAME}.movie USING gin(to_tsvector('russian', description));"
    results1, results2 = _executeIndexExperiment('movie', 'description', indexSql, ftsSingleTest, rowCounts, 'fts_single_word_effect.csv', 'FTS SINGLE WORD')

    csvPath = outputDir + '/fts_single_word_effect.csv'
    plotPath = outputDir + '/fts_single_word_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект полнотекстового индекса (одно слово)', 'С FTS индексом', 'Без индекса (LIKE)', 'Количество строк в таблице')

    return results1, results2

def measureFtsMultiWordExperiment(rowCounts, outputDir, rasterFormat):
    def ftsMultiTest(cur, rowCount):
        wordPairs = ["action adventure", "romantic comedy", "science fiction", "horror thriller", "family drama"]
        targetPhrase = random.choice(wordPairs)
        cur.execute(f"SELECT movie_id, title FROM {SANDBOX_SCHEMA_NAME}.movie WHERE to_tsvector('russian', description) @@ plainto_tsquery('russian', %s)", (targetPhrase,))
        cur.fetchall()

    indexSql = f"CREATE INDEX IF NOT EXISTS movie_description_fts_idx ON {SANDBOX_SCHEMA_NAME}.movie USING gin(to_tsvector('russian', description));"
    results1, results2 = _executeIndexExperiment('movie', 'description', indexSql, ftsMultiTest, rowCounts, 'fts_multi_word_effect.csv', 'FTS MULTI WORD')

    csvPath = outputDir + '/fts_multi_word_effect.csv'
    plotPath = outputDir + '/fts_multi_word_effect' if rasterFormat else None
    _saveResults(results1, results2, csvPath, plotPath, 'Эффект полнотекстового индекса (несколько слов)', 'С FTS индексом', 'Без индекса (LIKE)', 'Количество строк в таблице')

    return results1, results2

def measureFtsInsertExperiment(rowCounts, outputDir, rasterFormat):
    resultsWithIndex = []
    resultsWithoutIndex = []

    ensureBaseDataMinimalInSandbox()

    for insertCount in rowCounts:
        print(f'FTS INSERT insertCount {insertCount}', flush=True)

        totalTimeWithIndex = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.movie RESTART IDENTITY CASCADE;")
                cur.execute(f"CREATE INDEX IF NOT EXISTS movie_description_fts_idx ON {SANDBOX_SCHEMA_NAME}.movie USING gin(to_tsvector('russian', description));")

                def insertWithIndex():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    dataGenerator.generateMovies(insertCount)

                timeWithIndex = measureAverageTime(insertWithIndex, 1)
                totalTimeWithIndex += timeWithIndex

        avgTimeWithIndex = totalTimeWithIndex / RUNS

        totalTimeWithoutIndex = 0.0
        for runIndex in range(RUNS):
            with getDbConnection() as (conn, cur):
                cur.execute(f"TRUNCATE TABLE {SANDBOX_SCHEMA_NAME}.movie RESTART IDENTITY CASCADE;")
                cur.execute(f"DROP INDEX IF EXISTS {SANDBOX_SCHEMA_NAME}.movie_description_fts_idx;")

                def insertWithoutIndex():
                    dataGenerator = RandomDataGenerator()
                    dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                    dataGenerator.generateMovies(insertCount)

                timeWithoutIndex = measureAverageTime(insertWithoutIndex, 1)
                totalTimeWithoutIndex += timeWithoutIndex

        avgTimeWithoutIndex = totalTimeWithoutIndex / RUNS

        resultsWithIndex.append([insertCount, avgTimeWithIndex])
        resultsWithoutIndex.append([insertCount, avgTimeWithoutIndex])

    csvPath = outputDir + '/fts_insert_effect.csv'
    plotPath = outputDir + '/fts_insert_effect' if rasterFormat else None
    _saveResults(resultsWithIndex, resultsWithoutIndex, csvPath, plotPath, 'Эффект полнотекстового индекса (INSERT)', 'С FTS индексом', 'Без индекса', 'Количество вставляемых строк')

    return resultsWithIndex, resultsWithoutIndex
