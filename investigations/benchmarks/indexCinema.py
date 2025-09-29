import os
import timeit
from lib.db.connection import getDbConnection
from lib.visualization.plots import PlotBuilder
import json

SIZES = [1000, 3000, 6000, 10000]
REPEATS = 3
REPEAT_NUMBER = 1
RESULTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'results', 'index_bench')
TITLE_PREFIX = 'Title_'
GENRE_PREFIX = 'Genre_'
PREFIX_MATCH = 'Title_1'
SUBSTRING_MATCH = '_5'
FTS_WORD_ONE = 'alpha'
FTS_WORD_TWO = 'alpha beta'
WORDS_A = ['alpha', 'beta', 'gamma', 'delta', 'omega']
WORDS_B = ['red', 'green', 'blue', 'white', 'black']


def runSql(cursorObj, sqlText):
    cursorObj.execute(sqlText)


def resetViewerTablePair(cursorObj):
    runSql(cursorObj, 'DROP TABLE IF EXISTS viewer_t1_pk CASCADE;')
    runSql(cursorObj, 'DROP TABLE IF EXISTS viewer_t2_nopk CASCADE;')
    runSql(cursorObj, 'CREATE TABLE viewer_t1_pk (viewer_id BIGSERIAL PRIMARY KEY, first_name VARCHAR(100) NOT NULL, last_name VARCHAR(100) NOT NULL, email TEXT NOT NULL, phone_number VARCHAR(32) NOT NULL);')
    runSql(cursorObj, 'CREATE TABLE viewer_t2_nopk (viewer_id BIGINT NOT NULL, first_name VARCHAR(100) NOT NULL, last_name VARCHAR(100) NOT NULL, email TEXT NOT NULL, phone_number VARCHAR(32) NOT NULL);')


def insertViewerRows(cursorObj, tableName, rowCount):
    rowsData = []
    for indexRow in range(rowCount):
        baseVal = 'v' + str(indexRow)
        rowsData.append((baseVal, baseVal + 'L', 'mail' + str(indexRow) + '@ex.com', '+10000000000'))
    placeholders = []
    for _ in range(len(rowsData)):
        placeholders.append('(%s,%s,%s,%s)')
    flatValues = []
    for record in rowsData:
        for field in record:
            flatValues.append(field)
    sqlInsert = 'INSERT INTO ' + tableName + ' (first_name,last_name,email,phone_number) VALUES ' + ','.join(placeholders)
    cursorObj.execute(sqlInsert, flatValues)


def measureViewerPrimaryKeyPerformance():
    selectEqTimesWithPk = []
    selectEqTimesNoPk = []
    selectLtTimesWithPk = []
    selectLtTimesNoPk = []
    insertTimesWithPk = []
    insertTimesNoPk = []
    with getDbConnection() as (conn, cur):
        for rowCount in SIZES:
            def runInsertWithPk():
                resetViewerTablePair(cur)
                insertViewerRows(cur, 'viewer_t1_pk', rowCount)
            def runInsertNoPk():
                resetViewerTablePair(cur)
                insertViewerRows(cur, 'viewer_t2_nopk', rowCount)
            insertTimeWithPk = min(timeit.repeat('runInsertWithPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            insertTimeNoPk = min(timeit.repeat('runInsertNoPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            resetViewerTablePair(cur)
            insertViewerRows(cur, 'viewer_t1_pk', rowCount)
            insertViewerRows(cur, 'viewer_t2_nopk', rowCount)
            targetId = rowCount // 2
            def runSelectEqWithPk():
                runSql(cur, 'SELECT viewer_id FROM viewer_t1_pk WHERE viewer_id = ' + str(targetId))
            def runSelectEqNoPk():
                runSql(cur, 'SELECT viewer_id FROM viewer_t2_nopk WHERE viewer_id = ' + str(targetId))
            def runSelectLtWithPk():
                runSql(cur, 'SELECT viewer_id FROM viewer_t1_pk WHERE viewer_id < ' + str(targetId))
            def runSelectLtNoPk():
                runSql(cur, 'SELECT viewer_id FROM viewer_t2_nopk WHERE viewer_id < ' + str(targetId))
            selectEqTimeWithPk = min(timeit.repeat('runSelectEqWithPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectEqTimeNoPk = min(timeit.repeat('runSelectEqNoPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectLtTimeWithPk = min(timeit.repeat('runSelectLtWithPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectLtTimeNoPk = min(timeit.repeat('runSelectLtNoPk()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectEqTimesWithPk.append((rowCount, selectEqTimeWithPk))
            selectEqTimesNoPk.append((rowCount, selectEqTimeNoPk))
            selectLtTimesWithPk.append((rowCount, selectLtTimeWithPk))
            selectLtTimesNoPk.append((rowCount, selectLtTimeNoPk))
            insertTimesWithPk.append((rowCount, insertTimeWithPk))
            insertTimesNoPk.append((rowCount, insertTimeNoPk))
    return {
        'selectEq': {'withPk': selectEqTimesWithPk, 'noPk': selectEqTimesNoPk},
        'selectLt': {'withPk': selectLtTimesWithPk, 'noPk': selectLtTimesNoPk},
        'insert': {'withPk': insertTimesWithPk, 'noPk': insertTimesNoPk}
    }


def resetMovieTitleIndexPair(cursorObj):
    runSql(cursorObj, 'DROP TABLE IF EXISTS movie_t3_idx CASCADE;')
    runSql(cursorObj, 'DROP TABLE IF EXISTS movie_t4_no_idx CASCADE;')
    runSql(cursorObj, 'CREATE TABLE movie_t3_idx (movie_id BIGSERIAL PRIMARY KEY, title VARCHAR(255) NOT NULL, genre VARCHAR(100) NOT NULL);')
    runSql(cursorObj, 'CREATE TABLE movie_t4_no_idx (movie_id BIGINT NOT NULL, title VARCHAR(255) NOT NULL, genre VARCHAR(100) NOT NULL);')
    runSql(cursorObj, 'CREATE INDEX movie_t3_title_idx ON movie_t3_idx(title);')


def insertMovieTitleRows(cursorObj, tableName, rowCount):
    rowsData = []
    for indexRow in range(rowCount):
        titleValue = TITLE_PREFIX + str(indexRow)
        genreValue = GENRE_PREFIX + str(indexRow % 5)
        rowsData.append((titleValue, genreValue))
    placeholders = []
    for _ in range(len(rowsData)):
        placeholders.append('(%s,%s)')
    flatValues = []
    for record in rowsData:
        for field in record:
            flatValues.append(field)
    sqlInsert = 'INSERT INTO ' + tableName + ' (title, genre) VALUES ' + ','.join(placeholders)
    cursorObj.execute(sqlInsert, flatValues)


def measureMovieTitleIndexPerformance():
    selectEqTimesWithIndex = []
    selectEqTimesNoIndex = []
    selectPrefixTimesWithIndex = []
    selectPrefixTimesNoIndex = []
    selectSubstringTimesWithIndex = []
    selectSubstringTimesNoIndex = []
    insertTimesWithIndex = []
    insertTimesNoIndex = []
    with getDbConnection() as (conn, cur):
        for rowCount in SIZES:
            def runInsertWithIndex():
                resetMovieTitleIndexPair(cur)
                insertMovieTitleRows(cur, 'movie_t3_idx', rowCount)
            def runInsertNoIndex():
                resetMovieTitleIndexPair(cur)
                insertMovieTitleRows(cur, 'movie_t4_no_idx', rowCount)
            insertTimeWithIndex = min(timeit.repeat('runInsertWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            insertTimeNoIndex = min(timeit.repeat('runInsertNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            resetMovieTitleIndexPair(cur)
            insertMovieTitleRows(cur, 'movie_t3_idx', rowCount)
            insertMovieTitleRows(cur, 'movie_t4_no_idx', rowCount)
            targetId = rowCount // 2
            targetTitle = TITLE_PREFIX + str(targetId)
            def runSelectEqWithIndex():
                runSql(cur, "SELECT movie_id FROM movie_t3_idx WHERE title = '" + targetTitle + "'")
            def runSelectEqNoIndex():
                runSql(cur, "SELECT movie_id FROM movie_t4_no_idx WHERE title = '" + targetTitle + "'")
            def runSelectPrefixWithIndex():
                runSql(cur, "SELECT movie_id FROM movie_t3_idx WHERE title LIKE '" + PREFIX_MATCH + "%'")
            def runSelectPrefixNoIndex():
                runSql(cur, "SELECT movie_id FROM movie_t4_no_idx WHERE title LIKE '" + PREFIX_MATCH + "%'")
            def runSelectSubstringWithIndex():
                runSql(cur, "SELECT movie_id FROM movie_t3_idx WHERE title LIKE '%" + SUBSTRING_MATCH + "%'")
            def runSelectSubstringNoIndex():
                runSql(cur, "SELECT movie_id FROM movie_t4_no_idx WHERE title LIKE '%" + SUBSTRING_MATCH + "%'")
            selectEqTimeWithIndex = min(timeit.repeat('runSelectEqWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectEqTimeNoIndex = min(timeit.repeat('runSelectEqNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectPrefixTimeWithIndex = min(timeit.repeat('runSelectPrefixWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectPrefixTimeNoIndex = min(timeit.repeat('runSelectPrefixNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectSubstringTimeWithIndex = min(timeit.repeat('runSelectSubstringWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectSubstringTimeNoIndex = min(timeit.repeat('runSelectSubstringNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            selectEqTimesWithIndex.append((rowCount, selectEqTimeWithIndex))
            selectEqTimesNoIndex.append((rowCount, selectEqTimeNoIndex))
            selectPrefixTimesWithIndex.append((rowCount, selectPrefixTimeWithIndex))
            selectPrefixTimesNoIndex.append((rowCount, selectPrefixTimeNoIndex))
            selectSubstringTimesWithIndex.append((rowCount, selectSubstringTimeWithIndex))
            selectSubstringTimesNoIndex.append((rowCount, selectSubstringTimeNoIndex))
            insertTimesWithIndex.append((rowCount, insertTimeWithIndex))
            insertTimesNoIndex.append((rowCount, insertTimeNoIndex))
    return {
        'selectEq': {'withIndex': selectEqTimesWithIndex, 'noIndex': selectEqTimesNoIndex},
        'selectPrefix': {'withIndex': selectPrefixTimesWithIndex, 'noIndex': selectPrefixTimesNoIndex},
        'selectSubstring': {'withIndex': selectSubstringTimesWithIndex, 'noIndex': selectSubstringTimesNoIndex},
        'insert': {'withIndex': insertTimesWithIndex, 'noIndex': insertTimesNoIndex}
    }


def resetMovieFullTextPair(cursorObj):
    runSql(cursorObj, 'DROP TABLE IF EXISTS movie_ft_t5 CASCADE;')
    runSql(cursorObj, 'DROP TABLE IF EXISTS movie_ft_t6_no_idx CASCADE;')
    runSql(cursorObj, "CREATE TABLE movie_ft_t5 (movie_id BIGSERIAL PRIMARY KEY, title TEXT NOT NULL, genre TEXT NOT NULL, search_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(genre,''))) STORED);")
    runSql(cursorObj, "CREATE TABLE movie_ft_t6_no_idx (movie_id BIGSERIAL PRIMARY KEY, title TEXT NOT NULL, genre TEXT NOT NULL, search_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(genre,''))) STORED);")
    runSql(cursorObj, 'CREATE INDEX movie_ft_t5_search_idx ON movie_ft_t5 USING GIN (search_tsv);')


def insertMovieFullTextRows(cursorObj, tableName, rowCount):
    rowsData = []
    for indexRow in range(rowCount):
        wordOne = WORDS_A[indexRow % len(WORDS_A)]
        wordTwo = WORDS_B[indexRow % len(WORDS_B)]
        titleValue = wordOne + ' ' + wordTwo
        genreValue = 'genre' + str(indexRow % 7)
        rowsData.append((titleValue, genreValue))
    placeholders = []
    for _ in range(len(rowsData)):
        placeholders.append('(%s,%s)')
    flatValues = []
    for record in rowsData:
        for field in record:
            flatValues.append(field)
    sqlInsert = 'INSERT INTO ' + tableName + ' (title, genre) VALUES ' + ','.join(placeholders)
    cursorObj.execute(sqlInsert, flatValues)


def measureMovieFullTextPerformance():
    ftsOneWordTimesWithIndex = []
    ftsOneWordTimesNoIndex = []
    ftsTwoWordsTimesWithIndex = []
    ftsTwoWordsTimesNoIndex = []
    insertTimesWithIndex = []
    insertTimesNoIndex = []
    with getDbConnection() as (conn, cur):
        for rowCount in SIZES:
            def runInsertWithFtsIndex():
                resetMovieFullTextPair(cur)
                insertMovieFullTextRows(cur, 'movie_ft_t5', rowCount)
            def runInsertNoFtsIndex():
                resetMovieFullTextPair(cur)
                insertMovieFullTextRows(cur, 'movie_ft_t6_no_idx', rowCount)
            insertTimeWithIndex = min(timeit.repeat('runInsertWithFtsIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            insertTimeNoIndex = min(timeit.repeat('runInsertNoFtsIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            resetMovieFullTextPair(cur)
            insertMovieFullTextRows(cur, 'movie_ft_t5', rowCount)
            insertMovieFullTextRows(cur, 'movie_ft_t6_no_idx', rowCount)
            def runFtsOneWordWithIndex():
                runSql(cur, "SELECT movie_id FROM movie_ft_t5 WHERE search_tsv @@ plainto_tsquery('english','" + FTS_WORD_ONE + "')")
            def runFtsOneWordNoIndex():
                runSql(cur, "SELECT movie_id FROM movie_ft_t6_no_idx WHERE search_tsv @@ plainto_tsquery('english','" + FTS_WORD_ONE + "')")
            def runFtsTwoWordsWithIndex():
                runSql(cur, "SELECT movie_id FROM movie_ft_t5 WHERE search_tsv @@ plainto_tsquery('english','" + FTS_WORD_TWO + "')")
            def runFtsTwoWordsNoIndex():
                runSql(cur, "SELECT movie_id FROM movie_ft_t6_no_idx WHERE search_tsv @@ plainto_tsquery('english','" + FTS_WORD_TWO + "')")
            ftsOneWordTimeWithIndex = min(timeit.repeat('runFtsOneWordWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            ftsOneWordTimeNoIndex = min(timeit.repeat('runFtsOneWordNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            ftsTwoWordsTimeWithIndex = min(timeit.repeat('runFtsTwoWordsWithIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            ftsTwoWordsTimeNoIndex = min(timeit.repeat('runFtsTwoWordsNoIndex()', repeat=REPEATS, number=REPEAT_NUMBER, globals=locals()))
            insertTimesWithIndex.append((rowCount, insertTimeWithIndex))
            insertTimesNoIndex.append((rowCount, insertTimeNoIndex))
            ftsOneWordTimesWithIndex.append((rowCount, ftsOneWordTimeWithIndex))
            ftsOneWordTimesNoIndex.append((rowCount, ftsOneWordTimeNoIndex))
            ftsTwoWordsTimesWithIndex.append((rowCount, ftsTwoWordsTimeWithIndex))
            ftsTwoWordsTimesNoIndex.append((rowCount, ftsTwoWordsTimeNoIndex))
    return {
        'ftsOneWord': {'withFtsIndex': ftsOneWordTimesWithIndex, 'noFtsIndex': ftsOneWordTimesNoIndex},
        'ftsTwoWords': {'withFtsIndex': ftsTwoWordsTimesWithIndex, 'noFtsIndex': ftsTwoWordsTimesNoIndex},
        'insert': {'withFtsIndex': insertTimesWithIndex, 'noFtsIndex': insertTimesNoIndex}
    }


def measureGenericIndexSet(configList, outDir):
    if not configList:
        return None
    if not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    rows = []
    names = []
    with getDbConnection() as (conn, cur):
        for idx, cfg in enumerate(configList):
            table = cfg.get('table')
            columns = cfg.get('columns', [])
            indexType = cfg.get('indexType', 'btree')
            if not table or not columns:
                continue
            idxName = 'gen_idx_' + table + '_' + '_'.join(columns) + '_' + str(idx)
            whereParts = []
            for c in columns:
                whereParts.append(c + ' = %s')
            sampleSql = 'SELECT COUNT(*) FROM ' + table + ' WHERE ' + ' AND '.join(whereParts)
            params = [None] * len(columns)
            def runNo():
                cur.execute(sampleSql, params)
                cur.fetchone()
            def runWith():
                cur.execute(sampleSql, params)
                cur.fetchone()
            tNo = min(timeit.Timer(runNo).repeat(repeat=REPEATS, number=1))
            cur.execute('CREATE INDEX ' + idxName + ' ON ' + table + ' USING ' + indexType + ' (' + ', '.join(columns) + ')')
            tWith = min(timeit.Timer(runWith).repeat(repeat=REPEATS, number=1))
            cur.execute('DROP INDEX IF EXISTS ' + idxName)
            rows.append((table, columns, indexType, tNo, tWith))
            names.append(idxName)
    csvPath = os.path.join(outDir, 'generic_index.csv')
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write('table,columns,index_type,time_without_index,time_with_index\n')
        for r in rows:
            f.write(r[0] + ',' + ';'.join(r[1]) + ',' + r[2] + ',' + f"{r[3]:.6f}" + ',' + f"{r[4]:.6f}" + '\n')
    builder = PlotBuilder(outDir)
    xs = list(range(len(rows)))
    withoutSeries = (xs, [r[3] for r in rows])
    withSeries = (xs, [r[4] for r in rows])
    builder.buildChart({'no_index': withoutSeries, 'with_index': withSeries}, 'Generic index performance', 'index case', 'Time (s)', 'generic_index', True)
    return csvPath


def writeCsv(fileBaseName, dataMap):
    if not os.path.isdir(RESULTS_DIR):
        os.makedirs(RESULTS_DIR, exist_ok=True)
    csvPath = os.path.join(RESULTS_DIR, fileBaseName + '.csv')
    with open(csvPath, 'w', encoding='utf-8') as outFile:
        outFile.write('graph,series,rows,time_seconds\n')
        for graphName in dataMap:
            seriesMap = dataMap[graphName]
            for seriesName in seriesMap:
                seriesRows = seriesMap[seriesName]
                for point in seriesRows:
                    outFile.write(graphName + ',' + seriesName + ',' + str(point[0]) + ',' + f"{point[1]:.6f}" + '\n')
    return csvPath


def buildGraphs(prefixName, dataMap):
    builder = PlotBuilder(os.path.join(RESULTS_DIR))
    for graphName in dataMap:
        seriesMap = dataMap[graphName]
        chartSeries = {}
        for seriesName in seriesMap:
            rows = seriesMap[seriesName]
            xValues = []
            yValues = []
            for pair in rows:
                xValues.append(pair[0])
                yValues.append(pair[1])
            chartSeries[seriesName] = (xValues, yValues)
        builder.buildChart(chartSeries, graphName, 'rows', 'time(s)', prefixName + '_' + graphName, isRaster=False)


def runIndexBenchmarks():
    viewerData = measureViewerPrimaryKeyPerformance()
    writeCsv('viewer_pk', viewerData)
    buildGraphs('viewer_pk', viewerData)
    movieTitleData = measureMovieTitleIndexPerformance()
    writeCsv('movie_title', movieTitleData)
    buildGraphs('movie_title', movieTitleData)
    movieFtsData = measureMovieFullTextPerformance()
    writeCsv('movie_fts', movieFtsData)
    buildGraphs('movie_fts', movieFtsData)
    genCfgPath = os.environ.get('GENERIC_INDEX_CONFIG')
    if genCfgPath and os.path.isfile(genCfgPath):
        try:
            with open(genCfgPath, 'r', encoding='utf-8') as jf:
                cfg = json.load(jf)
                if isinstance(cfg, list):
                    measureGenericIndexSet(cfg, RESULTS_DIR)
                elif isinstance(cfg, dict):
                    confList = cfg.get('indexes', [])
                    measureGenericIndexSet(confList, RESULTS_DIR)
        except Exception:
            pass
    return True

if __name__ == '__main__':
    runIndexBenchmarks()
