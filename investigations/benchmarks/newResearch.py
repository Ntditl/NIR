import os
import random
import timeit
from decimal import Decimal
from datetime import datetime, timezone
from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql, getTableNames
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder

ROW_COUNTS_DEFAULT = [5, 50, 100, 150, 200, 250,300,350,400,450]
REPEATS_DEFAULT = 3
RANDOM_SEED = 12345
USE_OPTIMIZED_FLOW = True
PK_ROW_COUNTS = [10, 50, 100, 200, 500, 1000, 1500, 2000]
STRING_INDEX_ROW_COUNTS = [1000, 5000, 10000, 20000, 40000]
PK_RUNS = 5
PK_QUERIES_PER_RUN = 100
STRING_QUERIES = 100
STRING_INDEX_SAMPLE_QUERIES = 100
FTS_ROW_COUNTS = [1000, 5000, 10000, 20000, 40000]
FTS_SAMPLE_QUERIES = 100
FTS_MULTI_SAMPLE_QUERIES = 100
FTS_DICTIONARY = [
    'error','warning','timeout','failure','success','update','insert','delete','select','commit',
    'rollback','network','disk','memory','cache','index','table','query','plan','analyze',
    'optimize','engine','thread','process','batch','user','session','login','logout','permission',
    'denied','granted','read','write','latency','throughput','overflow','underflow','exception','handler'
]

random.seed(RANDOM_SEED)

def _median(values):
    ln = len(values)
    if ln == 0:
        return 0.0
    arr = list(values)
    arr.sort()
    mid = ln // 2
    if ln % 2 == 1:
        return arr[mid]
    return (arr[mid - 1] + arr[mid]) / 2.0


def resetDatabase():
    print('Сброс БД старт', flush=True)
    ddls = getCreateTablesSql()
    with getDbConnection() as (conn, cur):
        tableNames = getTableNames()
        for name in reversed(tableNames):
            cur.execute("DROP TABLE IF EXISTS " + name + " CASCADE;")
        for ddl in ddls:
            cur.execute(ddl)
    print('Сброс БД завершен', flush=True)


def truncateAll():
    with getDbConnection() as (conn, cur):
        names = getTableNames()
        for name in names:
            cur.execute("TRUNCATE TABLE " + name + " RESTART IDENTITY CASCADE;")


def generateDataset(rowCount):
    print('Генерация набора данных rowCount', rowCount, flush=True)
    gen = RandomDataGenerator()
    viewersCount = rowCount
    moviesCount = rowCount
    cinemasCount = max(1, rowCount // 50 + 1)
    hallsPerCinema = 2
    sessionsPerHall = 2
    favoritesRate = 0.2
    reviewRate = 0.15
    ticketRate = 0.1
    gen.generateData(viewersCount, moviesCount, cinemasCount, hallsPerCinema, sessionsPerHall, favoritesRate, reviewRate, ticketRate)
    print('Генерация завершена rowCount', rowCount, flush=True)


def measure(func):
    times = []
    for i in range(REPEATS_DEFAULT):
        t = timeit.Timer(func).timeit(number=1)
        times.append(t)
    total = 0.0
    for v in times:
        total = total + v
    avg = total / len(times)
    return avg


def measureAvg(func):
    times = []
    for i in range(REPEATS_DEFAULT):
        startTime = timeit.default_timer()
        func()
        endTime = timeit.default_timer()
        times.append(endTime - startTime)
    total = 0.0
    for v in times:
        total = total + v
    return total / len(times)


def ensureBaseDataMinimal():
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM cinema")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO cinema (name,address,phone_number,city) VALUES ('base_c','addr','+0','C')")
        cur.execute("SELECT COUNT(*) FROM hall")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO hall (cinema_id,hall_name,seat_count,base_ticket_price) SELECT min(cinema_id),'base_h',100,10.00 FROM cinema")
        cur.execute("SELECT COUNT(*) FROM movie")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO movie (title,genre,duration_minutes,release_date,rating,age_restriction) VALUES ('base_m','Action',100,'2020-01-01','PG-13',13)")
        cur.execute("SELECT COUNT(*) FROM viewer")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO viewer (first_name,last_name,email,phone_number) VALUES ('b','v','b@b','+1')")
        cur.execute("SELECT COUNT(*) FROM session")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO session (movie_id,hall_id,session_datetime,available_seats,final_price) SELECT min(movie_id),min(hall_id), now(), 100, 20.00 FROM movie, hall")


def insertDeleteBatch(tableName, batchSize, conn=None, cur=None):
    if conn is not None and cur is not None:
        if tableName == 'viewer':
            cur.execute("INSERT INTO viewer (first_name,last_name,email,phone_number) SELECT 'del','v','del_mark_'||g||'@ex.com','+d'||g FROM generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM viewer WHERE email LIKE 'del_mark_%'"
        if tableName == 'movie':
            cur.execute("INSERT INTO movie (title,genre,duration_minutes,release_date,rating,age_restriction) SELECT 'del_mark_'||g,'Test',100,'2020-01-01','PG-13',13 FROM generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM movie WHERE title LIKE 'del_mark_%'"
        if tableName == 'cinema':
            cur.execute("INSERT INTO cinema (name,address,phone_number,city) SELECT 'del_mark_'||g,'a','+c'||g,'City' FROM generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM cinema WHERE name LIKE 'del_mark_%'"
        if tableName == 'hall':
            ensureBaseDataMinimal()
            cur.execute("WITH c AS (SELECT min(cinema_id) cid FROM cinema) INSERT INTO hall (cinema_id,hall_name,seat_count,base_ticket_price) SELECT cid,'del_mark_'||g,100,10.00 FROM c, generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM hall WHERE hall_name LIKE 'del_mark_%'"
        if tableName == 'session':
            ensureBaseDataMinimal()
            cur.execute("WITH h AS (SELECT min(hall_id) hid FROM hall), m AS (SELECT min(movie_id) mid FROM movie) INSERT INTO session (movie_id,hall_id,session_datetime,available_seats,final_price) SELECT (SELECT mid FROM m),(SELECT hid FROM h), now(), 100, 99.99 FROM generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM session WHERE final_price = 99.99"
        if tableName == 'viewer_profile':
            ensureBaseDataMinimal()
            cur.execute("WITH v AS (SELECT viewer_id FROM viewer WHERE viewer_id NOT IN (SELECT viewer_id FROM viewer_profile) LIMIT %s) INSERT INTO viewer_profile (viewer_id,male_gender,nickname,birth_date) SELECT viewer_id,true,'del_mark', '2000-01-01' FROM v", (batchSize,))
            return "DELETE FROM viewer_profile WHERE nickname='del_mark'"
        if tableName == 'movie_review':
            ensureBaseDataMinimal()
            cur.execute("WITH v AS (SELECT min(viewer_id) vid FROM viewer), m AS (SELECT min(movie_id) mid FROM movie) INSERT INTO movie_review (movie_id,viewer_id,rating,comment) SELECT (SELECT mid FROM m),(SELECT vid FROM v),7,'del_mark' FROM generate_series(1,%s) g", (batchSize,))
            return "DELETE FROM movie_review WHERE comment='del_mark'"
        if tableName == 'favorite_movies':
            ensureBaseDataMinimal()
            cur.execute("WITH v AS (SELECT viewer_id FROM viewer LIMIT %s), m AS (SELECT movie_id FROM movie LIMIT %s) INSERT INTO favorite_movies (viewer_id,movie_id) SELECT v.viewer_id,m.movie_id FROM v JOIN m ON 1=1 WHERE NOT EXISTS (SELECT 1 FROM favorite_movies f WHERE f.viewer_id=v.viewer_id AND f.movie_id=m.movie_id) LIMIT %s", (batchSize, batchSize, batchSize))
            return "DELETE FROM favorite_movies USING movie WHERE favorite_movies.movie_id=movie.movie_id"
        if tableName == 'ticket':
            ensureBaseDataMinimal()
            cur.execute("WITH s AS (SELECT session_id FROM session LIMIT %s), v AS (SELECT viewer_id FROM viewer LIMIT %s) INSERT INTO ticket (session_id,viewer_id) SELECT s.session_id,v.viewer_id FROM s JOIN v ON 1=1 WHERE NOT EXISTS (SELECT 1 FROM ticket t WHERE t.session_id=s.session_id AND t.viewer_id=v.viewer_id) LIMIT %s", (batchSize, batchSize, batchSize))
            return "DELETE FROM ticket USING session WHERE ticket.session_id=session.session_id"
        return None
    return insertDeleteBatch(tableName, batchSize)  # fallback на старый путь если не передали соединение


def measureDeleteWhere(rowCounts):
    print('Замер DELETE старт', rowCounts, flush=True)
    results = {}
    targetTables = ['viewer','movie','cinema','hall','session','viewer_profile','movie_review','favorite_movies','ticket']
    for tableName in targetTables:
        print('  Таблица', tableName, 'старт', flush=True)
        series = []
        for n in rowCounts:
            print('    rowCount', n, 'подготовка', flush=True)
            truncateAll()
            generateDataset(n)
            deleteSql = insertDeleteBatch(tableName, min(50, n))
            def run():
                with getDbConnection() as (conn, cur):
                    cur.execute(deleteSql)
            avgTime = measure(run)
            print('    rowCount', n, 'готово среднее', f'{avgTime:.6f}', flush=True)
            series.append((n, avgTime))
        results[tableName] = series
        print('  Таблица', tableName, 'готово', flush=True)
    print('Замер DELETE завершен', flush=True)
    return results


def buildSeriesDict(results):
    out = {}
    for name in results:
        xs = []
        ys = []
        arr = results[name]
        for point in arr:
            xs.append(point[0])
            ys.append(point[1])
        out[name] = (xs, ys)
    return out


def measureJoins(rowCounts):
    print('Замер JOIN старт', rowCounts, flush=True)
    joinDefs = [
        ("cinema_hall", "SELECT COUNT(*) FROM hall h JOIN cinema c ON h.cinema_id=c.cinema_id"),
        ("hall_session_movie", "SELECT COUNT(*) FROM session s JOIN hall h ON s.hall_id=h.hall_id JOIN movie m ON s.movie_id=m.movie_id"),
        ("viewer_profile", "SELECT COUNT(*) FROM viewer_profile vp JOIN viewer v ON vp.viewer_id=v.viewer_id"),
        ("viewer_favorites_movie", "SELECT COUNT(*) FROM favorite_movies f JOIN viewer v ON f.viewer_id=v.viewer_id JOIN movie m ON f.movie_id=m.movie_id"),
        ("viewer_reviews_movie", "SELECT COUNT(*) FROM movie_review r JOIN viewer v ON r.viewer_id=v.viewer_id JOIN movie m ON r.movie_id=m.movie_id"),
        ("ticket_session_viewer", "SELECT COUNT(*) FROM ticket t JOIN session s ON t.session_id=s.session_id JOIN viewer v ON t.viewer_id=v.viewer_id")
    ]
    results = {}
    for name, sqlText in joinDefs:
        print('  JOIN', name, 'старт', flush=True)
        series = []
        for n in rowCounts:
            print('    rowCount', n, 'подготовка', flush=True)
            truncateAll()
            generateDataset(n)
            def run():
                with getDbConnection() as (conn, cur):
                    cur.execute(sqlText)
                    cur.fetchone()
            avgTime = measure(run)
            print('    rowCount', n, 'готово среднее', f'{avgTime:.6f}', flush=True)
            series.append((n, avgTime))
        results[name] = series
        print('  JOIN', name, 'готово', flush=True)
    print('Замер JOIN завершен', flush=True)
    return results


def selectPk(rowCounts):
    print('Замер SELECT PK старт', rowCounts, flush=True)
    results = {"with_pk": [], "no_pk": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("SELECT viewer_id FROM viewer ORDER BY viewer_id LIMIT 1 OFFSET %s", (max(0, n//2 -1),))
            row = cur.fetchone()
            if row is None:
                targetId = 1
            else:
                targetId = row[0]
            def runPk():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT * FROM viewer WHERE viewer_id=%s", (targetId,))
                    cur2.fetchone()
            avgPk = measure(runPk)
            cur.execute("DROP TABLE IF EXISTS viewer_noidx")
            cur.execute("CREATE TABLE viewer_noidx AS SELECT first_name,last_name,email,phone_number,viewer_id FROM viewer")
            conn.commit()
            def runNoPk():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT * FROM viewer_noidx WHERE viewer_id=%s", (targetId,))
                    cur3.fetchone()
            avgNo = measure(runNoPk)
        results["with_pk"].append((n, avgPk))
        results["no_pk"].append((n, avgNo))
        print('  rowCount', n, 'готово avgPk', f'{avgPk:.6f}', 'avgNoPk', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT PK завершен', flush=True)
    return results


def selectStringIndex(rowCounts):
    print('Замер SELECT строка индекс старт', rowCounts, flush=True)
    results = {"with_index": [], "no_index": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("SELECT email FROM viewer ORDER BY viewer_id LIMIT 1")
            row = cur.fetchone()
            if row is None:
                targetEmail = 'none'
            else:
                targetEmail = row[0]
            def runIdx():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT * FROM viewer WHERE email=%s", (targetEmail,))
                    cur2.fetchone()
            avgIdx = measure(runIdx)
            cur.execute("DROP TABLE IF EXISTS viewer_copy_noidx")
            cur.execute("CREATE TABLE viewer_copy_noidx AS SELECT * FROM viewer")
            conn.commit()
            def runNoIdx():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT * FROM viewer_copy_noidx WHERE email=%s", (targetEmail,))
                    cur3.fetchone()
            avgNo = measure(runNoIdx)
        results["with_index"].append((n, avgIdx))
        results["no_index"].append((n, avgNo))
        print('  rowCount', n, 'готово avgIdx', f'{avgIdx:.6f}', 'avgNoIdx', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT строка индекс завершен', flush=True)
    return results


def selectWord(rowCounts):
    print('Замер SELECT слово старт', rowCounts, flush=True)
    results = {"with_index": [], "no_index": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("SELECT title FROM movie ORDER BY movie_id LIMIT 1")
            row = cur.fetchone()
            if row is None:
                targetTitle = 'none'
            else:
                targetTitle = row[0]
            cur.execute("CREATE INDEX IF NOT EXISTS movie_title_idx ON movie(title)")
            conn.commit()
            def runIdx():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT * FROM movie WHERE title=%s", (targetTitle,))
                    cur2.fetchone()
            avgIdx = measure(runIdx)
            cur.execute("DROP INDEX IF EXISTS movie_title_idx")
            conn.commit()
            def runNo():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT * FROM movie WHERE title=%s", (targetTitle,))
                    cur3.fetchone()
            avgNo = measure(runNo)
        results["with_index"].append((n, avgIdx))
        results["no_index"].append((n, avgNo))
        print('  rowCount', n, 'готово avgIdx', f'{avgIdx:.6f}', 'avgNoIdx', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT слово завершен', flush=True)
    return results


def selectWordsMulti(rowCounts):
    print('Замер SELECT несколько условий старт', rowCounts, flush=True)
    results = {"with_index": [], "no_index": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("CREATE INDEX IF NOT EXISTS movie_genre_age_idx ON movie(genre, age_restriction)")
            conn.commit()
            def runIdx():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT COUNT(*) FROM movie WHERE genre='Action' AND age_restriction >= 0")
                    cur2.fetchone()
            avgIdx = measure(runIdx)
            cur.execute("DROP INDEX IF EXISTS movie_genre_age_idx")
            conn.commit()
            def runNo():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT COUNT(*) FROM movie WHERE genre='Action' AND age_restriction >= 0")
                    cur3.fetchone()
            avgNo = measure(runNo)
        results["with_index"].append((n, avgIdx))
        results["no_index"].append((n, avgNo))
        print('  rowCount', n, 'готово avgIdx', f'{avgIdx:.6f}', 'avgNoIdx', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT несколько условий завершен', flush=True)
    return results


def selectNumber(rowCounts):
    print('Замер SELECT число старт', rowCounts, flush=True)
    results = {"with_index": [], "no_index": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("CREATE INDEX IF NOT EXISTS hall_seat_idx ON hall(seat_count)")
            conn.commit()
            def runIdx():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT COUNT(*) FROM hall WHERE seat_count >= 50")
                    cur2.fetchone()
            avgIdx = measure(runIdx)
            cur.execute("DROP INDEX IF EXISTS hall_seat_idx")
            conn.commit()
            def runNo():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT COUNT(*) FROM hall WHERE seat_count >= 50")
                    cur3.fetchone()
            avgNo = measure(runNo)
        results["with_index"].append((n, avgIdx))
        results["no_index"].append((n, avgNo))
        print('  rowCount', n, 'готово avgIdx', f'{avgIdx:.6f}', 'avgNoIdx', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT число завершен', flush=True)
    return results


def selectString(rowCounts):
    print('Замер SELECT строка старт', rowCounts, flush=True)
    results = {"with_index": [], "no_index": []}
    for n in rowCounts:
        print('  rowCount', n, 'подготовка', flush=True)
        truncateAll()
        generateDataset(n)
        with getDbConnection() as (conn, cur):
            cur.execute("CREATE INDEX IF NOT EXISTS cinema_city_idx ON cinema(city)")
            conn.commit()
            def runIdx():
                with getDbConnection() as (conn2, cur2):
                    cur2.execute("SELECT COUNT(*) FROM cinema WHERE city='C'")
                    cur2.fetchone()
            avgIdx = measure(runIdx)
            cur.execute("DROP INDEX IF EXISTS cinema_city_idx")
            conn.commit()
            def runNo():
                with getDbConnection() as (conn3, cur3):
                    cur3.execute("SELECT COUNT(*) FROM cinema WHERE city='C'")
                    cur3.fetchone()
            avgNo = measure(runNo)
        results["with_index"].append((n, avgIdx))
        results["no_index"].append((n, avgNo))
        print('  rowCount', n, 'готово avgIdx', f'{avgIdx:.6f}', 'avgNoIdx', f'{avgNo:.6f}', flush=True)
    print('Замер SELECT строка завершен', flush=True)
    return results


def saveCombinedChart(seriesDict, title, fileName, saveDir, isRaster):
    builder = PlotBuilder(saveDir)
    builder.buildChart(seriesDict, title, 'Строки', 'Время (с)', fileName, isRaster)


def measurePkIndexEffect(rowCounts, outputDir, raster):
    results = {"with_pk": [], "no_pk": []}
    with getDbConnection() as (conn, cur):
        for n in rowCounts:
            print('PK тест размер', n, flush=True)
            cur.execute("DROP TABLE IF EXISTS pk_test")
            cur.execute("DROP TABLE IF EXISTS nopk_test")
            cur.execute("CREATE TABLE pk_test (id INT PRIMARY KEY, payload INT)")
            cur.execute("CREATE TABLE nopk_test (id INT, payload INT)")
            rows = []
            for i in range(1, n + 1):
                rows.append((i, random.randint(0, 1000)))
            cur.executemany("INSERT INTO pk_test (id,payload) VALUES (%s,%s)", rows)
            cur.executemany("INSERT INTO nopk_test (id,payload) VALUES (%s,%s)", rows)
            conn.commit()
            timesWith = []
            for r in range(PK_RUNS):
                def runBatchPk():
                    for q in range(PK_QUERIES_PER_RUN):
                        targetId = random.randint(1, n)
                        cur.execute("SELECT * FROM pk_test WHERE id=%s", (targetId,))
                        cur.fetchone()
                startT = timeit.default_timer()
                runBatchPk()
                endT = timeit.default_timer()
                timesWith.append(endT - startT)
            timesNo = []
            for r in range(PK_RUNS):
                def runBatchNo():
                    for q in range(PK_QUERIES_PER_RUN):
                        targetId = random.randint(1, n)
                        cur.execute("SELECT * FROM nopk_test WHERE id=%s", (targetId,))
                        cur.fetchone()
                startT = timeit.default_timer()
                runBatchNo()
                endT = timeit.default_timer()
                timesNo.append(endT - startT)
            medianWith = _median(timesWith)
            medianNo = _median(timesNo)
            results['with_pk'].append((n, medianWith))
            results['no_pk'].append((n, medianNo))
            print('  pk медиана', f'{medianWith:.6f}', 'no_pk медиана', f'{medianNo:.6f}', flush=True)
    builder = PlotBuilder(outputDir)
    seriesDict = {}
    xsPk = []
    ysPk = []
    xsNo = []
    ysNo = []
    for p in results['with_pk']:
        xsPk.append(p[0])
        ysPk.append(p[1])
    for p in results['no_pk']:
        xsNo.append(p[0])
        ysNo.append(p[1])
    seriesDict['PK'] = (xsPk, ysPk)
    seriesDict['Без PK'] = (xsNo, ysNo)
    builder.buildChart(seriesDict, 'Время выполнения SELECT WHERE с PK и без PK (медиана 5×100)', 'Строки', 'Время (с)', 'select_pk', raster)
    return results


def measureStringIndexExperiment(rowCounts, outputDir, raster):
    results = {"with_index": [], "no_index": []}
    with getDbConnection() as (conn, cur):
        for n in rowCounts:
            print('STR IDX эксперимент размер', n, flush=True)
            cur.execute("DROP TABLE IF EXISTS str_idx_exp")
            cur.execute("CREATE TABLE str_idx_exp (id SERIAL PRIMARY KEY, val VARCHAR(64))")
            rows = []
            for i in range(n):
                rows.append(('val_' + str(i),))
            cur.executemany("INSERT INTO str_idx_exp (val) VALUES (%s)", rows)
            conn.commit()
            cur.execute("SELECT val FROM str_idx_exp")
            allVals = [r[0] for r in cur.fetchall()]
            sampleVals = []
            for i in range(STRING_INDEX_SAMPLE_QUERIES):
                sampleVals.append(allVals[random.randint(0, len(allVals) - 1)])
            # сначала без индекса
            startNo = timeit.default_timer()
            for v in sampleVals:
                cur.execute("SELECT * FROM str_idx_exp WHERE val=%s", (v,))
                cur.fetchone()
            endNo = timeit.default_timer()
            avgNo = (endNo - startNo) / STRING_INDEX_SAMPLE_QUERIES
            # теперь с индексом
            cur.execute("CREATE INDEX IF NOT EXISTS str_idx_exp_val_idx ON str_idx_exp(val)")
            conn.commit()
            startIdx = timeit.default_timer()
            for v in sampleVals:
                cur.execute("SELECT * FROM str_idx_exp WHERE val=%s", (v,))
                cur.fetchone()
            endIdx = timeit.default_timer()
            avgIdx = (endIdx - startIdx) / STRING_INDEX_SAMPLE_QUERIES
            results['with_index'].append((n, avgIdx))
            results['no_index'].append((n, avgNo))
            print('  avg with index', f'{avgIdx:.6f}', 'avg no index', f'{avgNo:.6f}', flush=True)
    builder = PlotBuilder(outputDir)
    seriesDict = {
        'C индексом': ([p[0] for p in results['with_index']], [p[1] for p in results['with_index']]),
        'Без индекса': ([p[0] for p in results['no_index']], [p[1] for p in results['no_index']])
    }
    builder.buildChart(seriesDict, 'Время выполнения SELECT WHERE по строковому полю (с индексом и без)', 'Строки', 'Время (с)', 'select_string_index', raster)
    return results


def measureFtsSingleWordExperiment(rowCounts, outputDir, raster):
    print('FTS эксперимент (одно слово) старт', flush=True)
    results = { 'fts_index': [], 'plain': [] }
    with getDbConnection() as (conn, cur):
        for n in rowCounts:
            print('  размер', n, flush=True)
            cur.execute('DROP TABLE IF EXISTS violations_plain')
            cur.execute('DROP TABLE IF EXISTS violations_fts')
            cur.execute('CREATE TABLE violations_plain (id SERIAL PRIMARY KEY, description TEXT)')
            cur.execute('CREATE TABLE violations_fts (id SERIAL PRIMARY KEY, description TEXT)')
            rowsPlain = []
            # формируем описания с контролируемым распределением слов
            for i in range(n):
                # каждое описание: 12 слов случайных из словаря
                words = [random.choice(FTS_DICTIONARY) for _ in range(12)]
                rowsPlain.append((' '.join(words),))
            cur.executemany('INSERT INTO violations_plain (description) VALUES (%s)', rowsPlain)
            cur.executemany('INSERT INTO violations_fts (description) VALUES (%s)', rowsPlain)
            # создаем GIN индекс (expression index) для FTS
            cur.execute("CREATE INDEX violations_fts_desc_fts_idx ON violations_fts USING GIN (to_tsvector('simple', description))")
            conn.commit()
            # собираем выборку слов для запросов
            sampleWords = [random.choice(FTS_DICTIONARY) for _ in range(FTS_SAMPLE_QUERIES)]
            # plain (ILIKE)
            startPlain = timeit.default_timer()
            for w in sampleWords:
                # шаблон: ищем слово как подстроку с пробелами/началом/концом (упрощенно)
                cur.execute("SELECT COUNT(*) FROM violations_plain WHERE description ILIKE %s", (f'% {w} %',))
                cur.fetchone()
            endPlain = timeit.default_timer()
            avgPlain = (endPlain - startPlain) / FTS_SAMPLE_QUERIES
            # fts (to_tsquery)
            startFts = timeit.default_timer()
            for w in sampleWords:
                cur.execute("SELECT COUNT(*) FROM violations_fts WHERE to_tsvector('simple', description) @@ to_tsquery('simple', %s)", (w,))
                cur.fetchone()
            endFts = timeit.default_timer()
            avgFts = (endFts - startFts) / FTS_SAMPLE_QUERIES
            results['fts_index'].append((n, avgFts))
            results['plain'].append((n, avgPlain))
            print('    avg fts', f'{avgFts:.6f}', 'avg plain', f'{avgPlain:.6f}', flush=True)
    builder = PlotBuilder(outputDir)
    seriesDict = {
        'FTS индекс': ([p[0] for p in results['fts_index']], [p[1] for p in results['fts_index']]),
        'Без индекса': ([p[0] for p in results['plain']], [p[1] for p in results['plain']])
    }
    builder.buildChart(seriesDict, 'FTS: одно слово', 'строк', 'секунд', 'select_fts_single', raster)
    print('FTS эксперимент (одно слово) завершен', flush=True)
    return results


def measureFtsMultiWordExperiment(rowCounts, outputDir, raster):
    print('FTS эксперимент (два слова) старт', flush=True)
    results = { 'fts_index': [], 'plain': [] }
    with getDbConnection() as (conn, cur):
        for n in rowCounts:
            print('  размер', n, flush=True)
            cur.execute('DROP TABLE IF EXISTS violations_plain_multi')
            cur.execute('DROP TABLE IF EXISTS violations_fts_multi')
            cur.execute('CREATE TABLE violations_plain_multi (id SERIAL PRIMARY KEY, description TEXT)')
            cur.execute('CREATE TABLE violations_fts_multi (id SERIAL PRIMARY KEY, description TEXT)')
            rowsPlain = []
            for i in range(n):
                words = [random.choice(FTS_DICTIONARY) for _ in range(14)]
                rowsPlain.append((' '.join(words),))
            cur.executemany('INSERT INTO violations_plain_multi (description) VALUES (%s)', rowsPlain)
            cur.executemany('INSERT INTO violations_fts_multi (description) VALUES (%s)', rowsPlain)
            cur.execute("CREATE INDEX violations_fts_multi_desc_fts_idx ON violations_fts_multi USING GIN (to_tsvector('simple', description))")
            conn.commit()
            # формируем пары слов
            samplePairs = []
            for i in range(FTS_MULTI_SAMPLE_QUERIES):
                w1 = random.choice(FTS_DICTIONARY)
                w2 = random.choice(FTS_DICTIONARY)
                # допускаем одинаковые, но чаще разные
                if w1 == w2 and random.random() < 0.7:
                    # заставим различаться
                    w2 = random.choice(FTS_DICTIONARY)
                samplePairs.append((w1, w2))
            # plain (ILIKE обе части)
            startPlain = timeit.default_timer()
            for (w1, w2) in samplePairs:
                cur.execute("SELECT COUNT(*) FROM violations_plain_multi WHERE description ILIKE %s AND description ILIKE %s", (f'% {w1} %', f'% {w2} %'))
                cur.fetchone()
            endPlain = timeit.default_timer()
            avgPlain = (endPlain - startPlain) / FTS_MULTI_SAMPLE_QUERIES
            # fts (to_tsquery с &)
            startFts = timeit.default_timer()
            for (w1, w2) in samplePairs:
                tsq = f"{w1} & {w2}"
                cur.execute("SELECT COUNT(*) FROM violations_fts_multi WHERE to_tsvector('simple', description) @@ to_tsquery('simple', %s)", (tsq,))
                cur.fetchone()
            endFts = timeit.default_timer()
            avgFts = (endFts - startFts) / FTS_MULTI_SAMPLE_QUERIES
            results['fts_index'].append((n, avgFts))
            results['plain'].append((n, avgPlain))
            print('    avg fts', f'{avgFts:.6f}', 'avg plain', f'{avgPlain:.6f}', flush=True)
    builder = PlotBuilder(outputDir)
    seriesDict = {
        'FTS индекс': ([p[0] for p in results['fts_index']], [p[1] for p in results['fts_index']]),
        'Без индекса': ([p[0] for p in results['plain']], [p[1] for p in results['plain']])
    }
    builder.buildChart(seriesDict, 'FTS: два слова', 'строк', 'секунд', 'select_fts_multi', raster)
    print('FTS эксперимент (два слова) завершен', flush=True)
    return results


def optimizedRunAllResearch(rowCounts, outputDir, raster):
    deleteResults = {}
    joinResults = {}
    targetDeleteTables = ['viewer','movie','cinema','hall','session','viewer_profile','movie_review','favorite_movies','ticket']
    joinDefs = [
        ("cinema_hall", "SELECT COUNT(*) FROM hall h JOIN cinema c ON h.cinema_id=c.cinema_id"),
        ("hall_session_movie", "SELECT COUNT(*) FROM session s JOIN hall h ON s.hall_id=h.hall_id JOIN movie m ON s.movie_id=m.movie_id"),
        ("viewer_profile", "SELECT COUNT(*) FROM viewer_profile vp JOIN viewer v ON vp.viewer_id=v.viewer_id"),
        ("viewer_favorites_movie", "SELECT COUNT(*) FROM favorite_movies f JOIN viewer v ON f.viewer_id=v.viewer_id JOIN movie m ON f.movie_id=m.movie_id"),
        ("viewer_reviews_movie", "SELECT COUNT(*) FROM movie_review r JOIN viewer v ON r.viewer_id=v.viewer_id JOIN movie m ON r.movie_id=m.movie_id"),
        ("ticket_session_viewer", "SELECT COUNT(*) FROM ticket t JOIN session s ON t.session_id=s.session_id JOIN viewer v ON t.viewer_id=v.viewer_id")
    ]
    for name in targetDeleteTables:
        deleteResults[name] = []
    for name, _ in joinDefs:
        joinResults[name] = []
    for rowCount in rowCounts:
        print('ROWCOUNT', rowCount, 'старт подготовки', flush=True)
        truncateAll()
        generateDataset(rowCount)
        with getDbConnection() as (conn, cur):
            print('  DELETE замеры', flush=True)
            for tableName in targetDeleteTables:
                deleteSql = insertDeleteBatch(tableName, min(50, rowCount), conn, cur)
                conn.commit()
                def runDelete():
                    cur.execute(deleteSql)
                times = []
                for r in range(REPEATS_DEFAULT):
                    startT = timeit.default_timer()
                    runDelete()
                    endT = timeit.default_timer()
                    times.append(endT - startT)
                total = 0.0
                for v in times:
                    total = total + v
                avgDelete = total / len(times)
                deleteResults[tableName].append((rowCount, avgDelete))
                print('   ', tableName, 'avg', f'{avgDelete:.6f}', flush=True)
            print('  JOIN замеры', flush=True)
            for jname, jsql in joinDefs:
                times = []
                for r in range(REPEATS_DEFAULT):
                    startJ = timeit.default_timer()
                    cur.execute(jsql)
                    cur.fetchone()
                    endJ = timeit.default_timer()
                    times.append(endJ - startJ)
                total = 0.0
                for v in times:
                    total = total + v
                avgJoin = total / len(times)
                joinResults[jname].append((rowCount, avgJoin))
                print('   ', jname, 'avg', f'{avgJoin:.6f}', flush=True)
        print('ROWCOUNT', rowCount, 'готов', flush=True)
    saveCombinedChart(buildSeriesDict(deleteResults), 'Удаление по условию', 'delete_where', outputDir, raster)
    saveCombinedChart(buildSeriesDict(joinResults), 'JOIN объединения', 'join_performance', outputDir, raster)
    return {
        'delete': deleteResults,
        'joins': joinResults
    }


def runAllResearch(rowCounts=None, repeats=None, outputDir='results', raster=True, includePkExperiment=False, includeIndexExperiments=False, includeStringIndexExperiment=False, includeFtsExperiment=False):
    if includeIndexExperiments and not includePkExperiment:
        includePkExperiment = True
    print('Старт исследований', flush=True)
    if rowCounts is None:
        rowCounts = ROW_COUNTS_DEFAULT
    if repeats is not None:
        global REPEATS_DEFAULT
        REPEATS_DEFAULT = repeats
    if not os.path.isdir(outputDir):
        os.makedirs(outputDir, exist_ok=True)
    resetDatabase()
    if USE_OPTIMIZED_FLOW:
        baseResults = optimizedRunAllResearch(rowCounts, outputDir, raster)
        pkIndexResults = None
        stringIdxResults = None
        ftsResults = None
        ftsMultiResults = None
        if includePkExperiment:
            pkIndexResults = measurePkIndexEffect(PK_ROW_COUNTS, outputDir, raster)
        if includeStringIndexExperiment:
            stringIdxResults = measureStringIndexExperiment(STRING_INDEX_ROW_COUNTS, outputDir, raster)
        if includeFtsExperiment:
            ftsResults = measureFtsSingleWordExperiment(FTS_ROW_COUNTS, outputDir, raster)
            ftsMultiResults = measureFtsMultiWordExperiment(FTS_ROW_COUNTS, outputDir, raster)
        print('Исследования завершены', flush=True)
        return {
            'base': baseResults,
            'pk_index': pkIndexResults,
            'string_index': stringIdxResults,
            'fts_single': ftsResults,
            'fts_multi': ftsMultiResults
        }
    baseResults = optimizedRunAllResearch(rowCounts, outputDir, raster)
    pkIndexResults = None
    stringIdxResults = None
    ftsResults = None
    ftsMultiResults = None
    if includePkExperiment:
        pkIndexResults = measurePkIndexEffect(PK_ROW_COUNTS, outputDir, raster)
    if includeStringIndexExperiment:
        stringIdxResults = measureStringIndexExperiment(STRING_INDEX_ROW_COUNTS, outputDir, raster)
    if includeFtsExperiment:
        ftsResults = measureFtsSingleWordExperiment(FTS_ROW_COUNTS, outputDir, raster)
        ftsMultiResults = measureFtsMultiWordExperiment(FTS_ROW_COUNTS, outputDir, raster)
    print('Исследования завершены', flush=True)
    return {
        'base': baseResults,
        'pk_index': pkIndexResults,
        'string_index': stringIdxResults,
        'fts_single': ftsResults,
        'fts_multi': ftsMultiResults
    }
