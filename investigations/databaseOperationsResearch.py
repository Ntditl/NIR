import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

def selectNumberField(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('SELECT NUMBER исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)

            def selectByNumberField():
                targetDuration = random.choice([90, 120, 150, 180])
                cur.execute("SELECT movie_id, title, duration_minutes FROM " + SANDBOX_SCHEMA_NAME + ".movie WHERE duration_minutes = %s", (targetDuration,))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(selectByNumberField)
            results[rowCount] = avgTime

    return results

def selectDateField(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('SELECT DATE исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".session RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateSessions(rowCount)

            def selectByDateField():
                cur.execute("SELECT session_id, session_datetime FROM " + SANDBOX_SCHEMA_NAME + ".session WHERE session_datetime >= CURRENT_DATE")
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(selectByDateField)
            results[rowCount] = avgTime

    return results

def insertMovieData(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for insertCount in rowCounts:
        print('INSERT операции исследование insertCount', insertCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            def insertMovies():
                dataGenerator = RandomDataGenerator()
                dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
                dataGenerator.generateMovies(insertCount)

            avgTime, lastResult = measureAverageTime(insertMovies)
            results[insertCount] = avgTime

    return results

def measureDeleteWhere(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('DELETE WHERE исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovieReviews(rowCount)

            def deleteByRating():
                targetRating = random.randint(1, 5)
                cur.execute("DELETE FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE rating = %s", (targetRating,))
                return cur.rowcount

            avgTime, lastResult = measureAverageTime(deleteByRating)
            results[rowCount] = avgTime

    return results

def measureJoinOperations(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('JOIN операции исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".session RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".ticket RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateSessions(rowCount)
            dataGenerator.generateTickets(rowCount * 2)

            def joinSessionsWithTickets():
                cur.execute("""
                    SELECT s.session_id, s.session_datetime, t.ticket_id, t.seat_number
                    FROM {}.session s
                    INNER JOIN {}.ticket t ON s.session_id = t.session_id
                    WHERE s.session_datetime >= CURRENT_DATE
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(joinSessionsWithTickets)
            results[rowCount] = avgTime

    return results

def measureComplexJoinOperations(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('Сложные JOIN операции исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".session RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".ticket RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie_review RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)
            dataGenerator.generateSessions(rowCount)
            dataGenerator.generateTickets(rowCount * 2)
            dataGenerator.generateMovieReviews(rowCount)

            def complexJoin():
                cur.execute("""
                    SELECT m.title, s.session_datetime, t.seat_number, r.rating
                    FROM {}.movie m
                    INNER JOIN {}.session s ON m.movie_id = s.movie_id
                    INNER JOIN {}.ticket t ON s.session_id = t.session_id
                    LEFT JOIN {}.movie_review r ON m.movie_id = r.movie_id
                    WHERE m.rating >= 7
                    LIMIT 50
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(complexJoin)
            results[rowCount] = avgTime

    return results

def measureManyToManyJoin(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('Many-to-Many JOIN исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".favorite_movies RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)
            dataGenerator.generateViewers(rowCount)
            dataGenerator.generateFavoriteMovies(rowCount * 2)

            def manyToManyJoin():
                cur.execute("""
                    SELECT v.name, m.title
                    FROM {}.viewer v
                    INNER JOIN {}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    INNER JOIN {}.movie m ON fm.movie_id = m.movie_id
                    WHERE v.age >= 18
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(manyToManyJoin)
            results[rowCount] = avgTime

    return results
