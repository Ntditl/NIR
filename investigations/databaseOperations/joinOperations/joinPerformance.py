import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

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

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateSessions(rowCount)
            dataGenerator.generateTickets(rowCount)
            dataGenerator.generateMovieReviews(rowCount)

            def complexJoinOperation():
                cur.execute("""
                    SELECT m.title, s.session_datetime, COUNT(t.ticket_id) as ticketCount, 
                           AVG(mr.rating) as avgRating
                    FROM {}.movie m
                    INNER JOIN {}.session s ON m.movie_id = s.movie_id
                    LEFT JOIN {}.ticket t ON s.session_id = t.session_id
                    LEFT JOIN {}.movie_review mr ON m.movie_id = mr.movie_id
                    GROUP BY m.movie_id, m.title, s.session_id, s.session_datetime
                    ORDER BY avgRating DESC
                    LIMIT 50
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(complexJoinOperation)
            results[rowCount] = avgTime

    return results

def measureManyToManyJoin(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('Many-to-Many JOIN исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".favorite_movies RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateFavoriteMovies(rowCount)

            def manyToManyJoinOperation():
                cur.execute("""
                    SELECT v.email, m.title, fm.added_date
                    FROM {}.viewer v
                    INNER JOIN {}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    INNER JOIN {}.movie m ON fm.movie_id = m.movie_id
                    ORDER BY fm.added_date DESC
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(manyToManyJoinOperation)
            results[rowCount] = avgTime

    return results
