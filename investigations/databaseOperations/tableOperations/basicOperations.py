import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME, STRING_QUERIES, PK_QUERIES_PER_RUN, REPEATS_DEFAULT
from investigations.sandboxUtils import ensureBaseDataMinimalInSandbox

def selectPrimaryKey(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('SELECT PK исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateMovies(rowCount)

            def selectByPrimaryKey():
                targetId = random.randint(1, rowCount)
                cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie WHERE movie_id = %s", (targetId,))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(selectByPrimaryKey)
            results[rowCount] = avgTime

    return results

def selectStringField(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('SELECT STRING исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)

            def selectByStringField():
                targetIndex = random.randint(1, rowCount)
                targetEmail = "user" + str(targetIndex) + "@example.com"
                cur.execute("SELECT viewer_id, email FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE email = %s", (targetEmail,))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(selectByStringField)
            results[rowCount] = avgTime

    return results

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
                targetDuration = random.randint(90, 180)
                cur.execute("SELECT movie_id, title FROM " + SANDBOX_SCHEMA_NAME + ".movie WHERE duration_minutes = %s", (targetDuration,))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(selectByNumberField)
            results[rowCount] = avgTime

    return results

def insertDeleteBatchInSandbox(tableName, batchSize, conn=None, cur=None):
    shouldClose = False
    if conn is None:
        conn, cur = getDbConnection()
        shouldClose = True

    try:
        dataGenerator = RandomDataGenerator()
        dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")

        if tableName == 'cinema':
            dataGenerator.generateCinemas(batchSize)
        elif tableName == 'movie':
            dataGenerator.generateMovies(batchSize)
        elif tableName == 'viewer':
            dataGenerator.generateViewers(batchSize)
        elif tableName == 'session':
            dataGenerator.generateSessions(batchSize)
        elif tableName == 'ticket':
            dataGenerator.generateTickets(batchSize)
        elif tableName == 'movie_review':
            dataGenerator.generateMovieReviews(batchSize)
        elif tableName == 'favorite_movies':
            dataGenerator.generateFavoriteMovies(batchSize)

        cur.execute("DELETE FROM " + SANDBOX_SCHEMA_NAME + "." + tableName + " WHERE " + tableName + "_id > (SELECT MAX(" + tableName + "_id) - %s FROM " + SANDBOX_SCHEMA_NAME + "." + tableName + ")", (batchSize,))

    finally:
        if shouldClose:
            conn.close()

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

            def deleteWhereOperation():
                targetRating = random.randint(1, 5)
                cur.execute("DELETE FROM " + SANDBOX_SCHEMA_NAME + ".movie_review WHERE rating = %s", (targetRating,))
                deletedCount = cur.rowcount
                conn.rollback()
                return deletedCount

            avgTime, lastResult = measureAverageTime(deleteWhereOperation)
            results[rowCount] = avgTime

    return results
