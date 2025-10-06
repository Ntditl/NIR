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
                targetDuration = random.randint(90, 180)
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
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)

            def selectByDateField():
                cur.execute("SELECT viewer_id, first_name, birth_date FROM " + SANDBOX_SCHEMA_NAME + ".viewer WHERE birth_date >= '1990-01-01'")
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
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".favorite_movies RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)
            dataGenerator.generateMovies(rowCount)
            dataGenerator.generateFavoriteMovies(rowCount)

            def deleteByMovieId():
                cur.execute("SELECT movie_id FROM " + SANDBOX_SCHEMA_NAME + ".movie LIMIT 1")
                result = cur.fetchone()
                if result:
                    targetMovieId = result[0]
                    cur.execute("DELETE FROM " + SANDBOX_SCHEMA_NAME + ".favorite_movies WHERE movie_id = %s", (targetMovieId,))
                    return cur.rowcount
                return 0

            avgTime, lastResult = measureAverageTime(deleteByMovieId)
            results[rowCount] = avgTime

    return results

def measureJoinOperations(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('JOIN операции исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".favorite_movies RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".movie RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)
            dataGenerator.generateMovies(rowCount)
            dataGenerator.generateFavoriteMovies(rowCount * 2)

            def joinViewerWithFavorites():
                cur.execute("""
                    SELECT v.viewer_id, v.first_name, m.title, fm.added_date
                    FROM {}.viewer v
                    INNER JOIN {}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    INNER JOIN {}.movie m ON fm.movie_id = m.movie_id
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(joinViewerWithFavorites)
            results[rowCount] = avgTime

    return results

def measureOneToOneJoin(rowCounts):
    results = {}
    ensureBaseDataMinimalInSandbox()

    for rowCount in rowCounts:
        print('One-to-One JOIN исследование rowCount', rowCount, flush=True)

        with getDbConnection() as (conn, cur):
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer_profile RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE " + SANDBOX_SCHEMA_NAME + ".viewer RESTART IDENTITY CASCADE;")

            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateViewers(rowCount)
            dataGenerator.generateViewerProfiles(rowCount)

            def oneToOneJoin():
                cur.execute("""
                    SELECT v.viewer_id, v.first_name, v.email, vp.nickname, vp.theme
                    FROM {}.viewer v
                    INNER JOIN {}.viewer_profile vp ON v.viewer_id = vp.viewer_id
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(oneToOneJoin)
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
                    SELECT v.first_name, v.last_name, m.title, m.genre
                    FROM {}.viewer v
                    INNER JOIN {}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    INNER JOIN {}.movie m ON fm.movie_id = m.movie_id
                    WHERE m.duration_minutes >= 120
                    LIMIT 100
                """.format(SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME, SANDBOX_SCHEMA_NAME))
                return cur.fetchall()

            avgTime, lastResult = measureAverageTime(manyToManyJoin)
            results[rowCount] = avgTime

    return results
