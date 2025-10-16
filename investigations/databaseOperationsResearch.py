import os
import sys
from lib.db.connection import getDbConnection
from lib.utils.timing import measureExecutionTime
from investigations.researchUtils import SANDBOX_SCHEMA_NAME

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def selectNumberField(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < {count // 2}")

            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < {count // 2}")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def selectNumberFieldViewer(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE viewer_id < {count // 2}")

            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE viewer_id < {count // 2}")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def selectNumberFieldViewerProfile(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_SCHEMA_NAME}.viewer_profile WHERE viewer_id < {count // 2}")

            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.viewer_profile WHERE viewer_id < {count // 2}")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def selectDateField(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE release_date > '2020-01-01'")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def selectDateFieldViewer(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE birth_date > '2000-01-01'")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def selectDateFieldViewerProfile(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.viewer_profile WHERE registration_date > '2023-01-01'")
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def insertMovieData(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeInsert():
                for i in range(count):
                    cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) VALUES ('Test Movie', 'Action', 120, '2024-01-01')")

            executionTime = measureExecutionTime(executeInsert)
            results.append({'count': count, 'time': executionTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Test Movie'")

    return results


def insertViewerData(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeInsert():
                for i in range(count):
                    cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer (first_name, last_name, email, birth_date) VALUES ('Test', 'User', 'test{i}@example.com', '1990-01-01')")

            executionTime = measureExecutionTime(executeInsert)
            results.append({'count': count, 'time': executionTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE first_name = 'Test' AND last_name = 'User'")

    return results


def insertViewerProfileData(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        cur.execute(f"SELECT viewer_id FROM {SANDBOX_SCHEMA_NAME}.viewer ORDER BY viewer_id LIMIT 1")
        firstViewerRow = cur.fetchone()
        if firstViewerRow is None:
            return results

        firstViewerId = firstViewerRow[0]

        for count in rowCounts:
            insertedViewerIds = []
            for i in range(count):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer (first_name, last_name, email, birth_date) VALUES ('TempProfile', 'User', 'tempprofile{i}@example.com', '1990-01-01') RETURNING viewer_id")
                newViewerId = cur.fetchone()[0]
                insertedViewerIds.append(newViewerId)

            def executeInsert():
                for viewerId in insertedViewerIds:
                    cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer_profile (viewer_id, nickname, registration_date) VALUES ({viewerId}, 'TestNick', '2024-01-01')")

            executionTime = measureExecutionTime(executeInsert)
            results.append({'count': count, 'time': executionTime})

            for viewerId in insertedViewerIds:
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer_profile WHERE viewer_id = {viewerId}")
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE first_name = 'TempProfile'")

    return results


def measureDeleteWhere(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) SELECT 'Temp', 'Drama', 90, '2024-01-01' FROM generate_series(1, {count})")

            def executeDelete():
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Temp'")

            executionTime = measureExecutionTime(executeDelete)
            results.append({'count': count, 'time': executionTime})

    return results


def measureDeleteWhereViewer(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            for i in range(count):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer (first_name, last_name, email, birth_date) VALUES ('TempDelete', 'User', 'tempdel{i}@example.com', '1990-01-01')")

            def executeDelete():
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE first_name = 'TempDelete'")

            executionTime = measureExecutionTime(executeDelete)
            results.append({'count': count, 'time': executionTime})

    return results


def measureDeleteWhereViewerProfile(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            insertedViewerIds = []
            for i in range(count):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer (first_name, last_name, email, birth_date) VALUES ('TempDelProf', 'User', 'tempdelprof{i}@example.com', '1990-01-01') RETURNING viewer_id")
                newViewerId = cur.fetchone()[0]
                insertedViewerIds.append(newViewerId)
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.viewer_profile (viewer_id, nickname, registration_date) VALUES ({newViewerId}, 'TempNickDel', '2024-01-01')")

            def executeDelete():
                cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer_profile WHERE nickname = 'TempNickDel'")

            executionTime = measureExecutionTime(executeDelete)
            results.append({'count': count, 'time': executionTime})

            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.viewer WHERE first_name = 'TempDelProf'")

    return results


def measureJoinOperations(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"""
                    SELECT v.viewer_id, m.title
                    FROM {SANDBOX_SCHEMA_NAME}.viewer v
                    JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    JOIN {SANDBOX_SCHEMA_NAME}.movie m ON fm.movie_id = m.movie_id
                    LIMIT {count}
                """)
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def measureComplexJoinOperations(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"""
                    SELECT v.viewer_id, m.title, vp.nickname
                    FROM {SANDBOX_SCHEMA_NAME}.viewer v
                    JOIN {SANDBOX_SCHEMA_NAME}.viewer_profile vp ON v.viewer_id = vp.viewer_id
                    JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    JOIN {SANDBOX_SCHEMA_NAME}.movie m ON fm.movie_id = m.movie_id
                    LIMIT {count}
                """)
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results


def measureManyToManyJoin(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            def executeQuery():
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {SANDBOX_SCHEMA_NAME}.viewer v
                    JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                    GROUP BY v.viewer_id
                    LIMIT {count}
                """)
                rows = cur.fetchall()
                return rows

            executionTime = measureExecutionTime(executeQuery)
            results.append({'count': count, 'time': executionTime})

    return results
