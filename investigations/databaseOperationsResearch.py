import os
import sys
import time
import random
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.managers.sandboxManager import SandboxManager
from investigations.researchUtils import measureAverageTime, SANDBOX_SCHEMA_NAME

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def selectNumberField(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < {count // 2}")
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE movie_id < {count // 2}")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results


def selectDateField(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"SELECT * FROM {SANDBOX_SCHEMA_NAME}.movie WHERE release_date > '2020-01-01'")
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results


def insertMovieData(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            for i in range(count):
                cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) VALUES ('Test Movie', 'Action', 120, '2024-01-01')")
            conn.commit()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Test Movie'")
            conn.commit()

    return results


def measureDeleteWhere(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            cur.execute(f"INSERT INTO {SANDBOX_SCHEMA_NAME}.movie (title, genre, duration_minutes, release_date) SELECT 'Temp', 'Drama', 90, '2024-01-01' FROM generate_series(1, {count})")
            conn.commit()
            startTime = time.perf_counter()
            cur.execute(f"DELETE FROM {SANDBOX_SCHEMA_NAME}.movie WHERE title = 'Temp'")
            conn.commit()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results


def measureJoinOperations(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"""
                SELECT v.viewer_id, m.title
                FROM {SANDBOX_SCHEMA_NAME}.viewer v
                JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                JOIN {SANDBOX_SCHEMA_NAME}.movie m ON fm.movie_id = m.movie_id
                LIMIT {count}
            """)
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results


def measureComplexJoinOperations(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"""
                SELECT v.viewer_id, m.title, vp.nickname
                FROM {SANDBOX_SCHEMA_NAME}.viewer v
                JOIN {SANDBOX_SCHEMA_NAME}.viewer_profile vp ON v.viewer_id = vp.viewer_id
                JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                JOIN {SANDBOX_SCHEMA_NAME}.movie m ON fm.movie_id = m.movie_id
                LIMIT {count}
            """)
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results


def measureManyToManyJoin(rowCounts):
    results = []

    with getDbConnection() as (conn, cur):
        for count in rowCounts:
            startTime = time.perf_counter()
            cur.execute(f"""
                SELECT COUNT(*)
                FROM {SANDBOX_SCHEMA_NAME}.viewer v
                JOIN {SANDBOX_SCHEMA_NAME}.favorite_movies fm ON v.viewer_id = fm.viewer_id
                GROUP BY v.viewer_id
                LIMIT {count}
            """)
            rows = cur.fetchall()
            endTime = time.perf_counter()
            results.append({'count': count, 'time': endTime - startTime})

    return results
