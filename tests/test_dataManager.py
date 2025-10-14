import pytest
from lib.managers.dataManager import DataManager
from lib.db.connection import getDbConnection
from lib.db.models import recreateAllTables

@pytest.fixture(scope='module', autouse=True)
def setup_database():
    recreateAllTables(withIndexes=True)
    yield

def test_truncate_table_viewer():
    manager = DataManager()

    with getDbConnection() as (conn, cur):
        cur.execute("INSERT INTO viewer (first_name, last_name, email, birth_date) VALUES ('Test', 'User', 'test@example.com', '1990-01-01')")
        cur.execute("SELECT COUNT(*) FROM viewer")
        count_before = cur.fetchone()[0]
        assert count_before >= 1

    manager.truncateTable('viewer')

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer")
        count_after = cur.fetchone()[0]
        assert count_after == 0

def test_truncate_table_movie():
    manager = DataManager()

    with getDbConnection() as (conn, cur):
        cur.execute("INSERT INTO movie (title, genre, duration_minutes, release_date) VALUES ('Test Movie', 'Action', 120, '2020-01-01')")
        cur.execute("SELECT COUNT(*) FROM movie")
        count_before = cur.fetchone()[0]
        assert count_before >= 1

    manager.truncateTable('movie')

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM movie")
        count_after = cur.fetchone()[0]
        assert count_after == 0

def test_truncate_invalid_table():
    manager = DataManager()
    manager.truncateTable('nonexistent_table')

def test_delete_all_from_table_viewer():
    manager = DataManager()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")
        cur.execute("INSERT INTO viewer (first_name, last_name, email, birth_date) VALUES ('User1', 'Test', 'u1@test.com', '1990-01-01')")
        cur.execute("INSERT INTO viewer (first_name, last_name, email, birth_date) VALUES ('User2', 'Test', 'u2@test.com', '1991-01-01')")
        cur.execute("SELECT COUNT(*) FROM viewer")
        count_before = cur.fetchone()[0]
        assert count_before == 2

    manager.deleteAllFromTable('viewer')

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer")
        count_after = cur.fetchone()[0]
        assert count_after == 0

def test_delete_all_from_table_movie():
    manager = DataManager()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE movie RESTART IDENTITY CASCADE")
        cur.execute("INSERT INTO movie (title, genre, duration_minutes, release_date) VALUES ('Movie1', 'Action', 120, '2020-01-01')")
        cur.execute("INSERT INTO movie (title, genre, duration_minutes, release_date) VALUES ('Movie2', 'Drama', 90, '2021-01-01')")
        cur.execute("SELECT COUNT(*) FROM movie")
        count_before = cur.fetchone()[0]
        assert count_before == 2

    manager.deleteAllFromTable('movie')

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM movie")
        count_after = cur.fetchone()[0]
        assert count_after == 0

def test_replace_all_data_viewer():
    manager = DataManager()
    manager.replaceAllData('viewer', 5)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer")
        count = cur.fetchone()[0]
        assert count == 5

def test_replace_all_data_movie():
    manager = DataManager()
    manager.replaceAllData('movie', 10)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM movie")
        count = cur.fetchone()[0]
        assert count == 10

def test_replace_all_data_viewer_profile():
    manager = DataManager()
    manager.replaceAllData('viewer', 8)
    manager.replaceAllData('viewer_profile', 5)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer_profile")
        count = cur.fetchone()[0]
        assert count == 5

def test_replace_all_data_favorite_movies():
    manager = DataManager()
    manager.replaceAllData('viewer', 10)
    manager.replaceAllData('movie', 10)
    manager.replaceAllData('favorite_movies', 15)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM favorite_movies")
        count = cur.fetchone()[0]
        assert count == 15
