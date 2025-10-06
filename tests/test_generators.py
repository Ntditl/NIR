import pytest
from lib.data.generators import RandomDataGenerator
from lib.db.connection import getDbConnection
from lib.db.models import recreateAllTables

@pytest.fixture(scope='module', autouse=True)
def setup_database():
    recreateAllTables(withIndexes=True)
    yield

def test_generate_viewers():
    gen = RandomDataGenerator()
    gen.generateViewers(5)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer")
        count = cur.fetchone()[0]
        assert count >= 5

def test_generate_movies():
    gen = RandomDataGenerator()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE movie RESTART IDENTITY CASCADE")

    gen.generateMovies(3)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM movie")
        count = cur.fetchone()[0]
        assert count == 3

        cur.execute("SELECT title, genre, duration_minutes FROM movie LIMIT 1")
        row = cur.fetchone()
        assert row is not None
        assert len(row[0]) > 0
        assert len(row[1]) > 0
        assert row[2] > 0

def test_generate_viewer_profiles():
    gen = RandomDataGenerator()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE viewer_profile RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")

    gen.generateViewers(3)
    gen.generateViewerProfiles(2)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer_profile")
        count = cur.fetchone()[0]
        assert count == 2

def test_generate_favorite_movies():
    gen = RandomDataGenerator()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE favorite_movies RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE movie RESTART IDENTITY CASCADE")

    gen.generateViewers(3)
    gen.generateMovies(3)
    gen.generateFavoriteMovies(4)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM favorite_movies")
        count = cur.fetchone()[0]
        assert count >= 4

def test_email_uniqueness():
    gen = RandomDataGenerator()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")

    gen.generateViewers(10)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(DISTINCT email) as unique_emails, COUNT(*) as total FROM viewer")
        row = cur.fetchone()
        unique_count = row[0]
        total_count = row[1]
        assert unique_count == total_count

def test_unicode_support():
    gen = RandomDataGenerator()

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")

    gen.generateViewers(10)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT first_name, last_name FROM viewer")
        rows = cur.fetchall()
        assert len(rows) >= 10

