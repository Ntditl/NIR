import pytest
from lib.db.models import (
    getCreateTableStatements,
    getCreateIndexStatements,
    createAllTables,
    dropAllTables,
    recreateAllTables,
    tableExists,
    getTableNames
)
from lib.db.connection import getDbConnection

def test_get_create_table_statements():
    statements = getCreateTableStatements()
    assert len(statements) == 4
    assert 'CREATE TABLE viewer' in statements[0]
    assert 'CREATE TABLE viewer_profile' in statements[1]
    assert 'CREATE TABLE movie' in statements[2]
    assert 'CREATE TABLE favorite_movies' in statements[3]

def test_get_create_index_statements():
    indexes = getCreateIndexStatements()
    assert len(indexes) == 2
    assert 'favorite_movies' in indexes[0]
    assert 'favorite_movies' in indexes[1]

def test_get_table_names():
    names = getTableNames()
    assert len(names) == 4
    assert 'viewer' in names
    assert 'viewer_profile' in names
    assert 'movie' in names
    assert 'favorite_movies' in names

def test_recreate_all_tables():
    recreateAllTables(withIndexes=True)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('viewer', 'viewer_profile', 'movie', 'favorite_movies')")
        count = cur.fetchone()[0]
        assert count == 4

def test_table_exists():
    recreateAllTables(withIndexes=True)
    assert tableExists('viewer') == True
    assert tableExists('movie') == True
    assert tableExists('nonexistent_table') == False

def test_drop_all_tables():
    recreateAllTables(withIndexes=True)
    dropAllTables()

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('viewer', 'viewer_profile', 'movie', 'favorite_movies')")
        count = cur.fetchone()[0]
        assert count == 0

