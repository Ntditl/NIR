import pytest
import psycopg2
from lib.db.connection import getDbConnection, getDbSession


def test_get_connection_returns_valid_connection():
    with getDbConnection() as (dbConnection, dbCursor):
        assert dbConnection is not None
        assert dbCursor is not None
        assert dbConnection.closed == 0


def test_connection_executes_simple_query():
    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute("SELECT 1")
        result = dbCursor.fetchone()
        assert result is not None
        assert result[0] == 1


def test_connection_auto_commits():
    testTableName = 'test_connection_temp_table'
    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"DROP TABLE IF EXISTS {testTableName}")
        dbCursor.execute(f"CREATE TABLE {testTableName} (id INT)")

    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='{testTableName}')")
        tableExists = dbCursor.fetchone()[0]
        assert tableExists is True
        dbCursor.execute(f"DROP TABLE {testTableName}")


def test_connection_rolls_back_on_error():
    testTableName = 'test_connection_rollback_table'
    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"DROP TABLE IF EXISTS {testTableName}")
        dbCursor.execute(f"CREATE TABLE {testTableName} (id INT)")

    try:
        with getDbConnection() as (dbConnection, dbCursor):
            dbCursor.execute(f"INSERT INTO {testTableName} (id) VALUES (1)")
            dbCursor.execute("INVALID SQL STATEMENT")
    except psycopg2.Error:
        pass

    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"SELECT COUNT(*) FROM {testTableName}")
        rowCount = dbCursor.fetchone()[0]
        assert rowCount == 0
        dbCursor.execute(f"DROP TABLE {testTableName}")


def test_connection_closes_after_context():
    with getDbConnection() as (dbConnection, dbCursor):
        connectionId = id(dbConnection)

    with getDbConnection() as (dbConnection, dbCursor):
        newConnectionId = id(dbConnection)
        assert newConnectionId != connectionId


def test_auto_commit_on_insert():
    testTableName = 'test_auto_commit_table'
    with getDbConnection() as (conn, cur):
        cur.execute(f"DROP TABLE IF EXISTS {testTableName}")
        cur.execute(f"CREATE TABLE {testTableName} (id INT, value VARCHAR(50))")

    with getDbConnection() as (conn, cur):
        cur.execute(f"INSERT INTO {testTableName} (id, value) VALUES (1, 'test')")

    with getDbConnection() as (conn, cur):
        cur.execute(f"SELECT COUNT(*) FROM {testTableName}")
        count = cur.fetchone()[0]
        assert count == 1
        cur.execute(f"DROP TABLE {testTableName}")


def test_auto_rollback_prevents_partial_commit():
    testTableName = 'test_rollback_prevention'
    with getDbConnection() as (conn, cur):
        cur.execute(f"DROP TABLE IF EXISTS {testTableName}")
        cur.execute(f"CREATE TABLE {testTableName} (id INT PRIMARY KEY)")

    with getDbConnection() as (conn, cur):
        cur.execute(f"INSERT INTO {testTableName} (id) VALUES (1)")

    try:
        with getDbConnection() as (conn, cur):
            cur.execute(f"INSERT INTO {testTableName} (id) VALUES (2)")
            cur.execute(f"INSERT INTO {testTableName} (id) VALUES (1)")
    except psycopg2.Error:
        pass

    with getDbConnection() as (conn, cur):
        cur.execute(f"SELECT COUNT(*) FROM {testTableName}")
        count = cur.fetchone()[0]
        assert count == 1
        cur.execute(f"DROP TABLE {testTableName}")


def test_session_execute():
    with getDbSession() as session:
        session.execute("SELECT 1")
        result = session.scalar()
        assert result == 1


def test_session_scalar_returns_first_column():
    with getDbSession() as session:
        session.execute("SELECT 42, 'test'")
        result = session.scalar()
        assert result == 42


def test_session_all_returns_all_rows():
    testTableName = 'test_session_all_temp'
    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"DROP TABLE IF EXISTS {testTableName}")
        dbCursor.execute(f"CREATE TABLE {testTableName} (id INT)")
        dbCursor.execute(f"INSERT INTO {testTableName} (id) VALUES (1), (2), (3)")

    with getDbSession() as session:
        session.execute(f"SELECT id FROM {testTableName} ORDER BY id")
        results = session.all()
        assert len(results) == 3
        assert results[0][0] == 1
        assert results[1][0] == 2
        assert results[2][0] == 3

    with getDbConnection() as (dbConnection, dbCursor):
        dbCursor.execute(f"DROP TABLE {testTableName}")


def test_session_scalar_returns_none_when_no_rows():
    with getDbSession() as session:
        session.execute("SELECT 1 WHERE FALSE")
        result = session.scalar()
        assert result is None
