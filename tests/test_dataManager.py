import pytest
from lib.managers.dataManager import DataManager
from lib.db.connection import getDbConnection
from lib.db.models import recreateAllTables

@pytest.fixture(scope='module', autouse=True)
def setup_database():
    recreateAllTables(withIndexes=True)
    yield

def test_truncate_table():
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

def test_truncate_invalid_table():
    manager = DataManager()
    manager.truncateTable('nonexistent_table')

