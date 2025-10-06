import pytest
from lib.managers.sandboxManager import SandboxManager
from lib.db.connection import getDbConnection
from lib.db.models import recreateAllTables
from lib.data.generators import RandomDataGenerator

SANDBOX_NAME = 'test_sandbox'

@pytest.fixture(scope='module', autouse=True)
def setup_database():
    recreateAllTables(withIndexes=True)
    yield

@pytest.fixture
def sandbox_manager():
    manager = SandboxManager(SANDBOX_NAME)
    yield manager
    manager.dropSandboxSchema()

def test_create_sandbox_schema(sandbox_manager):
    gen = RandomDataGenerator()
    gen.generateViewers(3)
    gen.generateMovies(2)

    sandbox_manager.createSandboxSchema()

    with getDbConnection() as (conn, cur):
        cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_NAME}.viewer")
        count = cur.fetchone()[0]
        assert count == 3

        cur.execute(f"SELECT COUNT(*) FROM {SANDBOX_NAME}.movie")
        count = cur.fetchone()[0]
        assert count == 2

def test_drop_sandbox_schema(sandbox_manager):
    sandbox_manager.createSandboxSchema()
    sandbox_manager.dropSandboxSchema()

    with getDbConnection() as (conn, cur):
        cur.execute(f"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = %s", (SANDBOX_NAME,))
        count = cur.fetchone()[0]
        assert count == 0

