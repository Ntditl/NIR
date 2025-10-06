import pytest
import os
import tempfile
import shutil
from lib.managers.backupManager import BackupManager
from lib.db.connection import getDbConnection
from lib.db.models import recreateAllTables
from lib.data.generators import RandomDataGenerator

@pytest.fixture(scope='function')
def clean_backup_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

@pytest.fixture(scope='module')
def temp_backup_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

@pytest.fixture(scope='module', autouse=True)
def setup_database():
    recreateAllTables(withIndexes=True)
    yield

def test_backup_all_tables(temp_backup_dir):
    gen = RandomDataGenerator()
    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")

    gen.generateViewers(3)

    manager = BackupManager(temp_backup_dir)
    backup_path = manager.backupAllTables()

    assert os.path.exists(backup_path)
    assert os.path.exists(os.path.join(backup_path, 'viewer.csv'))

def test_restore_all_tables(clean_backup_dir):
    from lib.db.models import recreateAllTables

    recreateAllTables(withIndexes=True)

    gen = RandomDataGenerator()
    gen.generateViewers(5)
    gen.generateMovies(3)

    manager = BackupManager(clean_backup_dir)
    backup_path = manager.backupAllTables()
    backup_name = os.path.basename(backup_path)

    with getDbConnection() as (conn, cur):
        cur.execute("TRUNCATE TABLE favorite_movies RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE viewer_profile RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE viewer RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE movie RESTART IDENTITY CASCADE")
        cur.execute("SELECT COUNT(*) FROM viewer")
        assert cur.fetchone()[0] == 0

    manager.restoreAllTables(backup_name)

    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer")
        count = cur.fetchone()[0]
        assert count == 5

def test_list_backup_folders(temp_backup_dir):
    manager = BackupManager(temp_backup_dir)
    manager.backupAllTables()

    folders = manager.listBackupFolders()
    assert len(folders) >= 1
