from lib.db.connection import getDbConnection
from lib.db.models import getTableNames
import os
from datetime import datetime, timezone
import subprocess

TIME_FORMAT = '%Y%m%d_%H%M%S'

class BackupManager:
    def __init__(self, backupDirectory: str):
        self.backupDirectory = backupDirectory
        if not os.path.isdir(self.backupDirectory):
            os.makedirs(self.backupDirectory, exist_ok=True)
        self.tableNames = getTableNames()
        self.lastBackupDir = None
        self.dbConfig = self._loadDbConfig()

    def _loadDbConfig(self):
        baseDir = os.path.dirname(__file__)
        configPath = os.path.abspath(os.path.join(baseDir, '..', '..', 'configsettings.txt'))
        config = {}
        with open(configPath, 'r', encoding='utf-8') as fileObj:
            for line in fileObj:
                if '=' in line:
                    parts = line.strip().split('=', 1)
                    if len(parts) == 2:
                        key = parts[0]
                        value = parts[1]
                        config[key] = value
        return config

    def listBackupFolders(self):
        if not os.path.isdir(self.backupDirectory):
            return []
        entries = []
        for name in os.listdir(self.backupDirectory):
            fullPath = os.path.join(self.backupDirectory, name)
            if os.path.isdir(fullPath):
                entries.append(name)
        entries.sort()
        return entries

    def backupAllTables(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime(TIME_FORMAT)
        targetDir = os.path.join(self.backupDirectory, timestamp)
        os.makedirs(targetDir, exist_ok=True)

        backupFile = os.path.join(targetDir, 'backup.sql')

        pgDumpCmd = [
            'pg_dump',
            '-h', self.dbConfig.get('host', 'localhost'),
            '-p', self.dbConfig.get('port', '5432'),
            '-U', self.dbConfig.get('user', 'postgres'),
            '-d', self.dbConfig.get('dbname', 'postgres'),
            '-f', backupFile,
            '--no-owner',
            '--no-privileges',
            '--data-only'
        ]

        for i in range(len(self.tableNames)):
            pgDumpCmd.append('-t')
            pgDumpCmd.append(self.tableNames[i])

        env = os.environ.copy()
        env['PGPASSWORD'] = self.dbConfig.get('password', '')

        result = subprocess.run(pgDumpCmd, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError('pg_dump failed: ' + result.stderr)

        self.lastBackupDir = targetDir
        return targetDir

    def restoreAllTables(self, backupName: str = None) -> None:
        if backupName is None:
            if self.lastBackupDir is not None:
                targetDir = self.lastBackupDir
            else:
                folders = self.listBackupFolders()
                if len(folders) == 0:
                    raise ValueError('no backups found')
                targetDir = os.path.join(self.backupDirectory, folders[-1])
        else:
            targetDir = os.path.join(self.backupDirectory, backupName)

        if not os.path.isdir(targetDir):
            raise ValueError('backup folder not found: ' + targetDir)

        backupFile = os.path.join(targetDir, 'backup.sql')
        if not os.path.exists(backupFile):
            raise ValueError('backup file not found: ' + backupFile)

        with getDbConnection() as (databaseConnection, dbCursor):
            for i in range(len(self.tableNames) - 1, -1, -1):
                dbCursor.execute('TRUNCATE TABLE ' + self.tableNames[i] + ' RESTART IDENTITY CASCADE;')

        psqlCmd = [
            'psql',
            '-h', self.dbConfig.get('host', 'localhost'),
            '-p', self.dbConfig.get('port', '5432'),
            '-U', self.dbConfig.get('user', 'postgres'),
            '-d', self.dbConfig.get('dbname', 'postgres'),
            '-f', backupFile,
            '-q'
        ]

        env = os.environ.copy()
        env['PGPASSWORD'] = self.dbConfig.get('password', '')

        result = subprocess.run(psqlCmd, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError('psql restore failed: ' + result.stderr)

        self.lastBackupDir = targetDir
