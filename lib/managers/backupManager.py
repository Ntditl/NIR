from lib.db.connection import getDbConnection
from lib.db.models import getTableNames
import os
from datetime import datetime

TIME_FORMAT = '%Y%m%d_%H%M%S'

class BackupManager:
    def __init__(self, backupDirectory: str):
        self.backupDirectory = backupDirectory
        if not os.path.isdir(self.backupDirectory):
            os.makedirs(self.backupDirectory, exist_ok=True)
        self.tableNames = getTableNames()
        self.lastBackupDir = None

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
        timestamp = datetime.utcnow().strftime(TIME_FORMAT)
        targetDir = os.path.join(self.backupDirectory, timestamp)
        os.makedirs(targetDir, exist_ok=True)
        with getDbConnection() as (databaseConnection, dbCursor):
            for i in range(len(self.tableNames)):
                tableName = self.tableNames[i]
                filePath = os.path.join(targetDir, tableName + '.csv')
                with open(filePath, mode='w', encoding='utf-8', newline='') as outFile:
                    sql = 'COPY ' + tableName + ' TO STDOUT WITH (FORMAT CSV, HEADER TRUE)'
                    dbCursor.copy_expert(sql, outFile)
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
        with getDbConnection() as (databaseConnection, dbCursor):
            for i in range(len(self.tableNames) - 1, -1, -1):
                dbCursor.execute('TRUNCATE TABLE ' + self.tableNames[i] + ' RESTART IDENTITY CASCADE;')
            for i in range(len(self.tableNames)):
                tableName = self.tableNames[i]
                filePath = os.path.join(targetDir, tableName + '.csv')
                if os.path.exists(filePath):
                    if tableName == 'ticket':
                        dbCursor.execute('ALTER TABLE ticket DISABLE TRIGGER trg_ticket_reserve;')
                    with open(filePath, mode='r', encoding='utf-8', newline='') as inFile:
                        sql = 'COPY ' + tableName + ' FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
                        dbCursor.copy_expert(sql, inFile)
                    if tableName == 'ticket':
                        dbCursor.execute('ALTER TABLE ticket ENABLE TRIGGER trg_ticket_reserve;')
            for i in range(len(self.tableNames)):
                tableName = self.tableNames[i]
                dbCursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_default LIKE 'nextval%%'",
                    (tableName,)
                )
                rows = dbCursor.fetchall()
                for j in range(len(rows)):
                    col = rows[j][0]
                    dbCursor.execute(
                        'SELECT pg_get_serial_sequence(%s, %s)',
                        (tableName, col)
                    )
                    seqRow = dbCursor.fetchone()
                    if seqRow is not None and seqRow[0] is not None:
                        dbCursor.execute('SELECT COALESCE(MAX(' + col + '), 0) FROM ' + tableName)
                        maxId = dbCursor.fetchone()[0]
                        dbCursor.execute(
                            'SELECT setval(%s, %s, %s)',
                            (seqRow[0], maxId if maxId is not None else 0, True if maxId is not None and maxId > 0 else False)
                        )
        self.lastBackupDir = targetDir
