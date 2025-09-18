from lib.databaseConnection import getDbConnection
from lib.tableModels import getTableNames
import os

class BackupManager:
    def __init__(self, backupDirectory: str):
        self.backupDirectory = backupDirectory
        if not os.path.isdir(self.backupDirectory):
            os.makedirs(self.backupDirectory, exist_ok=True)
        self.tableNames = getTableNames()

    def backupAllTables(self) -> None:
        with getDbConnection() as (databaseConnection, dbCursor):
            i = 0
            while i < len(self.tableNames):
                tableName = self.tableNames[i]
                filePath = os.path.join(self.backupDirectory, tableName + ".csv")
                with open(filePath, mode="w", encoding="utf-8", newline="") as outFile:
                    sql = "COPY " + tableName + " TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
                    dbCursor.copy_expert(sql, outFile)
                i = i + 1

    def restoreAllTables(self) -> None:
        with getDbConnection() as (databaseConnection, dbCursor):
            i = len(self.tableNames) - 1
            while i >= 0:
                dbCursor.execute("TRUNCATE TABLE " + self.tableNames[i] + " RESTART IDENTITY CASCADE;")
                i = i - 1
            i = 0
            while i < len(self.tableNames):
                tableName = self.tableNames[i]
                filePath = os.path.join(self.backupDirectory, tableName + ".csv")
                if os.path.exists(filePath):
                    if tableName == 'ticket':
                        dbCursor.execute("ALTER TABLE ticket DISABLE TRIGGER trg_ticket_reserve;")
                    with open(filePath, mode="r", encoding="utf-8", newline="") as inFile:
                        sql = "COPY " + tableName + " FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                        dbCursor.copy_expert(sql, inFile)
                    if tableName == 'ticket':
                        dbCursor.execute("ALTER TABLE ticket ENABLE TRIGGER trg_ticket_reserve;")
                i = i + 1
            i = 0
            while i < len(self.tableNames):
                tableName = self.tableNames[i]
                dbCursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_default LIKE 'nextval%%'",
                    (tableName,)
                )
                rows = dbCursor.fetchall()
                j = 0
                while j < len(rows):
                    col = rows[j][0]
                    dbCursor.execute(
                        "SELECT pg_get_serial_sequence(%s, %s)",
                        (tableName, col)
                    )
                    seqRow = dbCursor.fetchone()
                    if seqRow is not None and seqRow[0] is not None:
                        dbCursor.execute("SELECT COALESCE(MAX(" + col + "), 0) FROM " + tableName)
                        maxId = dbCursor.fetchone()[0]
                        dbCursor.execute(
                            "SELECT setval(%s, %s, %s)",
                            (seqRow[0], maxId if maxId is not None else 0, True if maxId is not None and maxId > 0 else False)
                        )
                    j = j + 1
                i = i + 1
