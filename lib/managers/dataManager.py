from lib.db.connection import getDbConnection
from lib.db.models import getTableNames
from lib.data.generators import RandomDataGenerator
from psycopg2 import sql

class DataManager:
    def __init__(self):
        self.schemaPrefix = ""

    def setSchemaPrefix(self, prefix: str):
        self.schemaPrefix = prefix

    def _getTableName(self, tableName: str):
        return self.schemaPrefix + tableName

    def truncateTable(self, tableName: str):
        knownTables = getTableNames()
        if tableName not in knownTables:
            print(f"Error: Table '{tableName}' is not a valid table name.")
            return

        with getDbConnection() as (conn, cur):
            print(f"Truncating table {self._getTableName(tableName)}...")
            fullTableName = self._getTableName(tableName)
            cur.execute(f"TRUNCATE TABLE {fullTableName} RESTART IDENTITY CASCADE")
            print("Done.")

    def deleteAllFromTable(self, tableName: str):
        knownTables = getTableNames()
        if tableName not in knownTables:
            print(f"Error: Table '{tableName}' is not a valid table name.")
            return

        with getDbConnection() as (conn, cur):
            print(f"Deleting all rows from {self._getTableName(tableName)}...")
            fullTableName = self._getTableName(tableName)
            cur.execute(f"DELETE FROM {fullTableName}")
            deletedCount = cur.rowcount
            print(f"Deleted {deletedCount} rows.")

    def replaceAllData(self, tableName: str, rowCount: int):
        knownTables = getTableNames()
        if tableName not in knownTables:
            print(f"Error: Table '{tableName}' is not a valid table name.")
            return

        self.truncateTable(tableName)

        generator = RandomDataGenerator()
        if self.schemaPrefix != "":
            generator.setSchemaPrefix(self.schemaPrefix)

        print(f"Generating {rowCount} new rows for {tableName}...")

        if tableName == 'viewer':
            generator.generateViewers(rowCount)
        elif tableName == 'viewer_profile':
            generator.generateViewerProfiles(rowCount)
        elif tableName == 'movie':
            generator.generateMovies(rowCount)
        elif tableName == 'favorite_movies':
            generator.generateFavoriteMovies(rowCount)
        else:
            print(f"No generator available for table '{tableName}'")
            return

        print("Done.")
