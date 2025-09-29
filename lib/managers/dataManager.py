from lib.db.connection import getDbConnection
from lib.db.models import getTableNames
from psycopg2 import sql

class DataManager:
    def truncateTable(self, tableName: str):
        knownTables = getTableNames()
        if tableName not in knownTables:
            print(f"Error: Table '{tableName}' is not a valid table name.")
            return

        with getDbConnection() as (conn, cur):
            print(f"Truncating table {tableName}...")
            query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE").format(
                table=sql.Identifier(tableName)
            )
            cur.execute(query)
            print("Done.")

    def replaceData(self, tableName: str, generatorFunction, rowCount: int):

        self.truncateTable(tableName)
        with getDbConnection() as (conn, cur):
            print(f"Generating {rowCount} new rows for {tableName}...")
            generatorFunction(cur, rowCount)
            print("Done.")
