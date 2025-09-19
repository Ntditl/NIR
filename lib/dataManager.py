from lib.databaseConnection import getDbConnection
from lib.tableModels import getTableNames
from psycopg2 import sql

class DataManager:
    def truncateTable(self, tableName: str):
        """
        Удаляет все данные из таблицы и сбрасывает счетчики.
        """
        known_tables = getTableNames()
        if tableName not in known_tables:
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
        """
        Заменяет данные в таблице: сначала удаляет старые, потом генерирует новые.
        generatorFunction - это функция, которая примет (cursor, rowCount) и вставит данные.
        """
        self.truncateTable(tableName)
        with getDbConnection() as (conn, cur):
            print(f"Generating {rowCount} new rows for {tableName}...")
            generatorFunction(cur, rowCount)
            print("Done.")
