from lib.databaseConnection import getDbConnection

class SandboxManager:
    def __init__(self, sandboxSchemaName: str = 'sandbox'):
        self.sandboxSchemaName = sandboxSchemaName

    def createSandboxSchema(self):
        with getDbConnection() as (conn, cur):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.sandboxSchemaName};")
            tableNames = self._getPublicTables(cur)
            i = 0
            while i < len(tableNames):
                table = tableNames[i]
                cur.execute(
                    f"CREATE TABLE {self.sandboxSchemaName}.{table} (LIKE public.{table} INCLUDING ALL);"
                )
                cur.execute(
                    f"INSERT INTO {self.sandboxSchemaName}.{table} SELECT * FROM public.{table};"
                )
                i = i + 1

    def dropSandboxSchema(self):
        with getDbConnection() as (conn, cur):
            cur.execute(f"DROP SCHEMA IF EXISTS {self.sandboxSchemaName} CASCADE;")

    def _getPublicTables(self, cur):
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
        )
        tables = []
        rows = cur.fetchall()
        i = 0
        while i < len(rows):
            tables.append(rows[i][0])
            i = i + 1
        return tables
