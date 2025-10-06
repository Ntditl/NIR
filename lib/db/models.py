TABLE_NAMES = [
    "viewer", "viewer_profile", "movie", "favorite_movies"
]

def getCreateTableStatements():
    return [
        """
        CREATE TABLE viewer (
            viewer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            birth_date DATE NOT NULL
        );
        """,
        """
        CREATE TABLE viewer_profile (
            viewer_id INT PRIMARY KEY REFERENCES viewer(viewer_id) ON DELETE CASCADE,
            nickname VARCHAR(100) NOT NULL,
            avatar_path VARCHAR(500),
            theme VARCHAR(50),
            registration_date DATE NOT NULL
        );
        """,
        """
        CREATE TABLE movie (
            movie_id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            genre VARCHAR(100) NOT NULL,
            duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
            release_date DATE NOT NULL
        );
        """,
        """
        CREATE TABLE favorite_movies (
            viewer_id INT NOT NULL REFERENCES viewer(viewer_id) ON DELETE CASCADE,
            movie_id INT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
            added_date DATE NOT NULL,
            PRIMARY KEY (viewer_id, movie_id)
        );
        """
    ]

def getCreateIndexStatements():
    return [
        """CREATE INDEX ON favorite_movies (viewer_id);""",
        """CREATE INDEX ON favorite_movies (movie_id);"""
    ]

def getTriggerStatements():
    return []

def getCreateTablesSql(includeTriggers=False):
    sql = []
    for stmt in getCreateTableStatements():
        sql.append(stmt)
    for stmt in getCreateIndexStatements():
        sql.append(stmt)
    if includeTriggers:
        for stmt in getTriggerStatements():
            sql.append(stmt)
    return sql

def getDropTablesSql():
    sqlList = []
    for name in reversed(TABLE_NAMES):
        sqlList.append(f"DROP TABLE IF EXISTS {name} CASCADE;")
    return sqlList

def getTableNames():
    return TABLE_NAMES

def tableExists(tableName):
    from lib.db.connection import getDbConnection
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (tableName,))
        row = cur.fetchone()
        if row is None:
            return False
        return True

def createAllTables(withIndexes=True):
    from lib.db.connection import getDbConnection
    statements = []
    for stmt in getCreateTableStatements():
        statements.append(stmt)
    if withIndexes:
        for stmt in getCreateIndexStatements():
            statements.append(stmt)
    with getDbConnection() as (conn, cur):
        for sql in statements:
            cur.execute(sql)

def enableTriggers():
    from lib.db.connection import getDbConnection
    statements = getTriggerStatements()
    from lib.db.connection import getDbConnection as connFn
    with getDbConnection() as (conn, cur):
        for sql in statements:
            cur.execute(sql)

def dropAllTables():
    from lib.db.connection import getDbConnection
    statements = getDropTablesSql()
    with getDbConnection() as (conn, cur):
        for sql in statements:
            cur.execute(sql)

def recreateAllTables(withIndexes=True):
    dropAllTables()
    createAllTables(withIndexes=withIndexes)
