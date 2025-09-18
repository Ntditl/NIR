import psycopg2
from contextlib import contextmanager
import os

def _readConfig(configPath):
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

@contextmanager
def getDbConnection():
    configPath = os.path.join(os.path.dirname(__file__), '..', 'configsettings.txt')
    config = _readConfig(configPath)
    databaseConfig = {
        "user": config.get("user"),
        "password": config.get("password"),
        "host": config.get("host"),
        "port": int(config.get("port", 5432)),
        "dbname": config.get("dbname")
    }
    databaseConnection = psycopg2.connect(**databaseConfig)
    databaseConnection.set_client_encoding('UTF8')
    dbCursor = databaseConnection.cursor()
    try:
        yield databaseConnection, dbCursor
        databaseConnection.commit()
    except Exception:
        databaseConnection.rollback()
        raise
    finally:
        dbCursor.close()
        databaseConnection.close()

@contextmanager
def getDbSession():
    with getDbConnection() as (conn, cur):
        class Session:
            def execute(self, sql, params=None):
                if params is not None:
                    return cur.execute(sql, params)
                return cur.execute(sql)
            def scalar(self):
                row = cur.fetchone()
                if row is not None:
                    return row[0]
                return None
            def all(self):
                return cur.fetchall()
            def commit(self):
                conn.commit()
        yield Session()
