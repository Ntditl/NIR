from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator
from lib.managers.sandboxManager import SandboxManager
from investigations.researchUtils import SANDBOX_SCHEMA_NAME

def setupSandboxForResearch():
    print('Настройка песочницы для исследований', flush=True)
    sandboxManager = SandboxManager(SANDBOX_SCHEMA_NAME)
    sandboxManager.createSandboxSchema()
    print('Песочница готова', flush=True)

def cleanupSandboxAfterResearch():
    print('Очистка песочницы после исследований', flush=True)
    sandboxManager = SandboxManager(SANDBOX_SCHEMA_NAME)
    sandboxManager.dropSandboxSchema()
    print('Песочница удалена', flush=True)

def ensureBaseDataMinimalInSandbox():
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM " + SANDBOX_SCHEMA_NAME + ".cinema")
        cinemaCount = cur.fetchone()[0]
        if cinemaCount < 1:
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(SANDBOX_SCHEMA_NAME + ".")
            dataGenerator.generateCinemas(5)
            dataGenerator.generateMovies(10)
            dataGenerator.generateViewers(10)
            dataGenerator.generateSessions(15)

def resetSandboxDatabase():
    print('Сброс песочницы старт', flush=True)
    sandboxManager = SandboxManager(SANDBOX_SCHEMA_NAME)
    sandboxManager.createSandboxSchema()
    print('Сброс песочницы завершен', flush=True)
