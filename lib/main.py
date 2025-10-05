import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.models import createAllTables, dropAllTables, recreateAllTables, getTableNames
from lib.managers.sandboxManager import SandboxManager
from lib.data.generators import RandomDataGenerator
from lib.db.connection import getDbConnection
from lib.managers.dataManager import DataManager
from lib.managers.backupManager import BackupManager


def createTables():
    createAllTables(True)
    print("Таблицы созданы")


def dropTables():
    dropAllTables(True)
    print("Таблицы удалены")


def recreateTables():
    recreateAllTables(True)
    print("Таблицы пересозданы")


def generateSampleData():
    generator = RandomDataGenerator()
    generator.generateData(
        viewersCount=10,
        moviesCount=10,
        cinemasCount=3,
        hallsPerCinema=2,
        sessionsPerHall=3,
        favoriteRate=0.2,
        reviewRate=0.2,
        ticketRate=0.3
    )
    print("Примерные данные сгенерированы")


def showTableCounts():
    with getDbConnection() as (conn, cur):
        tables = getTableNames()
        for i in range(len(tables)):
            tableName = tables[i]
            cur.execute("SELECT COUNT(*) FROM " + tableName)
            count = cur.fetchone()[0]
            print(f"{tableName}: {count} строк")


def createSandbox():
    sandbox = SandboxManager()
    sandbox.createSandboxSchema()
    print("Песочница создана")


def truncateTable():
    tableName = input("Введите имя таблицы для очистки: ")
    if not tableName:
        print("Имя таблицы не может быть пустым.")
        return
    DataManager().truncateTable(tableName)


def backupData():
    backupDir = os.path.join(os.path.dirname(__file__), '..', 'backups')
    manager = BackupManager(backupDir)
    manager.backupAllTables()
    print(f"Резервное копирование завершено в {backupDir}")


def restoreData():
    backupDir = os.path.join(os.path.dirname(__file__), '..', 'backups')
    manager = BackupManager(backupDir)
    manager.restoreAllTables()
    print(f"Восстановление выполнено из {backupDir}")


def main():
    print("Доступные команды:")
    print("1 - Создать таблицы")
    print("2 - Удалить таблицы")
    print("3 - Пересоздать таблицы")
    print("4 - Сгенерировать примерные данные")
    print("5 - Показать количество строк в таблицах")
    print("6 - Создать песочницу")
    print("7 - Очистить таблицу")
    print("8 - Создать бэкап всех таблиц")
    print("9 - Восстановить все таблицы")
    print("0 - Выход")

    while True:
        choice = input("Введите номер команды: ")

        if choice == "1":
            createTables()
        elif choice == "2":
            dropTables()
        elif choice == "3":
            recreateTables()
        elif choice == "4":
            generateSampleData()
        elif choice == "5":
            showTableCounts()
        elif choice == "6":
            createSandbox()
        elif choice == "7":
            truncateTable()
        elif choice == "8":
            backupData()
        elif choice == "9":
            restoreData()
        elif choice == "0":
            print("Выход")
            break
        else:
            print("Неизвестная команда")


if __name__ == '__main__':
    main()
