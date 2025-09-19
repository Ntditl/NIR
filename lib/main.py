import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.tableModels import createAllTables, dropAllTables, recreateAllTables, getTableNames
from lib.sandboxManager import SandboxManager
from lib.randomDataGenerator import RandomDataGenerator
from lib.databaseConnection import getDbConnection
from lib.dataManager import DataManager
from lib.backupManager import BackupManager


def createTables():
    createAllTables(True)
    print("Tables created")


def dropTables():
    dropAllTables(True)
    print("Tables dropped")


def recreateTables():
    recreateAllTables(True)
    print("Tables recreated")


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
    print("Sample data generated")


def showTableCounts():
    with getDbConnection() as (conn, cur):
        tables = getTableNames()
        i = 0
        while i < len(tables):
            tableName = tables[i]
            cur.execute("SELECT COUNT(*) FROM " + tableName)
            count = cur.fetchone()[0]
            print(tableName, "has", count, "rows")
            i = i + 1


def createSandbox():
    sandbox = SandboxManager()
    sandbox.createSandboxSchema()
    print("Sandbox created")


def truncateTable():
    tableName = input("Enter table name to truncate: ")
    if not tableName:
        print("Table name cannot be empty.")
        return
    DataManager().truncateTable(tableName)


def backupData():
    backupDir = os.path.join(os.path.dirname(__file__), '..', 'backups')
    manager = BackupManager(backupDir)
    manager.backupAllTables()
    print(f"Backup completed to {backupDir}")


def restoreData():
    backupDir = os.path.join(os.path.dirname(__file__), '..', 'backups')
    manager = BackupManager(backupDir)
    manager.restoreAllTables()
    print(f"Restore completed from {backupDir}")


def main():
    print("Available commands:")
    print("1 - Create tables")
    print("2 - Drop tables")
    print("3 - Recreate tables")
    print("4 - Generate sample data")
    print("5 - Show table counts")
    print("6 - Create sandbox")
    print("7 - Truncate a table")
    print("8 - Backup all tables")
    print("9 - Restore all tables")
    print("0 - Exit")

    while True:
        choice = input("Enter command number: ")

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
            print("Goodbye")
            break
        else:
            print("Unknown command")


if __name__ == '__main__':
    main()
