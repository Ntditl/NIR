import sys
import os
import json

# Добавляем корень проекта в путь, чтобы работали импорты из lib
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем функции для исследований
from generationSpeed import measureGenerationSpeed
from queryPerformance import measureQueryPerformance
from joinAnalysis import analyzeJoinPerformance
from indexPerformance import measureIndexPerformance
from lib.tableModels import recreateAllTables

def loadConfig():
    """Загружает параметры исследования из paramsSettings.json."""
    configPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    if not os.path.exists(configPath):
        print(f"Ошибка: Файл конфигурации не найден по пути {configPath}")
        return None
    with open(configPath, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config

def prepareDatabase():
    """Пересоздает все таблицы для чистоты эксперимента."""
    print("Preparing database: dropping and recreating all tables...")
    recreateAllTables(verbose=False)
    print("Database is ready.")

def runGenerationSpeed():
    """Запускает исследование скорости генерации данных (в памяти)."""
    config = loadConfig()
    if config is None:
        return

    print("\n--- Running Generation Speed Benchmark ---")
    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    tablesConfig = config.get('tables', {})

    csvPath = os.path.join(resultsDir, 'generation_speed.csv')
    imgPath = os.path.join(resultsDir, 'generation_speed.png')

    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputCsvPath=csvPath,
        outputImagePath=imgPath
    )
    print(f"Generation speed results saved to {csvPath} and {imgPath}")

def runQueryPerformance():
    """Запускает исследование производительности запросов (SELECT, INSERT, DELETE)."""
    config = loadConfig()
    if config is None:
        return

    print("\n--- Running Query Performance Benchmark ---")
    prepareDatabase()  # Нужна чистая база
    # Seed minimal parent data to satisfy FK constraints
    from lib.randomDataGenerator import RandomDataGenerator
    print("Seeding parent tables for FK constraints...")
    gen = RandomDataGenerator()
    with __import__('lib.databaseConnection', fromlist=['getDbConnection']).getDbConnection() as (conn, cur):
        gen._generateCinemasAndHalls(cur, cinemasCount=5, hallsPerCinema=2)
        gen._generateMovies(cur, 5)
        gen._generateViewers(cur, 5)
    print("Parent tables seeded.")
    # Увеличиваем вместимость залов, чтобы избежать ошибок capacity trigger
    from lib.databaseConnection import getDbConnection
    with getDbConnection() as (conn, cur):
        cur.execute("UPDATE hall SET seat_count = 1000000;")
    print("Hall capacities adjusted.")

    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    queriesConfig = config.get('queries', [])

    csvPath = os.path.join(resultsDir, 'query_performance.csv')
    imgDir = os.path.join(resultsDir, 'query_images')

    measureQueryPerformance(
        queriesConfig=queriesConfig,
        outputCsvPath=csvPath,
        outputImageDir=imgDir
    )
    print(f"Query performance results saved to {csvPath} and images to {imgDir}")

def runJoinPerformance():
    """Запускает исследование производительности JOIN-операций."""
    config = loadConfig()
    if config is None:
        return

    print("\n--- Running JOIN Performance Benchmark ---")
    # Для JOIN нужны данные, сгенерируем их
    from lib.randomDataGenerator import RandomDataGenerator
    prepareDatabase()
    print("Generating data for JOIN analysis...")
    RandomDataGenerator().generateData(1000, 1000, 10, 2, 5, 0.1, 0.1, 0.1)

    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    joinConfig = config.get('joinSettings', [])

    csvPath = os.path.join(resultsDir, 'join_analysis.csv')

    analyzeJoinPerformance(
        joinConfig=joinConfig,
        outputCsvPath=csvPath
    )
    print(f"JOIN analysis results saved to {csvPath}")

def runAllBenchmarks():
    """Запускает все исследования последовательно."""
    runGenerationSpeed()
    runQueryPerformance()
    runJoinPerformance()
    # Сюда можно добавить и другие исследования, например, по индексам

def main():
    """Главное меню для запуска исследований."""
    print("--- Database Performance Research ---")
    print("Parameters are configured in 'investigations/paramsSettings.json'")
    print("\nAvailable commands:")
    print("1 - Benchmark: Data Generation Speed (in-memory)")
    print("2 - Benchmark: Query Performance (SELECT, INSERT, DELETE)")
    print("3 - Benchmark: JOIN Performance")
    print("4 - Run ALL benchmarks")
    print("0 - Exit")

    while True:
        choice = input("\nEnter command number: ")

        if choice == "1":
            runGenerationSpeed()
        elif choice == "2":
            runQueryPerformance()
        elif choice == "3":
            runJoinPerformance()
        elif choice == "4":
            runAllBenchmarks()
        elif choice == "0":
            print("Goodbye")
            break
        else:
            print("Unknown command")

if __name__ == '__main__':
    main()
