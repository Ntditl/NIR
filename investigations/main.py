import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.generationSpeed import measureGenerationSpeed
from benchmarks.queryPerformance import measureQueryPerformance
from benchmarks.joinAnalysis import analyzeJoinPerformance
from benchmarks.indexCinema import runIndexBenchmarks
from lib.db.models import recreateAllTables

def loadConfig():
    configPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    if not os.path.exists(configPath):
        print(f"Ошибка: файл конфигурации не найден по пути {configPath}")
        return None
    with open(configPath, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config

def prepareDatabase():
    print("Подготовка БД: удаляю и создаю таблицы...")
    recreateAllTables()
    print("База данных готова.")

def runGenerationSpeed():
    config = loadConfig()
    if config is None:
        return
    print("\n--- Бенчмарк: скорость генерации данных ---")
    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    tablesConfig = config.get('tables', {})
    csvPath = os.path.join(resultsDir, 'generation_speed.csv')
    imgPath = os.path.join(resultsDir, 'generation_speed.png')
    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputCsvPath=csvPath,
        outputImagePath=imgPath
    )
    print(f"Результаты скорости генерации сохранены в {csvPath} и {imgPath}")

def runQueryPerformance():
    config = loadConfig()
    if config is None:
        return
    print("\n--- Бенчмарк: производительность запросов ---")
    prepareDatabase()
    from lib.data.generators import RandomDataGenerator
    print("Заполняю родительские таблицы для FK...")
    gen = RandomDataGenerator()
    with __import__('lib.db.connection', fromlist=['getDbConnection']).getDbConnection() as (conn, cur):
        gen._generateCinemasAndHalls(cur, cinemasCount=5, hallsPerCinema=2)
        gen._generateMovies(cur, 5)
        gen._generateViewers(cur, 5)
    print("Родительские таблицы заполнены.")
    from lib.db.connection import getDbConnection
    with getDbConnection() as (conn, cur):
        cur.execute("UPDATE hall SET seat_count = 1000000;")
    print("Вместимость залов увеличена.")
    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    queriesConfig = config.get('queries', [])
    csvPath = os.path.join(resultsDir, 'query_performance.csv')
    imgDir = os.path.join(resultsDir, 'query_images')
    from benchmarks.queryPerformance import measureQueryPerformance
    measureQueryPerformance(
        queriesConfig=queriesConfig,
        outputCsvPath=csvPath,
        outputImageDir=imgDir
    )
    print(f"Результаты производительности запросов сохранены в {csvPath}, изображения в {imgDir}")

def runJoinPerformance():
    config = loadConfig()
    if config is None:
        return
    print("\n--- Бенчмарк: производительность JOIN ---")
    from lib.data.generators import RandomDataGenerator
    prepareDatabase()
    print("Генерирую данные для анализа JOIN...")
    RandomDataGenerator().generateData(1000, 1000, 10, 2, 5, 0.1, 0.1, 0.1)
    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    joinConfig = config.get('joinSettings', [])
    csvPath = os.path.join(resultsDir, 'join_analysis.csv')
    analyzeJoinPerformance(
        joinConfig=joinConfig,
        outputCsvPath=csvPath
    )
    print(f"Результаты анализа JOIN сохранены в {csvPath}")

def runIndexPerformanceCinema():
    print("\n--- Бенчмарк: эффективность индексов (кино-схема) ---")
    runIndexBenchmarks()
    print("Готово: результаты в папке results/index_bench")

def runAllBenchmarks():
    runGenerationSpeed()
    runQueryPerformance()
    runJoinPerformance()
    runIndexPerformanceCinema()

def main():
    print("--- Исследование производительности БД ---")
    print("Параметры задаются в 'investigations/paramsSettings.json'")
    print("\nДоступные команды:")
    print("1 - Бенчмарк: скорость генерации данных (in-memory)")
    print("2 - Бенчмарк: производительность запросов (SELECT, INSERT, DELETE)")
    print("3 - Бенчмарк: производительность JOIN")
    print("4 - Бенчмарк: индексы")
    print("5 - Запустить ВСЕ бенчмарки")
    print("0 - Выход")
    while True:
        choice = input("\nВведите номер команды: ")
        if choice == "1":
            runGenerationSpeed()
        elif choice == "2":
            runQueryPerformance()
        elif choice == "3":
            runJoinPerformance()
        elif choice == "4":
            runIndexPerformanceCinema()
        elif choice == "5":
            runAllBenchmarks()
        elif choice == "0":
            print("Выход")
            break
        else:
            print("Неизвестная команда")

if __name__ == '__main__':
    main()
