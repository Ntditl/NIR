import os
import argparse
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql
from benchmarks.generationSpeed import measureGenerationSpeed
from benchmarks.queryPerformance import measureQueryPerformance
from benchmarks.indexCinema import runIndexBenchmarks
from experiments.customDbResearch import runCustomDbResearch
from experiments.indexResearch import runIndexResearch
from benchmarks.simpleDbIndexBench import runSimpleDbIndexBench


def runBenchmarks(configPath: str) -> None:
    with getDbConnection() as (conn, cur):
        tableNames = [
            "ticket", "session", "hall", "cinema", "movie_review",
            "favorite_movies", "viewer_profile", "movie", "viewer"
        ]
        for table in tableNames:
            cur.execute("DROP TABLE IF EXISTS " + table + " CASCADE;")
        ddls = getCreateTablesSql()
        for ddl in ddls:
            cur.execute(ddl)
        for table in tableNames:
            cur.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT FROM pg_class WHERE relname = '""" + table + """_id_seq'
                    ) THEN
                        PERFORM setval('""" + table + """_id_seq', 1, false);
                    END IF;
                END $$;
                """
            )

    with open(configPath, 'r', encoding='utf-8') as configFile:
        config = json.load(configFile)

    resultsDir = config.get('resultsDirectory', 'results')
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)

    tablesConfig = config.get('tables', {})
    generationResultsPath = os.path.join(resultsDir, 'generation_speed.csv')
    generationImagePath = os.path.join(resultsDir, 'generation_speed.png')
    print("Измеряю скорость генерации данных →", generationResultsPath, "и изображение →", generationImagePath)
    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputCsvPath=generationResultsPath,
        outputImagePath=generationImagePath
    )

    queriesConfig = config.get('queries', [])
    queryResultsPath = os.path.join(resultsDir, 'query_performance.csv')
    queryImageDir = os.path.join(resultsDir, 'query_images')
    print("Измеряю производительность запросов →", queryResultsPath, "и изображения →", queryImageDir)
    measureQueryPerformance(
        queriesConfig=queriesConfig,
        outputCsvPath=queryResultsPath,
        outputImageDir=queryImageDir
    )

    print("Запускаю бенчмарки индексов (полный набор) → index_bench")
    runIndexBenchmarks()

    customConfig = config.get('customSettings', {})
    customResultsPath = os.path.join(resultsDir, 'custom_db_research.csv')
    print("Запускаю исследование собственной СУБД →", customResultsPath)
    runCustomDbResearch(
        researchConfig=customConfig,
        outputCsvPath=customResultsPath
    )

    indexResearchConfig = config.get('indexResearch', {})
    indexResearchDir = os.path.join(resultsDir, 'index_research')
    print("Запускаю пакет детального исследования индексов →", indexResearchDir)
    runIndexResearch(indexResearchConfig, indexResearchDir)

    simpleDbConfig = config.get('simpleDb', {})
    simpleDbDir = os.path.join(resultsDir, 'simpledb')
    print("Запускаю бенчмарки индексации SimpleDB →", simpleDbDir)
    rowCounts = simpleDbConfig.get('rowCounts', [1000, 5000, 10000])
    repeats = int(simpleDbConfig.get('repeats', 3))
    runSimpleDbIndexBench(simpleDbDir, rowCounts=rowCounts, repeats=repeats)

    print("Все бенчмарки завершены. Результаты сохранены в:", resultsDir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Запуск полного набора исследований производительности БД."
    )
    defaultConfigPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    parser.add_argument(
        '-c', '--config',
        type=str,
        default=defaultConfigPath,
        help="Путь к JSON-файлу конфигурации с параметрами бенчмарков."
    )
    args = parser.parse_args()
    runBenchmarks(args.config)
