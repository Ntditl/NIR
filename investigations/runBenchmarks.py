import os
import argparse
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql
from benchmarks.generationSpeed import measureGenerationSpeed
from benchmarks.newResearch import runAllResearch


def runBenchmarks(configPath: str, disablePk: bool, disableStringIndex: bool, disableFts: bool) -> None:
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

    print("Запускаю исследования: DELETE, JOIN, PK, строковый индекс, FTS →", resultsDir)
    runAllResearch(
        outputDir=resultsDir,
        raster=True,
        includePkExperiment=(not disablePk),
        includeStringIndexExperiment=(not disableStringIndex),
        includeFtsExperiment=(not disableFts)
    )

    print("Все исследования завершены. Результаты сохранены в:", resultsDir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Запуск исследований производительности БД (генерация + операции + PK + строковый индекс + FTS)."
    )
    defaultConfigPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    parser.add_argument(
        '-c', '--config',
        type=str,
        default=defaultConfigPath,
        help="Путь к JSON-файлу конфигурации."
    )
    parser.add_argument(
        '--no-pk',
        action='store_true',
        help='Отключить эксперимент с первичным ключом (по умолчанию включен)'
    )
    parser.add_argument(
        '--no-string-index',
        action='store_true',
        help='Отключить эксперимент со строковым индексом (по умолчанию включен)'
    )
    parser.add_argument(
        '--no-fts',
        action='store_true',
        help='Отключить FTS эксперимент (по умолчанию включен)'
    )
    args = parser.parse_args()
    runBenchmarks(args.config, args.no_pk, args.no_string_index, args.no_fts)
