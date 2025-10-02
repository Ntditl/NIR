import os
import argparse
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql
from investigations.benchmarks.generationSpeed import measureGenerationSpeed
from investigations.sandboxUtils import setupSandboxForResearch, cleanupSandboxAfterResearch, resetSandboxDatabase
from investigations.databaseOperations.tableOperations.basicOperations import selectPrimaryKey, selectStringField, selectNumberField, measureDeleteWhere
from investigations.databaseOperations.joinOperations.joinPerformance import measureJoinOperations, measureComplexJoinOperations, measureManyToManyJoin
from investigations.indexPerformance.primaryKeyIndexes.pkPerformance import measurePkIndexEffect, measurePkInequalityEffect, measurePkInsertEffect
from investigations.indexPerformance.stringIndexes.stringIndexPerformance import measureStringIndexExperiment, measureStringLikePrefix, measureStringLikeContains, measureStringInsertExperiment
from investigations.indexPerformance.fullTextIndexes.ftsPerformance import measureFtsSingleWordExperiment, measureFtsMultiWordExperiment, measureFtsInsertExperiment
from investigations.researchUtils import ROW_COUNTS_DEFAULT, PK_ROW_COUNTS, STRING_INDEX_ROW_COUNTS, FTS_ROW_COUNTS
from investigations.benchmarks.simpledbDeleteNumber import runSimpleDbDeleteNumber
from investigations.benchmarks.simpledbDeleteString import runSimpleDbDeleteString
from investigations.benchmarks.simpledbInsertNumber import runSimpleDbInsertNumber
from investigations.benchmarks.simpledbInsertString import runSimpleDbInsertString
from investigations.benchmarks.simpledbSelectNumber import runSimpleDbSelectNumber
from investigations.benchmarks.simpledbSelectString import runSimpleDbSelectString


def runBenchmarks(configPath: str, disablePk: bool, disableStringIndex: bool, disableFts: bool, disableSimpleDb: bool) -> None:
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

    resetSandboxDatabase()
    setupSandboxForResearch()

    try:
        print('=== ИССЛЕДОВАНИЯ ОПЕРАЦИЙ С ТАБЛИЦАМИ (ПУНКТ 5) ===', flush=True)

        print('Исследование SELECT операций', flush=True)
        selectPkResults = selectPrimaryKey(ROW_COUNTS_DEFAULT)
        selectStringResults = selectStringField(ROW_COUNTS_DEFAULT)
        selectNumberResults = selectNumberField(ROW_COUNTS_DEFAULT)

        print('Исследование DELETE операций', flush=True)
        deleteResults = measureDeleteWhere(ROW_COUNTS_DEFAULT)

        print('=== ИССЛЕДОВАНИЯ JOIN ОПЕРАЦИЙ ===', flush=True)
        joinResults = measureJoinOperations(ROW_COUNTS_DEFAULT)
        complexJoinResults = measureComplexJoinOperations(ROW_COUNTS_DEFAULT)
        manyToManyResults = measureManyToManyJoin(ROW_COUNTS_DEFAULT)

        if not disablePk:
            print('=== ИССЛЕДОВАНИЯ ПЕРВИЧНЫХ КЛЮЧЕЙ (ПУНКТ 6.a) ===', flush=True)
            pkResults = measurePkIndexEffect(PK_ROW_COUNTS, resultsDir, True)
            pkIneqResults = measurePkInequalityEffect(PK_ROW_COUNTS, resultsDir, True)
            pkInsertResults = measurePkInsertEffect(PK_ROW_COUNTS, resultsDir, True)

        if not disableStringIndex:
            print('=== ИССЛЕДОВАНИЯ СТРОКОВЫХ ИНДЕКСОВ (ПУНКТ 6.b) ===', flush=True)
            stringIdxResults = measureStringIndexExperiment(STRING_INDEX_ROW_COUNTS, resultsDir, True)
            stringPrefixResults = measureStringLikePrefix(STRING_INDEX_ROW_COUNTS, resultsDir, True)
            stringContainsResults = measureStringLikeContains(STRING_INDEX_ROW_COUNTS, resultsDir, True)
            stringInsertResults = measureStringInsertExperiment(STRING_INDEX_ROW_COUNTS, resultsDir, True)

        if not disableFts:
            print('=== ИССЛЕДОВАНИЯ ПОЛНОТЕКСТОВЫХ ИНДЕКСОВ (ПУНКТ 6.c) ===', flush=True)
            ftsSingleResults = measureFtsSingleWordExperiment(FTS_ROW_COUNTS, resultsDir, True)
            ftsMultiResults = measureFtsMultiWordExperiment(FTS_ROW_COUNTS, resultsDir, True)
            ftsInsertResults = measureFtsInsertExperiment(FTS_ROW_COUNTS, resultsDir, True)

    finally:
        cleanupSandboxAfterResearch()

    if not disableSimpleDb:
        print("SimpleDB: SELECT WHERE по числовому полю (с индексом и без) →", resultsDir)
        runSimpleDbSelectNumber(resultsDir, True)
        print("SimpleDB: SELECT WHERE по строковому полю (с индексом и без) →", resultsDir)
        runSimpleDbSelectString(resultsDir, True)
        print("SimpleDB: INSERT по числовому полю (с индексом и без) →", resultsDir)
        runSimpleDbInsertNumber(resultsDir, True)
        print("SimpleDB: INSERT по строковому полю (с индексом и без) →", resultsDir)
        runSimpleDbInsertString(resultsDir, True)
        print("SimpleDB: DELETE WHERE по числовому полю (с индексом и без) →", resultsDir)
        runSimpleDbDeleteNumber(resultsDir, True)
        print("SimpleDB: DELETE WHERE по строковому полю (с индексом и без) →", resultsDir)
        runSimpleDbDeleteString(resultsDir, True)

    print("Все исследования завершены. Результаты сохранены в:", resultsDir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Запуск исследований производительности БД (генерация + операции + PK + строковый индекс + FTS + SimpleDB)."
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
    parser.add_argument(
        '--no-simpledb',
        action='store_true',
        help='Отключить эксперимент SimpleDB (по умолчанию включен)'
    )
    args = parser.parse_args()
    runBenchmarks(args.config, args.no_pk, args.no_string_index, args.no_fts, args.no_simpledb)
