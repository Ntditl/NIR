import os
import argparse
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql
from lib.managers.sandboxManager import SandboxManager
from lib.visualization.plots import PlotBuilder
from investigations.benchmarks.generationSpeed import measureGenerationSpeed
from investigations.databaseOperationsResearch import selectNumberField, selectNumberFieldViewer, selectNumberFieldViewerProfile, selectDateField, selectDateFieldViewer, selectDateFieldViewerProfile, insertMovieData, insertViewerData, insertViewerProfileData, measureDeleteWhere, measureDeleteWhereViewer, measureDeleteWhereViewerProfile, measureJoinOperations, measureComplexJoinOperations, measureManyToManyJoin
from investigations.indexPerformanceResearch import measurePkIndexEffect, measurePkInequalityEffect, measurePkInsertEffect, measureStringIndexExperiment, measureStringLikePrefix, measureStringLikeContains, measureStringInsertExperiment, measureFtsSingleWordExperiment, measureFtsMultiWordExperiment, measureFtsInsertExperiment
from investigations.researchUtils import SANDBOX_SCHEMA_NAME
from investigations.benchmarks.simpledbBenchmarks import runSimpleDbDeleteNumber, runSimpleDbDeleteString, runSimpleDbInsertNumber, runSimpleDbInsertString, runSimpleDbSelectNumber, runSimpleDbSelectString


def runBenchmarks(configPath: str, disablePk: bool, disableStringIndex: bool, disableFts: bool, disableSimpleDb: bool) -> None:
    with open(configPath, 'r', encoding='utf-8') as configFile:
        config = json.load(configFile)

    resetTables = config['resetTables']
    operationsRowCounts = config['operationsResearch']['rowCounts']

    indexResearchConfig = config['indexResearch']
    pkRowCounts = indexResearchConfig['pkRowCounts']
    stringIndexRowCounts = indexResearchConfig['stringIndexRowCounts']
    ftsRowCounts = indexResearchConfig['ftsRowCounts']

    try:
        pkRuns = indexResearchConfig['pkRuns']
        pkQueriesPerRun = indexResearchConfig['pkQueriesPerRun']
        stringQueriesPerRun = indexResearchConfig['stringQueriesPerRun']
        ftsSampleQueries = indexResearchConfig['ftsSampleQueries']
        ftsMultiSampleQueries = indexResearchConfig['ftsMultiSampleQueries']
    except KeyError as exc:
        missingKey = str(exc).strip("\'")
        raise ValueError("В конфигурации 'paramsSettings.json' отсутствует ключ indexResearch.%s. Добавьте параметр в файл настроек." % missingKey)

    simpleDbConfig = config['simpleDb']
    simpleDbRowCounts = simpleDbConfig['rowCounts']
    simpleDbRepeats = simpleDbConfig['repeats']
    simpleDbQueriesPerRun = simpleDbConfig['queriesPerRun']

    resultsDir = config['resultsDirectory']
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)

    subdirs = config.get('resultsSubdirectories', {})
    generationSingleDir = os.path.join(resultsDir, subdirs.get('generationSingle', 'point5b_1'))
    generationFkGroupsDir = os.path.join(resultsDir, subdirs.get('generationFkGroups', 'point5b_2'))
    operationsDir = os.path.join(resultsDir, subdirs.get('operations', 'point5c_operations'))
    pkIndexDir = os.path.join(resultsDir, subdirs.get('pkIndex', 'point6a_pk_index'))
    stringIndexDir = os.path.join(resultsDir, subdirs.get('stringIndex', 'point6b_string_index'))
    ftsIndexDir = os.path.join(resultsDir, subdirs.get('ftsIndex', 'point6c_fts_index'))
    simpleDbDir = os.path.join(resultsDir, subdirs.get('simpleDb', 'point7_simpledb'))

    for directory in [generationSingleDir, generationFkGroupsDir, operationsDir, pkIndexDir, stringIndexDir, ftsIndexDir, simpleDbDir]:
        if not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)

    operationsSelectNumberDir = os.path.join(operationsDir, '1_select_number')
    operationsSelectDateDir = os.path.join(operationsDir, '2_select_date')
    operationsInsertDir = os.path.join(operationsDir, '3_insert')
    operationsDeleteDir = os.path.join(operationsDir, '4_delete')
    operationsJoinOneToManyDir = os.path.join(operationsDir, '5_join_one_to_many')
    operationsJoinComplexDir = os.path.join(operationsDir, '6_join_complex')
    operationsJoinManyToManyDir = os.path.join(operationsDir, '7_join_many_to_many')

    pkSelectEqualityDir = os.path.join(pkIndexDir, '1_select_equality')
    pkSelectInequalityDir = os.path.join(pkIndexDir, '2_select_inequality')
    pkInsertDir = os.path.join(pkIndexDir, '3_insert')

    stringSelectExactDir = os.path.join(stringIndexDir, '1_select_exact')
    stringSelectLikePrefixDir = os.path.join(stringIndexDir, '2_select_like_prefix')
    stringSelectLikeContainsDir = os.path.join(stringIndexDir, '3_select_like_contains')
    stringInsertDir = os.path.join(stringIndexDir, '4_insert')

    ftsSelectSingleWordDir = os.path.join(ftsIndexDir, '1_select_single_word')
    ftsSelectMultiWordDir = os.path.join(ftsIndexDir, '2_select_multi_word')
    ftsInsertDir = os.path.join(ftsIndexDir, '3_insert')

    simpleDbSelectNumberDir = os.path.join(simpleDbDir, '1_select_number')
    simpleDbSelectStringDir = os.path.join(simpleDbDir, '2_select_string')
    simpleDbInsertNumberDir = os.path.join(simpleDbDir, '3_insert_number')
    simpleDbInsertStringDir = os.path.join(simpleDbDir, '4_insert_string')
    simpleDbDeleteNumberDir = os.path.join(simpleDbDir, '5_delete_number')
    simpleDbDeleteStringDir = os.path.join(simpleDbDir, '6_delete_string')

    allSubdirectories = [
        operationsSelectNumberDir, operationsSelectDateDir, operationsInsertDir,
        operationsDeleteDir, operationsJoinOneToManyDir, operationsJoinComplexDir,
        operationsJoinManyToManyDir, pkSelectEqualityDir, pkSelectInequalityDir,
        pkInsertDir, stringSelectExactDir, stringSelectLikePrefixDir,
        stringSelectLikeContainsDir, stringInsertDir, ftsSelectSingleWordDir,
        ftsSelectMultiWordDir, ftsInsertDir, simpleDbSelectNumberDir,
        simpleDbSelectStringDir, simpleDbInsertNumberDir, simpleDbInsertStringDir,
        simpleDbDeleteNumberDir, simpleDbDeleteStringDir
    ]

    for directory in allSubdirectories:
        if not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)

    with getDbConnection() as (conn, cur):
        for table in resetTables:
            cur.execute("DROP TABLE IF EXISTS " + table + " CASCADE;")
        ddls = getCreateTablesSql()
        for ddl in ddls:
            cur.execute(ddl)
        for table in resetTables:
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

    tablesConfig = config['tables']
    print("Измеряю скорость генерации данных →", generationSingleDir, "и", generationFkGroupsDir)
    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputSingleDir=generationSingleDir,
        outputFkGroupsDir=generationFkGroupsDir
    )

    print("Запускаю исследования: DELETE, JOIN, PK, строковый индекс, FTS →", resultsDir)

    print('Сброс песочницы старт', flush=True)
    sandboxManager = SandboxManager(SANDBOX_SCHEMA_NAME)
    sandboxManager.resetSandbox()
    sandboxManager.ensureMinimalData()
    print('Сброс песочницы завершен', flush=True)

    print('Песочница готова', flush=True)

    try:
        print('=== ИССЛЕДОВАНИЯ ОПЕРАЦИЙ С ТАБЛИЦАМИ (ПУНКТ 5) ===', flush=True)

        print('Исследование SELECT операций по числовому полю для всех таблиц', flush=True)
        selectNumberResultsMovie = selectNumberField(operationsRowCounts)
        selectNumberResultsViewer = selectNumberFieldViewer(operationsRowCounts)
        selectNumberResultsViewerProfile = selectNumberFieldViewerProfile(operationsRowCounts)

        selectNumberCsvMovie = os.path.join(operationsSelectNumberDir, 'select_number_movie.csv')
        with open(selectNumberCsvMovie, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectNumberResultsMovie:
                f.write(f"{result['count']},{result['time']}\n")

        selectNumberCsvViewer = os.path.join(operationsSelectNumberDir, 'select_number_viewer.csv')
        with open(selectNumberCsvViewer, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectNumberResultsViewer:
                f.write(f"{result['count']},{result['time']}\n")

        selectNumberCsvViewerProfile = os.path.join(operationsSelectNumberDir, 'select_number_viewer_profile.csv')
        with open(selectNumberCsvViewerProfile, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectNumberResultsViewerProfile:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsSelectNumberDir)
        xValuesMovie = [r['count'] for r in selectNumberResultsMovie]
        yValuesMovie = [r['time'] for r in selectNumberResultsMovie]
        xValuesViewer = [r['count'] for r in selectNumberResultsViewer]
        yValuesViewer = [r['time'] for r in selectNumberResultsViewer]
        xValuesViewerProfile = [r['count'] for r in selectNumberResultsViewerProfile]
        yValuesViewerProfile = [r['time'] for r in selectNumberResultsViewerProfile]
        builder.buildChart(
            {
                'movie': (xValuesMovie, yValuesMovie),
                'viewer': (xValuesViewer, yValuesViewer),
                'viewer_profile': (xValuesViewerProfile, yValuesViewerProfile)
            },
            'SELECT WHERE по числовому полю (сравнение таблиц)',
            'Количество строк',
            'Время выполнения (сек)',
            'select_number_field_comparison',
            True
        )

        print('Исследование SELECT операций по полю даты для всех таблиц', flush=True)
        selectDateResultsMovie = selectDateField(operationsRowCounts)
        selectDateResultsViewer = selectDateFieldViewer(operationsRowCounts)
        selectDateResultsViewerProfile = selectDateFieldViewerProfile(operationsRowCounts)

        selectDateCsvMovie = os.path.join(operationsSelectDateDir, 'select_date_movie.csv')
        with open(selectDateCsvMovie, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectDateResultsMovie:
                f.write(f"{result['count']},{result['time']}\n")

        selectDateCsvViewer = os.path.join(operationsSelectDateDir, 'select_date_viewer.csv')
        with open(selectDateCsvViewer, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectDateResultsViewer:
                f.write(f"{result['count']},{result['time']}\n")

        selectDateCsvViewerProfile = os.path.join(operationsSelectDateDir, 'select_date_viewer_profile.csv')
        with open(selectDateCsvViewerProfile, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectDateResultsViewerProfile:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsSelectDateDir)
        xValuesMovie = [r['count'] for r in selectDateResultsMovie]
        yValuesMovie = [r['time'] for r in selectDateResultsMovie]
        xValuesViewer = [r['count'] for r in selectDateResultsViewer]
        yValuesViewer = [r['time'] for r in selectDateResultsViewer]
        xValuesViewerProfile = [r['count'] for r in selectDateResultsViewerProfile]
        yValuesViewerProfile = [r['time'] for r in selectDateResultsViewerProfile]
        builder.buildChart(
            {
                'movie': (xValuesMovie, yValuesMovie),
                'viewer': (xValuesViewer, yValuesViewer),
                'viewer_profile': (xValuesViewerProfile, yValuesViewerProfile)
            },
            'SELECT WHERE по полю даты (сравнение таблиц)',
            'Количество строк',
            'Время выполнения (сек)',
            'select_date_field_comparison',
            True
        )

        print('Исследование INSERT операций для всех таблиц', flush=True)
        insertResultsMovie = insertMovieData(operationsRowCounts)
        insertResultsViewer = insertViewerData(operationsRowCounts)
        insertResultsViewerProfile = insertViewerProfileData(operationsRowCounts)

        insertCsvMovie = os.path.join(operationsInsertDir, 'insert_movie.csv')
        with open(insertCsvMovie, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in insertResultsMovie:
                f.write(f"{result['count']},{result['time']}\n")

        insertCsvViewer = os.path.join(operationsInsertDir, 'insert_viewer.csv')
        with open(insertCsvViewer, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in insertResultsViewer:
                f.write(f"{result['count']},{result['time']}\n")

        insertCsvViewerProfile = os.path.join(operationsInsertDir, 'insert_viewer_profile.csv')
        with open(insertCsvViewerProfile, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in insertResultsViewerProfile:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsInsertDir)
        xValuesMovie = [r['count'] for r in insertResultsMovie]
        yValuesMovie = [r['time'] for r in insertResultsMovie]
        xValuesViewer = [r['count'] for r in insertResultsViewer]
        yValuesViewer = [r['time'] for r in insertResultsViewer]
        xValuesViewerProfile = [r['count'] for r in insertResultsViewerProfile]
        yValuesViewerProfile = [r['time'] for r in insertResultsViewerProfile]
        builder.buildChart(
            {
                'movie': (xValuesMovie, yValuesMovie),
                'viewer': (xValuesViewer, yValuesViewer),
                'viewer_profile': (xValuesViewerProfile, yValuesViewerProfile)
            },
            'INSERT операции (сравнение таблиц)',
            'Количество вставляемых строк',
            'Время выполнения (сек)',
            'insert_operations_comparison',
            True
        )

        print('Исследование DELETE операций для всех таблиц', flush=True)
        deleteResultsMovie = measureDeleteWhere(operationsRowCounts)
        deleteResultsViewer = measureDeleteWhereViewer(operationsRowCounts)
        deleteResultsViewerProfile = measureDeleteWhereViewerProfile(operationsRowCounts)

        deleteCsvMovie = os.path.join(operationsDeleteDir, 'delete_movie.csv')
        with open(deleteCsvMovie, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in deleteResultsMovie:
                f.write(f"{result['count']},{result['time']}\n")

        deleteCsvViewer = os.path.join(operationsDeleteDir, 'delete_viewer.csv')
        with open(deleteCsvViewer, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in deleteResultsViewer:
                f.write(f"{result['count']},{result['time']}\n")

        deleteCsvViewerProfile = os.path.join(operationsDeleteDir, 'delete_viewer_profile.csv')
        with open(deleteCsvViewerProfile, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in deleteResultsViewerProfile:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsDeleteDir)
        xValuesMovie = [r['count'] for r in deleteResultsMovie]
        yValuesMovie = [r['time'] for r in deleteResultsMovie]
        xValuesViewer = [r['count'] for r in deleteResultsViewer]
        yValuesViewer = [r['time'] for r in deleteResultsViewer]
        xValuesViewerProfile = [r['count'] for r in deleteResultsViewerProfile]
        yValuesViewerProfile = [r['time'] for r in deleteResultsViewerProfile]
        builder.buildChart(
            {
                'movie': (xValuesMovie, yValuesMovie),
                'viewer': (xValuesViewer, yValuesViewer),
                'viewer_profile': (xValuesViewerProfile, yValuesViewerProfile)
            },
            'DELETE WHERE операции (сравнение таблиц)',
            'Количество удаляемых строк',
            'Время выполнения (сек)',
            'delete_operations_comparison',
            True
        )

        print('=== ИССЛЕДОВАНИЯ JOIN ОПЕРАЦИЙ ===', flush=True)
        joinResults = measureJoinOperations(operationsRowCounts)
        complexJoinResults = measureComplexJoinOperations(operationsRowCounts)
        manyToManyResults = measureManyToManyJoin(operationsRowCounts)

        joinCsv = os.path.join(operationsJoinOneToManyDir, 'join_operations.csv')
        with open(joinCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in joinResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsJoinOneToManyDir)
        xValues = [r['count'] for r in joinResults]
        yValues = [r['time'] for r in joinResults]
        builder.buildChart(
            {'JOIN операции': (xValues, yValues)},
            'JOIN операции (один-ко-многим)',
            'Количество строк',
            'Время выполнения (сек)',
            'join_operations',
            True
        )

        complexJoinCsv = os.path.join(operationsJoinComplexDir, 'complex_join_operations.csv')
        with open(complexJoinCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in complexJoinResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsJoinComplexDir)
        xValues = [r['count'] for r in complexJoinResults]
        yValues = [r['time'] for r in complexJoinResults]
        builder.buildChart(
            {'Сложные JOIN': (xValues, yValues)},
            'Сложные JOIN операции',
            'Количество строк',
            'Время выполнения (сек)',
            'complex_join_operations',
            True
        )

        manyToManyCsv = os.path.join(operationsJoinManyToManyDir, 'many_to_many_join.csv')
        with open(manyToManyCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in manyToManyResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsJoinManyToManyDir)
        xValues = [r['count'] for r in manyToManyResults]
        yValues = [r['time'] for r in manyToManyResults]
        builder.buildChart(
            {'JOIN многие-ко-многим': (xValues, yValues)},
            'JOIN многие-ко-многим',
            'Количество строк',
            'Время выполнения (сек)',
            'many_to_many_join',
            True
        )

        if not disablePk:
            print('=== ИССЛЕДОВАНИЯ ПЕРВИЧНЫХ КЛЮЧЕЙ (ПУНКТ 6.a) ===', flush=True)
            pkResults = measurePkIndexEffect(pkRowCounts, pkSelectEqualityDir, True, pkRuns, pkQueriesPerRun)
            pkIneqResults = measurePkInequalityEffect(pkRowCounts, pkSelectInequalityDir, True, pkRuns, pkQueriesPerRun)
            pkInsertResults = measurePkInsertEffect(pkRowCounts, pkInsertDir, True, pkRuns, pkQueriesPerRun)

        if not disableStringIndex:
            print('=== ИССЛЕДОВАНИЯ СТРОКОВЫХ ИНДЕКСОВ (ПУНКТ 6.b) ===', flush=True)
            stringIdxResults = measureStringIndexExperiment(stringIndexRowCounts, stringSelectExactDir, True, pkRuns, stringQueriesPerRun)
            stringPrefixResults = measureStringLikePrefix(stringIndexRowCounts, stringSelectLikePrefixDir, True, pkRuns, stringQueriesPerRun)
            stringContainsResults = measureStringLikeContains(stringIndexRowCounts, stringSelectLikeContainsDir, True, pkRuns, stringQueriesPerRun)
            stringInsertResults = measureStringInsertExperiment(stringIndexRowCounts, stringInsertDir, True, pkRuns, stringQueriesPerRun)

        if not disableFts:
            print('=== ИССЛЕДОВАНИЯ ПОЛНОТЕКСТОВЫХ ИНДЕКСОВ (ПУНКТ 6.c) ===', flush=True)
            ftsSingleResults = measureFtsSingleWordExperiment(ftsRowCounts, ftsSelectSingleWordDir, True, pkRuns, ftsSampleQueries)
            ftsMultiResults = measureFtsMultiWordExperiment(ftsRowCounts, ftsSelectMultiWordDir, True, pkRuns, ftsMultiSampleQueries)
            ftsInsertResults = measureFtsInsertExperiment(ftsRowCounts, ftsInsertDir, True, pkRuns, ftsSampleQueries)
    finally:
        print('Очистка песочницы после исследований', flush=True)
        sandboxManager.dropSandboxSchema()
        print('Песочница удалена', flush=True)

    if not disableSimpleDb:
        print("SimpleDB: SELECT WHERE по числовому полю (с индексом и без) →", simpleDbSelectNumberDir)
        runSimpleDbSelectNumber(simpleDbSelectNumberDir, True, simpleDbRowCounts, simpleDbRepeats, simpleDbQueriesPerRun)
        print("SimpleDB: SELECT WHERE по строковому полю (с индексом и без) →", simpleDbSelectStringDir)
        runSimpleDbSelectString(simpleDbSelectStringDir, True, simpleDbRowCounts, simpleDbRepeats, simpleDbQueriesPerRun)
        print("SimpleDB: INSERT по числовому полю (с индексом и без) →", simpleDbInsertNumberDir)
        runSimpleDbInsertNumber(simpleDbInsertNumberDir, True, simpleDbRowCounts, simpleDbRepeats)
        print("SimpleDB: INSERT по строковому полю (с индексом и без) →", simpleDbInsertStringDir)
        runSimpleDbInsertString(simpleDbInsertStringDir, True, simpleDbRowCounts, simpleDbRepeats)
        print("SimpleDB: DELETE WHERE по числовому полю (с индексом и без) →", simpleDbDeleteNumberDir)
        runSimpleDbDeleteNumber(simpleDbDeleteNumberDir, True, simpleDbRowCounts, simpleDbRepeats, simpleDbQueriesPerRun)
        print("SimpleDB: DELETE WHERE по строковому полю (с индексом и без) →", simpleDbDeleteStringDir)
        runSimpleDbDeleteString(simpleDbDeleteStringDir, True, simpleDbRowCounts, simpleDbRepeats, simpleDbQueriesPerRun)

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
