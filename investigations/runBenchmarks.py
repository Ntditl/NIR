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
from investigations.databaseOperationsResearch import selectNumberField, selectDateField, insertMovieData, measureDeleteWhere, measureJoinOperations, measureComplexJoinOperations, measureManyToManyJoin
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

    simpleDbConfig = config['simpleDb']
    simpleDbRowCounts = simpleDbConfig['rowCounts']
    simpleDbRepeats = simpleDbConfig['repeats']
    simpleDbQueriesPerRun = simpleDbConfig['queriesPerRun']

    resultsDir = config['resultsDirectory']
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)

    subdirs = config.get('resultsSubdirectories', {})
    generationDir = os.path.join(resultsDir, subdirs.get('generation', 'point5b_generation'))
    operationsDir = os.path.join(resultsDir, subdirs.get('operations', 'point5c_operations'))
    pkIndexDir = os.path.join(resultsDir, subdirs.get('pkIndex', 'point6a_pk_index'))
    stringIndexDir = os.path.join(resultsDir, subdirs.get('stringIndex', 'point6b_string_index'))
    ftsIndexDir = os.path.join(resultsDir, subdirs.get('ftsIndex', 'point6c_fts_index'))
    simpleDbDir = os.path.join(resultsDir, subdirs.get('simpleDb', 'point7_simpledb'))

    for directory in [generationDir, operationsDir, pkIndexDir, stringIndexDir, ftsIndexDir, simpleDbDir]:
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
    generationResultsPath = os.path.join(generationDir, 'generation_speed.csv')
    generationImagePath = os.path.join(generationDir, 'generation_speed.png')
    print("Измеряю скорость генерации данных →", generationResultsPath, "и изображение →", generationImagePath)
    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputCsvPath=generationResultsPath,
        outputImagePath=generationImagePath
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

        print('Исследование SELECT операций', flush=True)
        selectNumberResults = selectNumberField(operationsRowCounts)
        selectDateResults = selectDateField(operationsRowCounts)

        selectNumberCsv = os.path.join(operationsSelectNumberDir, 'select_number_field.csv')
        with open(selectNumberCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectNumberResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsSelectNumberDir)
        xValues = [r['count'] for r in selectNumberResults]
        yValues = [r['time'] for r in selectNumberResults]
        builder.buildChart(
            {'SELECT по числовому полю': (xValues, yValues)},
            'SELECT WHERE по числовому полю',
            'Количество строк',
            'Время выполнения (сек)',
            'select_number_field',
            True
        )

        selectDateCsv = os.path.join(operationsSelectDateDir, 'select_date_field.csv')
        with open(selectDateCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in selectDateResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsSelectDateDir)
        xValues = [r['count'] for r in selectDateResults]
        yValues = [r['time'] for r in selectDateResults]
        builder.buildChart(
            {'SELECT по полю даты': (xValues, yValues)},
            'SELECT WHERE по полю даты',
            'Количество строк',
            'Время выполнения (сек)',
            'select_date_field',
            True
        )

        print('Исследование INSERT операций', flush=True)
        insertResults = insertMovieData(operationsRowCounts)

        insertCsv = os.path.join(operationsInsertDir, 'insert_operations.csv')
        with open(insertCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in insertResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsInsertDir)
        xValues = [r['count'] for r in insertResults]
        yValues = [r['time'] for r in insertResults]
        builder.buildChart(
            {'INSERT операции': (xValues, yValues)},
            'INSERT операции',
            'Количество вставляемых строк',
            'Время выполнения (сек)',
            'insert_operations',
            True
        )

        print('Исследование DELETE операций', flush=True)
        deleteResults = measureDeleteWhere(operationsRowCounts)

        deleteCsv = os.path.join(operationsDeleteDir, 'delete_operations.csv')
        with open(deleteCsv, 'w', encoding='utf-8') as f:
            f.write('row_count,time_seconds\n')
            for result in deleteResults:
                f.write(f"{result['count']},{result['time']}\n")

        builder = PlotBuilder(operationsDeleteDir)
        xValues = [r['count'] for r in deleteResults]
        yValues = [r['time'] for r in deleteResults]
        builder.buildChart(
            {'DELETE WHERE': (xValues, yValues)},
            'DELETE WHERE операции',
            'Количество удаляемых строк',
            'Время выполнения (сек)',
            'delete_operations',
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
            pkResults = measurePkIndexEffect(pkRowCounts, pkSelectEqualityDir, True)
            pkIneqResults = measurePkInequalityEffect(pkRowCounts, pkSelectInequalityDir, True)
            pkInsertResults = measurePkInsertEffect(pkRowCounts, pkInsertDir, True)

            pkCsv = os.path.join(pkSelectEqualityDir, 'pk_index_effect.csv')
            with open(pkCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in pkResults:
                    f.write(f"{result['count']},{result['time']}\n")

            pkPlot = 'pk_index_effect'
            builder = PlotBuilder(pkSelectEqualityDir)
            xValues = [r['count'] for r in pkResults]
            yValues = [r['time'] for r in pkResults]
            builder.buildChart(
                {'PK индекс': (xValues, yValues)},
                'Эффект индекса первичного ключа на SELECT',
                'Количество строк',
                'Время выполнения (сек)',
                pkPlot,
                True
            )

            pkIneqCsv = os.path.join(pkSelectInequalityDir, 'pk_inequality_effect.csv')
            with open(pkIneqCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in pkIneqResults:
                    f.write(f"{result['count']},{result['time']}\n")

            pkIneqPlot = 'pk_inequality_effect'
            builder = PlotBuilder(pkSelectInequalityDir)
            xValues = [r['count'] for r in pkIneqResults]
            yValues = [r['time'] for r in pkIneqResults]
            builder.buildChart(
                {'PK неравенства': (xValues, yValues)},
                'PK индекс с неравенствами (< >)',
                'Количество строк',
                'Время выполнения (сек)',
                pkIneqPlot,
                True
            )

            pkInsertCsv = os.path.join(pkInsertDir, 'pk_insert_effect.csv')
            with open(pkInsertCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in pkInsertResults:
                    f.write(f"{result['count']},{result['time']}\n")

            pkInsertPlot = 'pk_insert_effect'
            builder = PlotBuilder(pkInsertDir)
            xValues = [r['count'] for r in pkInsertResults]
            yValues = [r['time'] for r in pkInsertResults]
            builder.buildChart(
                {'INSERT с PK': (xValues, yValues)},
                'Влияние PK на INSERT операции',
                'Количество строк',
                'Время выполнения (сек)',
                pkInsertPlot,
                True
            )

        if not disableStringIndex:
            print('=== ИССЛЕДОВАНИЯ СТРОКОВЫХ ИНДЕКСОВ (ПУНКТ 6.b) ===', flush=True)
            stringIdxResults = measureStringIndexExperiment(stringIndexRowCounts, stringSelectExactDir, True)
            stringPrefixResults = measureStringLikePrefix(stringIndexRowCounts, stringSelectLikePrefixDir, True)
            stringContainsResults = measureStringLikeContains(stringIndexRowCounts, stringSelectLikeContainsDir, True)
            stringInsertResults = measureStringInsertExperiment(stringIndexRowCounts, stringInsertDir, True)

            stringIdxCsv = os.path.join(stringSelectExactDir, 'string_index_experiment.csv')
            with open(stringIdxCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in stringIdxResults:
                    f.write(f"{result['count']},{result['time']}\n")

            stringIdxPlot = 'string_index_experiment'
            builder = PlotBuilder(stringSelectExactDir)
            xValues = [r['count'] for r in stringIdxResults]
            yValues = [r['time'] for r in stringIdxResults]
            builder.buildChart(
                {'Строковый индекс': (xValues, yValues)},
                'Строковый индекс: SELECT с точным совпадением',
                'Количество строк',
                'Время выполнения (сек)',
                stringIdxPlot,
                True
            )

            stringPrefixCsv = os.path.join(stringSelectLikePrefixDir, 'string_like_prefix.csv')
            with open(stringPrefixCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in stringPrefixResults:
                    f.write(f"{result['count']},{result['time']}\n")

            stringPrefixPlot = 'string_like_prefix'
            builder = PlotBuilder(stringSelectLikePrefixDir)
            xValues = [r['count'] for r in stringPrefixResults]
            yValues = [r['time'] for r in stringPrefixResults]
            builder.buildChart(
                {'LIKE prefix%': (xValues, yValues)},
                'LIKE с префиксом (prefix%)',
                'Количество строк',
                'Время выполнения (сек)',
                stringPrefixPlot,
                True
            )

            stringContainsCsv = os.path.join(stringSelectLikeContainsDir, 'string_like_contains.csv')
            with open(stringContainsCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in stringContainsResults:
                    f.write(f"{result['count']},{result['time']}\n")

            stringContainsPlot = 'string_like_contains'
            builder = PlotBuilder(stringSelectLikeContainsDir)
            xValues = [r['count'] for r in stringContainsResults]
            yValues = [r['time'] for r in stringContainsResults]
            builder.buildChart(
                {'LIKE %substring%': (xValues, yValues)},
                'LIKE с подстрокой (%substring%)',
                'Количество строк',
                'Время выполнения (сек)',
                stringContainsPlot,
                True
            )

            stringInsertCsv = os.path.join(stringInsertDir, 'string_insert_experiment.csv')
            with open(stringInsertCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in stringInsertResults:
                    f.write(f"{result['count']},{result['time']}\n")

            stringInsertPlot = 'string_insert_experiment'
            builder = PlotBuilder(stringInsertDir)
            xValues = [r['count'] for r in stringInsertResults]
            yValues = [r['time'] for r in stringInsertResults]
            builder.buildChart(
                {'INSERT со строковым индексом': (xValues, yValues)},
                'INSERT со строковым индексом',
                'Количество строк',
                'Время выполнения (сек)',
                stringInsertPlot,
                True
            )

        if not disableFts:
            print('=== ИССЛЕДОВАНИЯ ПОЛНОТЕКСТОВЫХ ИНДЕКСОВ (ПУНКТ 6.c) ===', flush=True)
            ftsSingleResults = measureFtsSingleWordExperiment(ftsRowCounts, ftsSelectSingleWordDir, True)
            ftsMultiResults = measureFtsMultiWordExperiment(ftsRowCounts, ftsSelectMultiWordDir, True)
            ftsInsertResults = measureFtsInsertExperiment(ftsRowCounts, ftsInsertDir, True)

            ftsSingleCsv = os.path.join(ftsSelectSingleWordDir, 'fts_single_word.csv')
            with open(ftsSingleCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in ftsSingleResults:
                    f.write(f"{result['count']},{result['time']}\n")

            ftsSinglePlot = 'fts_single_word'
            builder = PlotBuilder(ftsSelectSingleWordDir)
            xValues = [r['count'] for r in ftsSingleResults]
            yValues = [r['time'] for r in ftsSingleResults]
            builder.buildChart(
                {'FTS одно слово': (xValues, yValues)},
                'Полнотекстовый поиск: одно слово',
                'Количество строк',
                'Время выполнения (сек)',
                ftsSinglePlot,
                True
            )

            ftsMultiCsv = os.path.join(ftsSelectMultiWordDir, 'fts_multi_word.csv')
            with open(ftsMultiCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in ftsMultiResults:
                    f.write(f"{result['count']},{result['time']}\n")

            ftsMultiPlot = 'fts_multi_word'
            builder = PlotBuilder(ftsSelectMultiWordDir)
            xValues = [r['count'] for r in ftsMultiResults]
            yValues = [r['time'] for r in ftsMultiResults]
            builder.buildChart(
                {'FTS несколько слов': (xValues, yValues)},
                'Полнотекстовый поиск: несколько слов',
                'Количество строк',
                'Время выполнения (сек)',
                ftsMultiPlot,
                True
            )

            ftsInsertCsv = os.path.join(ftsInsertDir, 'fts_insert_experiment.csv')
            with open(ftsInsertCsv, 'w', encoding='utf-8') as f:
                f.write('row_count,time_seconds\n')
                for result in ftsInsertResults:
                    f.write(f"{result['count']},{result['time']}\n")

            ftsInsertPlot = 'fts_insert_experiment'
            builder = PlotBuilder(ftsInsertDir)
            xValues = [r['count'] for r in ftsInsertResults]
            yValues = [r['time'] for r in ftsInsertResults]
            builder.buildChart(
                {'INSERT с FTS': (xValues, yValues)},
                'INSERT с полнотекстовым индексом',
                'Количество строк',
                'Время выполнения (сек)',
                ftsInsertPlot,
                True
            )
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
