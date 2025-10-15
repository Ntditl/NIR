import os
import shutil
import random
import time
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine
from lib.visualization.plots import PlotBuilder

RANDOM_SEED_NUMBER = 12345
RANDOM_SEED_STRING = 43210
BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'foldersForSimpleDb')

def clearDataDir(dataDir):
    os.makedirs(BASE_TEMP_DIR, exist_ok=True)
    if os.path.isdir(dataDir):
        shutil.rmtree(dataDir)
    os.makedirs(dataDir, exist_ok=True)

def createSchema(tableName, dataType, withIndex):
    if dataType == 'number':
        if withIndex:
            return Schema(tableName, [
                {"name": "id", "type": "INT", "index": True},
                {"name": "val", "type": "INT"}
            ])
        else:
            return Schema(tableName, [
                {"name": "id", "type": "INT"},
                {"name": "val", "type": "INT"}
            ])
    else:
        if withIndex:
            return Schema(tableName, [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR", "max": 32, "index": True}
            ])
        else:
            return Schema(tableName, [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR", "max": 32}
            ])

def createEngine(dataDir, schema):
    files = TableFiles(dataDir, schema.tableName)
    eng = TableEngine(files, schema)
    eng.create()
    eng.open()
    return eng

def populateNumberTable(engine, rowCount, seedValue):
    random.seed(seedValue)
    for i in range(1, rowCount + 1):
        engine.insertRow({"id": i, "val": random.randint(0, 1000000)})

def populateStringTable(engine, rowCount, seedValue):
    random.seed(seedValue)
    names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Helen"]
    for i in range(1, rowCount + 1):
        name = random.choice(names) + str(i % 100)
        engine.insertRow({"id": i, "name": name})

def buildIndex(engine, columnName):
    engine._rebuildSingleIndex(columnName)

def measureSelectPerformance(engine, dataType, withIndex, rowCount, queriesPerRun):
    totalTime = 0

    if dataType == 'number':
        for _ in range(queriesPerRun):
            targetId = random.randint(1, rowCount)
            startTime = time.perf_counter()
            results = engine.select(['*'], ('id', targetId))
            endTime = time.perf_counter()
            totalTime += (endTime - startTime)
    else:
        names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Helen"]
        for _ in range(queriesPerRun):
            targetName = random.choice(names) + str(random.randint(1, rowCount) % 100)
            startTime = time.perf_counter()
            results = engine.select(['*'], ('name', targetName))
            endTime = time.perf_counter()
            totalTime += (endTime - startTime)

    return totalTime / queriesPerRun

def measureInsertPerformance(engine, dataType, insertCount):
    if dataType == 'number':
        startTime = time.perf_counter()
        for i in range(insertCount):
            engine.insertRow({"id": 10000 + i, "val": random.randint(0, 1000000)})
        endTime = time.perf_counter()
    else:
        names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Helen"]
        startTime = time.perf_counter()
        for i in range(insertCount):
            name = random.choice(names) + str(10000 + i)
            engine.insertRow({"id": 10000 + i, "name": name})
        endTime = time.perf_counter()

    return endTime - startTime

def measureDeletePerformance(engine, dataType, rowCount, queriesPerRun):
    totalTime = 0

    if dataType == 'number':
        for _ in range(queriesPerRun):
            targetId = random.randint(1, rowCount)
            startTime = time.perf_counter()
            engine.deleteWhere("id", targetId)
            endTime = time.perf_counter()
            totalTime += (endTime - startTime)
    else:
        names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Helen"]
        for _ in range(queriesPerRun):
            targetName = random.choice(names) + str(random.randint(1, rowCount) % 100)
            startTime = time.perf_counter()
            engine.deleteWhere("name", targetName)
            endTime = time.perf_counter()
            totalTime += (endTime - startTime)

    return totalTime / queriesPerRun

def runSelectBenchmark(dataType, resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    dataDir = os.path.join(BASE_TEMP_DIR, f'select_{dataType}')
    csvFile = f'simpledb_select_{dataType}.csv'
    plotFile = f'simpledb_select_{dataType}'
    seedValue = RANDOM_SEED_NUMBER if dataType == 'number' else RANDOM_SEED_STRING
    indexColumn = "id" if dataType == 'number' else "name"

    clearDataDir(dataDir)

    results = []

    for rowCount in rowCounts:
        print(f"SimpleDB SELECT {dataType}: тестируем {rowCount} строк")

        timesWithIndex = []
        timesWithoutIndex = []

        for repeat in range(repeats):
            schemaWithIndex = createSchema("test_indexed", dataType, True)
            schemaWithoutIndex = createSchema("test_plain", dataType, False)

            engineWithIndex = createEngine(dataDir, schemaWithIndex)
            engineWithoutIndex = createEngine(dataDir, schemaWithoutIndex)

            if dataType == 'number':
                populateNumberTable(engineWithIndex, rowCount, seedValue)
                populateNumberTable(engineWithoutIndex, rowCount, seedValue)
            else:
                populateStringTable(engineWithIndex, rowCount, seedValue)
                populateStringTable(engineWithoutIndex, rowCount, seedValue)

            buildIndex(engineWithIndex, indexColumn)

            timeWithIndex = measureSelectPerformance(engineWithIndex, dataType, True, rowCount, queriesPerRun)
            timeWithoutIndex = measureSelectPerformance(engineWithoutIndex, dataType, False, rowCount, queriesPerRun)

            timesWithIndex.append(timeWithIndex)
            timesWithoutIndex.append(timeWithoutIndex)

            engineWithIndex.close()
            engineWithoutIndex.close()

        avgTimeWithIndex = sum(timesWithIndex) / len(timesWithIndex)
        avgTimeWithoutIndex = sum(timesWithoutIndex) / len(timesWithoutIndex)

        results.append([rowCount, avgTimeWithIndex, avgTimeWithoutIndex])

    csvPath = os.path.join(resultsDir, csvFile)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write("row_count,with_index,without_index\n")
        for row in results:
            f.write(f"{row[0]},{row[1]},{row[2]}\n")

    if showPlots:
        plotPath = plotFile
        builder = PlotBuilder(resultsDir)

        xValues = [r[0] for r in results]
        yWithIndex = [r[1] for r in results]
        yWithoutIndex = [r[2] for r in results]

        seriesData = {
            'С индексом': (xValues, yWithIndex),
            'Без индекса': (xValues, yWithoutIndex)
        }

        builder.buildChart(
            seriesData,
            f"SimpleDB SELECT WHERE по {dataType} полю",
            "Количество строк",
            "Время выполнения (сек)",
            plotPath,
            True
        )

def runInsertBenchmark(dataType, resultsDir, showPlots, rowCounts, repeats):
    dataDir = os.path.join(BASE_TEMP_DIR, f'insert_{dataType}')
    csvFile = f'simpledb_insert_{dataType}.csv'
    plotFile = f'simpledb_insert_{dataType}'
    indexColumn = "id" if dataType == 'number' else "name"

    clearDataDir(dataDir)

    results = []

    for rowCount in rowCounts:
        print(f"SimpleDB INSERT {dataType}: тестируем {rowCount} строк")

        timesWithIndex = []
        timesWithoutIndex = []

        for repeat in range(repeats):
            schemaWithIndex = createSchema("test_indexed", dataType, True)
            schemaWithoutIndex = createSchema("test_plain", dataType, False)

            engineWithIndex = createEngine(dataDir, schemaWithIndex)
            engineWithoutIndex = createEngine(dataDir, schemaWithoutIndex)

            buildIndex(engineWithIndex, indexColumn)

            timeWithIndex = measureInsertPerformance(engineWithIndex, dataType, rowCount)
            timeWithoutIndex = measureInsertPerformance(engineWithoutIndex, dataType, rowCount)

            timesWithIndex.append(timeWithIndex)
            timesWithoutIndex.append(timeWithoutIndex)

            engineWithIndex.close()
            engineWithoutIndex.close()

        avgTimeWithIndex = sum(timesWithIndex) / len(timesWithIndex)
        avgTimeWithoutIndex = sum(timesWithoutIndex) / len(timesWithoutIndex)

        results.append([rowCount, avgTimeWithIndex, avgTimeWithoutIndex])

    csvPath = os.path.join(resultsDir, csvFile)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write("row_count,with_index,without_index\n")
        for row in results:
            f.write(f"{row[0]},{row[1]},{row[2]}\n")

    if showPlots:
        plotPath = plotFile
        builder = PlotBuilder(resultsDir)

        xValues = [r[0] for r in results]
        yWithIndex = [r[1] for r in results]
        yWithoutIndex = [r[2] for r in results]

        seriesData = {
            'С индексом': (xValues, yWithIndex),
            'Без индекса': (xValues, yWithoutIndex)
        }

        builder.buildChart(
            seriesData,
            f"SimpleDB INSERT по {dataType} полю",
            "Количество вставляемых строк",
            "Время выполнения (сек)",
            plotPath,
            True
        )

def runDeleteBenchmark(dataType, resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    dataDir = os.path.join(BASE_TEMP_DIR, f'delete_{dataType}')
    csvFile = f'simpledb_delete_{dataType}.csv'
    plotFile = f'simpledb_delete_{dataType}'
    seedValue = RANDOM_SEED_NUMBER if dataType == 'number' else RANDOM_SEED_STRING
    indexColumn = "id" if dataType == 'number' else "name"

    clearDataDir(dataDir)

    results = []

    for rowCount in rowCounts:
        print(f"SimpleDB DELETE {dataType}: тестируем {rowCount} строк")

        timesWithIndex = []
        timesWithoutIndex = []

        for repeat in range(repeats):
            schemaWithIndex = createSchema("test_indexed", dataType, True)
            schemaWithoutIndex = createSchema("test_plain", dataType, False)

            engineWithIndex = createEngine(dataDir, schemaWithIndex)
            engineWithoutIndex = createEngine(dataDir, schemaWithoutIndex)

            if dataType == 'number':
                populateNumberTable(engineWithIndex, rowCount, seedValue)
                populateNumberTable(engineWithoutIndex, rowCount, seedValue)
            else:
                populateStringTable(engineWithIndex, rowCount, seedValue)
                populateStringTable(engineWithoutIndex, rowCount, seedValue)

            buildIndex(engineWithIndex, indexColumn)

            timeWithIndex = measureDeletePerformance(engineWithIndex, dataType, rowCount, queriesPerRun)
            timeWithoutIndex = measureDeletePerformance(engineWithoutIndex, dataType, rowCount, queriesPerRun)

            timesWithIndex.append(timeWithIndex)
            timesWithoutIndex.append(timeWithoutIndex)

            engineWithIndex.close()
            engineWithoutIndex.close()

        avgTimeWithIndex = sum(timesWithIndex) / len(timesWithIndex)
        avgTimeWithoutIndex = sum(timesWithoutIndex) / len(timesWithoutIndex)

        results.append([rowCount, avgTimeWithIndex, avgTimeWithoutIndex])

    csvPath = os.path.join(resultsDir, csvFile)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write("row_count,with_index,without_index\n")
        for row in results:
            f.write(f"{row[0]},{row[1]},{row[2]}\n")

    if showPlots:
        plotPath = plotFile
        builder = PlotBuilder(resultsDir)

        xValues = [r[0] for r in results]
        yWithIndex = [r[1] for r in results]
        yWithoutIndex = [r[2] for r in results]

        seriesData = {
            'С индексом': (xValues, yWithIndex),
            'Без индекса': (xValues, yWithoutIndex)
        }

        builder.buildChart(
            seriesData,
            f"SimpleDB DELETE WHERE по {dataType} полю",
            "Количество строк в таблице",
            "Время выполнения (сек)",
            plotPath,
            True
        )

def runSimpleDbSelectNumber(resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    runSelectBenchmark('number', resultsDir, showPlots, rowCounts, repeats, queriesPerRun)

def runSimpleDbSelectString(resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    runSelectBenchmark('string', resultsDir, showPlots, rowCounts, repeats, queriesPerRun)

def runSimpleDbInsertNumber(resultsDir, showPlots, rowCounts, repeats):
    runInsertBenchmark('number', resultsDir, showPlots, rowCounts, repeats)

def runSimpleDbInsertString(resultsDir, showPlots, rowCounts, repeats):
    runInsertBenchmark('string', resultsDir, showPlots, rowCounts, repeats)

def runSimpleDbDeleteNumber(resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    runDeleteBenchmark('number', resultsDir, showPlots, rowCounts, repeats, queriesPerRun)

def runSimpleDbDeleteString(resultsDir, showPlots, rowCounts, repeats, queriesPerRun):
    runDeleteBenchmark('string', resultsDir, showPlots, rowCounts, repeats, queriesPerRun)
