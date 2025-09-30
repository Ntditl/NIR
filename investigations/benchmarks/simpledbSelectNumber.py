import os
import shutil
import random
import time
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine
from lib.visualization.plots import PlotBuilder

ROW_COUNTS = [100, 300, 600, 850, 1000]
REPEATS = 3
QUERIES_PER_RUN = 100
RANDOM_SEED = 12345
BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'foldersForSimpleDb')
DATA_DIR = os.path.join(BASE_TEMP_DIR, 'select_number')
CSV_FILE = 'simpledb_select_number.csv'
PLOT_FILE = 'simpledb_select_number'

random.seed(RANDOM_SEED)

def clearDataDir():
    os.makedirs(BASE_TEMP_DIR, exist_ok=True)
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

def buildSchemaIndexed(tableName):
    return Schema(tableName, [
        {"name": "id", "type": "INT", "index": True},
        {"name": "val", "type": "INT"}
    ])

def buildSchemaPlain(tableName):
    return Schema(tableName, [
        {"name": "id", "type": "INT"},
        {"name": "val", "type": "INT"}
    ])

def createEngine(schema):
    files = TableFiles(DATA_DIR, schema.tableName)
    eng = TableEngine(files, schema)
    eng.create()
    eng.open()
    return eng

def populate(engine, rowCount):
    for i in range(1, rowCount + 1):
        engine.insertRow({"id": i, "val": random.randint(0, 1000000)})

def buildIndex(engine):
    engine._rebuildSingleIndex('id')

def measureSelect(engine, rowCount):
    totalTime = 0.0
    for r in range(REPEATS):
        startTime = time.perf_counter()
        for q in range(QUERIES_PER_RUN):
            targetId = random.randint(1, rowCount)
            engine.select(['id'], ('id', targetId))
        endTime = time.perf_counter()
        runTime = (endTime - startTime) / QUERIES_PER_RUN
        totalTime += runTime
    return totalTime / REPEATS

def runSimpleDbSelectNumber(outputDir, raster=True):
    if not os.path.isdir(outputDir):
        os.makedirs(outputDir, exist_ok=True)
    resultsIdx = []
    resultsNo = []
    for rowCount in ROW_COUNTS:
        print('SimpleDB rowCount', rowCount, flush=True)
        clearDataDir()
        schemaIdx = buildSchemaIndexed('num_idx')
        engIdx = createEngine(schemaIdx)
        populate(engIdx, rowCount)
        buildIndex(engIdx)
        tIdx = measureSelect(engIdx, rowCount)
        engIdx.close()
        clearDataDir()
        schemaNo = buildSchemaPlain('num_plain')
        engNo = createEngine(schemaNo)
        populate(engNo, rowCount)
        tNo = measureSelect(engNo, rowCount)
        engNo.close()
        resultsIdx.append((rowCount, tIdx))
        resultsNo.append((rowCount, tNo))
        print('  idx', f'{tIdx:.6f}', 'no_idx', f'{tNo:.6f}', flush=True)
    csvPath = os.path.join(outputDir, CSV_FILE)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write('rows,with_index,no_index\n')
        for i in range(len(resultsIdx)):
            f.write(str(resultsIdx[i][0]) + ',' + str(resultsIdx[i][1]) + ',' + str(resultsNo[i][1]) + '\n')
    builder = PlotBuilder(outputDir)
    xsIdx = [p[0] for p in resultsIdx]
    ysIdx = [p[1] for p in resultsIdx]
    xsNo = [p[0] for p in resultsNo]
    ysNo = [p[1] for p in resultsNo]
    seriesDict = {
        'С индексом': (xsIdx, ysIdx),
        'Без индекса': (xsNo, ysNo)
    }
    linearPath = builder.buildChart(seriesDict, 'SimpleDB: SELECT WHERE по числовому полю', 'Строки', 'Время (с)', PLOT_FILE, raster, logY=False)
    logPath = builder.buildChart(seriesDict, 'SimpleDB: SELECT WHERE по числовому полю (log)', 'Строки', 'Время (с)', PLOT_FILE + '_log', raster, logY=True)
    return {
        'with_index': resultsIdx,
        'no_index': resultsNo,
        'csv': csvPath,
        'plot_linear': linearPath,
        'plot_log': logPath
    }

if __name__ == '__main__':
    runSimpleDbSelectNumber(os.path.join('investigations', 'benchmarkResults'), True)
