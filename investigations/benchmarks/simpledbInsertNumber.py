import os, shutil, random, time
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine
from lib.visualization.plots import PlotBuilder

ROW_COUNTS = [100, 500, 1000]
REPEATS = 3
RANDOM_SEED = 24680
DATA_DIR = 'simpledb_bench_insert_num'
CSV_FILE = 'simpledb_insert_number.csv'
PLOT_FILE = 'simpledb_insert_number'

random.seed(RANDOM_SEED)

def reset():
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

def schemaWithIndex():
    return Schema('ins_idx', [
        {"name": "id", "type": "INT", "index": True},
        {"name": "val", "type": "INT"}
    ])

def schemaNoIndex():
    return Schema('ins_plain', [
        {"name": "id", "type": "INT"},
        {"name": "val", "type": "INT"}
    ])

def createEngine(schema):
    files = TableFiles(DATA_DIR, schema.tableName)
    eng = TableEngine(files, schema)
    eng.create(); eng.open(); return eng

def measureInsert(eng, rowCount):
    total = 0.0
    for r in range(REPEATS):
        start = time.perf_counter()
        for i in range(1, rowCount + 1):
            eng.insertRow({"id": i, "val": random.randint(0, 1000000)})
        end = time.perf_counter()
        total += (end - start)
    return (total / REPEATS) / rowCount  # среднее время одной вставки

def runSimpleDbInsertNumber(outputDir, raster=True):
    if not os.path.isdir(outputDir):
        os.makedirs(outputDir, exist_ok=True)
    withIdx = []
    noIdx = []
    for n in ROW_COUNTS:
        print('SimpleDB INSERT NUM rowCount', n, flush=True)
        reset()
        engIdx = createEngine(schemaWithIndex())
        tIdx = measureInsert(engIdx, n); engIdx.close()
        reset()
        engNo = createEngine(schemaNoIndex())
        tNo = measureInsert(engNo, n); engNo.close()
        withIdx.append((n, tIdx)); noIdx.append((n, tNo))
        print('  per insert idx', f'{tIdx:.6f}', 'no_idx', f'{tNo:.6f}', flush=True)
    csvPath = os.path.join(outputDir, CSV_FILE)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write('rows,with_index,no_index\n')
        for i in range(len(withIdx)):
            f.write(str(withIdx[i][0]) + ',' + str(withIdx[i][1]) + ',' + str(noIdx[i][1]) + '\n')
    builder = PlotBuilder(outputDir)
    xsI = [p[0] for p in withIdx]; ysI = [p[1] for p in withIdx]
    xsN = [p[0] for p in noIdx]; ysN = [p[1] for p in noIdx]
    series = {'С индексом': (xsI, ysI), 'Без индекса': (xsN, ysN)}
    builder.buildChart(series, 'SimpleDB: INSERT по числовому полю (время одной вставки)', 'Строки', 'Время (с)', PLOT_FILE, raster, logY=False)
    builder.buildChart(series, 'SimpleDB: INSERT по числовому полю (log)', 'Строки', 'Время (с)', PLOT_FILE + '_log', raster, logY=True)
    return {'with_index': withIdx,'no_index': noIdx,'csv': csvPath}

if __name__ == '__main__':
    runSimpleDbInsertNumber(os.path.join('investigations','benchmarkResults'), True)

