import os, shutil, random, time
from lib.simpledb.schema import Schema
from lib.simpledb.paths import TableFiles
from lib.simpledb.engine.table_engine import TableEngine
from lib.visualization.plots import PlotBuilder

ROW_COUNTS = [100, 500, 1000]
REPEATS = 3
RANDOM_SEED = 8642
BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'foldersForSimpleDb')
DATA_DIR = os.path.join(BASE_TEMP_DIR, 'simpledb_bench_delete_num')
CSV_FILE = 'simpledb_delete_number.csv'
PLOT_FILE = 'simpledb_delete_number'

def reset():
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

random.seed(RANDOM_SEED)

def schemaWithIndex():
    return Schema('del_num_idx', [
        {"name": "id", "type": "INT", "index": True},
        {"name": "val", "type": "INT"}
    ])

def schemaNoIndex():
    return Schema('del_num_plain', [
        {"name": "id", "type": "INT"},
        {"name": "val", "type": "INT"}
    ])

def createEngine(schema):
    files = TableFiles(DATA_DIR, schema.tableName)
    eng = TableEngine(files, schema); eng.create(); eng.open(); return eng

def populate(eng, n):
    for i in range(1, n + 1):
        eng.insertRow({"id": i, "val": random.randint(0, 1000000)})

# измеряем время удаления одного значения (среднее)

def measureDelete(eng, n, targetId):
    total = 0.0
    for r in range(REPEATS):
        start = time.perf_counter()
        eng.deleteWhere('id', targetId)
        end = time.perf_counter()
        total += (end - start)
    return total / REPEATS

def runSimpleDbDeleteNumber(outputDir, raster=True):
    if not os.path.isdir(outputDir):
        os.makedirs(outputDir, exist_ok=True)
    withIdx = []
    noIdx = []
    for n in ROW_COUNTS:
        print('SimpleDB DELETE NUM rowCount', n, flush=True)
        reset(); engI = createEngine(schemaWithIndex()); populate(engI, n); target = n//2; tI = measureDelete(engI, n, target); engI.close()
        reset(); engN = createEngine(schemaNoIndex()); populate(engN, n); target2 = n//2; tN = measureDelete(engN, n, target2); engN.close()
        withIdx.append((n, tI)); noIdx.append((n, tN))
        print('  delete idx', f'{tI:.6f}', 'no_idx', f'{tN:.6f}', flush=True)
    csvPath = os.path.join(outputDir, CSV_FILE)
    with open(csvPath, 'w', encoding='utf-8') as f:
        f.write('rows,with_index,no_index\n')
        for i in range(len(withIdx)):
            f.write(str(withIdx[i][0]) + ',' + str(withIdx[i][1]) + ',' + str(noIdx[i][1]) + '\n')
    builder = PlotBuilder(outputDir)
    xsI = [p[0] for p in withIdx]; ysI = [p[1] for p in withIdx]
    xsN = [p[0] for p in noIdx]; ysN = [p[1] for p in noIdx]
    series = {'С индексом': (xsI, ysI), 'Без индекса': (xsN, ysN)}
    builder.buildChart(series, 'SimpleDB: DELETE WHERE по числовому полю (время одной операции)', 'Строки', 'Время (с)', PLOT_FILE, raster, logY=False)
    builder.buildChart(series, 'SimpleDB: DELETE WHERE по числовому полю (log)', 'Строки', 'Время (с)', PLOT_FILE + '_log', raster, logY=True)
    return {'with_index': withIdx, 'no_index': noIdx, 'csv': csvPath}

if __name__ == '__main__':
    rootProjectDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    outputDir = os.path.join(rootProjectDir, 'benchmarkResults')
    runSimpleDbDeleteNumber(outputDir, True)
