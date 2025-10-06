import os
import random
import timeit
from decimal import Decimal
from datetime import datetime, timezone
from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql, getTableNames
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from lib.managers.sandboxManager import SandboxManager

ROW_COUNTS_DEFAULT = [5, 50]
REPEATS_DEFAULT = 3
RANDOM_SEED = 12345
USE_OPTIMIZED_FLOW = True
PK_ROW_COUNTS = [10, 50]
STRING_INDEX_ROW_COUNTS = [1000, 5000]
PK_RUNS = 5
PK_QUERIES_PER_RUN = 100
STRING_QUERIES = 100
STRING_INDEX_SAMPLE_QUERIES = 100
FTS_ROW_COUNTS = [1000, 5000, 10000, 20000, 40000]
FTS_SAMPLE_QUERIES = 100
FTS_MULTI_SAMPLE_QUERIES = 100
FTS_DICTIONARY = [
    'error','warning','timeout','failure','success','update','insert','delete','select','commit',
    'rollback','network','disk','memory','cache','index','table','query','plan','analyze',
    'optimize','engine','thread','process','batch','user','session','login','logout','permission',
    'denied','granted','read','write','latency','throughput','overflow','underflow','exception','handler'
]

random.seed(RANDOM_SEED)

SANDBOX_SCHEMA_NAME = 'research_sandbox'

def medianValue(values):
    valueLength = len(values)
    if valueLength == 0:
        return 0.0
    sortedArray = list(values)
    sortedArray.sort()
    middleIndex = valueLength // 2
    if valueLength % 2 == 1:
        return sortedArray[middleIndex]
    return (sortedArray[middleIndex - 1] + sortedArray[middleIndex]) / 2.0

def measureExecutionTime(func):
    startTime = timeit.default_timer()
    result = func()
    endTime = timeit.default_timer()
    return endTime - startTime, result

def measureAverageTime(func, repeats=None):
    if repeats is None:
        repeats = REPEATS_DEFAULT
    times = timeit.repeat(func, repeat=repeats, number=1)
    avgTime = sum(times) / len(times)
    result = func()
    return avgTime, result

def buildSeriesDict(results):
    seriesDict = {}
    for tableName, tableResults in results.items():
        xValues = []
        yValues = []
        for rowCount, executionTime in tableResults:
            xValues.append(rowCount)
            yValues.append(executionTime)
        seriesDict[tableName] = (xValues, yValues)
    return seriesDict

def saveCombinedChart(seriesDict, title, fileName, saveDir, isRaster):
    builder = PlotBuilder(saveDir)
    return builder.buildChart(seriesDict, title, 'Строки', 'Время (с)', fileName, isRaster)
