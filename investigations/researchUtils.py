import os
import random
import sys
import timeit
from decimal import Decimal
from datetime import datetime, timezone
from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql, getTableNames
from lib.data.generators import RandomDataGenerator
from lib.visualization.plots import PlotBuilder
from lib.managers.sandboxManager import SandboxManager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SANDBOX_SCHEMA_NAME = 'sandbox_research'

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
    executionTime = timeit.timeit(func, number=1)
    result = func()
    return executionTime, result

def measureAverageTime(func, repeats=3):
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
