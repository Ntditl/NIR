import csv
import timeit
import os
import matplotlib.pyplot as plt
import re
from typing import Dict, Any, List

from lib.databaseConnection import getDbConnection


def measureQueryPerformance(
    queriesConfig: List[Dict[str, Any]],
    outputCsvPath: str,
    outputImageDir: str = None
) -> None:
    results = []

    idx = 0
    while idx < len(queriesConfig):
        entry = queriesConfig[idx]
        name = entry.get('name')
        sqlText = entry.get('sql')
        params = entry.get('parameters', {})
        repeats = entry.get('repeats', 1)
        if not name or not sqlText:
            idx = idx + 1
            continue
        placeholderNames = re.findall(r':(\w+)', sqlText)
        sqlFormatted = re.sub(r':\w+', '%s', sqlText)
        paramValues = []
        j = 0
        while j < len(placeholderNames):
            key = placeholderNames[j]
            paramValues.append(params.get(key))
            j = j + 1
        def runOnce():
            with getDbConnection() as (conn, cur):
                cur.execute(sqlFormatted, paramValues)
        timer = timeit.Timer(runOnce)
        times = timer.repeat(repeat=repeats, number=1)
        bestTime = min(times)
        results.append((name, repeats, bestTime))
        idx = idx + 1

    dirPath = os.path.dirname(outputCsvPath)
    if dirPath != "" and not os.path.isdir(dirPath):
        os.makedirs(dirPath, exist_ok=True)
    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'repeats', 'best_time_seconds'])
        i = 0
        while i < len(results):
            row = results[i]
            writer.writerow([row[0], row[1], f"{row[2]:.6f}"])
            i = i + 1

    if outputImageDir:
        if not os.path.isdir(outputImageDir):
            os.makedirs(outputImageDir, exist_ok=True)
        plt.figure(figsize=(12, 8))
        names = []
        timesVals = []
        i = 0
        while i < len(results):
            names.append(results[i][0])
            timesVals.append(results[i][2])
            i = i + 1
        plt.bar(names, timesVals)
        plt.title('Производительность запросов')
        plt.xlabel('Запрос')
        plt.ylabel('Время выполнения (секунды)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(outputImageDir, 'query_performance.png'))
        plt.close()
