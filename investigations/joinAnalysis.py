import csv
import os
import timeit
from typing import List, Dict, Any
from lib.databaseConnection import getDbConnection


def analyzeJoinPerformance(joinConfig: List[Dict[str, Any]], outputCsvPath: str) -> None:
    results = []

    configIndex = 0
    while configIndex < len(joinConfig):
        currentEntry = joinConfig[configIndex]
        joinName = currentEntry.get('name')
        leftTableName = currentEntry.get('leftTable')
        rightTableName = currentEntry.get('rightTable')
        joinCondition = currentEntry.get('joinCondition')
        repeatCount = currentEntry.get('repeats', 1)

        if not joinName or not leftTableName or not rightTableName or not joinCondition:
            configIndex = configIndex + 1
            continue

        sqlQuery = "SELECT COUNT(*) FROM " + leftTableName + " JOIN " + rightTableName + " ON " + joinCondition

        def executeOnce():
            with getDbConnection() as (conn, cur):
                cur.execute(sqlQuery)
                recordCount = cur.fetchone()[0]
                return recordCount

        timer = timeit.Timer(executeOnce)
        executionTimes = timer.repeat(repeat=repeatCount, number=1)
        bestExecutionTime = min(executionTimes)
        results.append((joinName, leftTableName, rightTableName, repeatCount, bestExecutionTime))
        configIndex = configIndex + 1

    outputDirectory = os.path.dirname(outputCsvPath)
    if outputDirectory != "" and not os.path.isdir(outputDirectory):
        os.makedirs(outputDirectory, exist_ok=True)

    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvFile:
        csvWriter = csv.writer(csvFile)
        csvWriter.writerow(['name', 'left_table', 'right_table', 'repeats', 'best_time_seconds'])

        resultIndex = 0
        while resultIndex < len(results):
            currentResult = results[resultIndex]
            resultName = currentResult[0]
            resultLeftTable = currentResult[1]
            resultRightTable = currentResult[2]
            resultRepeats = currentResult[3]
            resultTime = currentResult[4]
            formattedTime = f"{resultTime:.6f}"
            csvWriter.writerow([resultName, resultLeftTable, resultRightTable, resultRepeats, formattedTime])
            resultIndex = resultIndex + 1

    import matplotlib.pyplot as plt
    joinNames = []
    executionTimes = []

    plotIndex = 0
    while plotIndex < len(results):
        currentResult = results[plotIndex]
        joinNames.append(currentResult[0])
        executionTimes.append(currentResult[4])
        plotIndex = plotIndex + 1

    plt.figure(figsize=(10, 6))
    plt.plot(joinNames, executionTimes, marker='o')
    plt.title('JOIN Performance')
    plt.xlabel('Join name')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    pngPath = outputCsvPath.replace('.csv', '.png')
    plt.savefig(pngPath)
    plt.close()
