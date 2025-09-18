import csv
import os
import timeit
from typing import List, Dict, Any
from lib.databaseConnection import getDbConnection


def analyzeJoinPerformance(
    joinConfig: List[Dict[str, Any]],
    outputCsvPath: str
) -> None:
    results = []

    idx = 0
    while idx < len(joinConfig):
        entry = joinConfig[idx]
        name = entry.get('name')
        leftTable = entry.get('leftTable')
        rightTable = entry.get('rightTable')
        condition = entry.get('joinCondition')
        repeats = entry.get('repeats', 1)
        if not name or not leftTable or not rightTable or not condition:
            idx = idx + 1
            continue
        sql = "SELECT COUNT(*) FROM " + leftTable + " JOIN " + rightTable + " ON " + condition
        def runOnce():
            with getDbConnection() as (conn, cur):
                cur.execute(sql)
                cnt = cur.fetchone()[0]
                return cnt
        timer = timeit.Timer(runOnce)
        times = timer.repeat(repeat=repeats, number=1)
        bestTime = min(times)
        results.append((name, leftTable, rightTable, repeats, bestTime))
        idx = idx + 1

    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'left_table', 'right_table', 'repeats', 'best_time_seconds'])
        i = 0
        while i < len(results):
            row = results[i]
            writer.writerow([row[0], row[1], row[2], row[3], f"{row[4]:.6f}"])
            i = i + 1

    import matplotlib.pyplot as plt
    names = []
    timesVals = []
    i = 0
    while i < len(results):
        names.append(results[i][0])
        timesVals.append(results[i][4])
        i = i + 1
    plt.figure(figsize=(10, 6))
    plt.plot(names, timesVals, marker='o')
    plt.title('JOIN Performance')
    plt.xlabel('Join name')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.savefig(outputCsvPath.replace('.csv', '.png'))
    plt.close()
