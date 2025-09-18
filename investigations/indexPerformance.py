from lib.databaseConnection import getDbConnection
import csv
import timeit
from typing import List, Dict, Any
import os
import matplotlib.pyplot as plt


def measureIndexPerformance(
    indexConfig: List[Dict[str, Any]],
    outputCsvPath: str
) -> None:
    results = []
    idx = 0
    while idx < len(indexConfig):
        entry = indexConfig[idx]
        table = entry.get('table')
        columns = entry.get('columns', [])
        indexType = entry.get('indexType', 'btree')
        if not table or len(columns) == 0:
            idx = idx + 1
            continue
        indexName = "idx_" + table + "_" + "_".join(columns)
        whereParts = []
        i = 0
        while i < len(columns):
            whereParts.append(columns[i] + " = %s")
            i = i + 1
        whereClause = " AND ".join(whereParts)
        sampleSql = "SELECT COUNT(*) FROM " + table + " WHERE " + whereClause
        params = []
        i = 0
        while i < len(columns):
            params.append(None)
            i = i + 1
        def runWithoutIndex():
            with getDbConnection() as (_, cur):
                cur.execute(sampleSql, params)
                cnt = cur.fetchone()[0]
                return cnt
        def runWithIndex():
            with getDbConnection() as (_, cur):
                cur.execute(sampleSql, params)
                cnt = cur.fetchone()[0]
                return cnt
        timeWithout = min(timeit.Timer(runWithoutIndex).repeat(repeat=5, number=1))
        with getDbConnection() as (conn, cur):
            cur.execute(
                "CREATE INDEX " + indexName + " ON " + table + " USING " + indexType + " (" + ", ".join(columns) + ")"
            )
        timeWith = min(timeit.Timer(runWithIndex).repeat(repeat=5, number=1))
        with getDbConnection() as (conn, cur):
            cur.execute("DROP INDEX IF EXISTS " + indexName)
        results.append((table, columns, indexType, timeWithout, timeWith))
        idx = idx + 1
    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['table', 'columns', 'indexType', 'time_without_index', 'time_with_index'])
        i = 0
        while i < len(results):
            row = results[i]
            columnsStr = ""; j = 0
            while j < len(row[1]):
                if j > 0:
                    columnsStr = columnsStr + ";"
                columnsStr = columnsStr + row[1][j]
                j = j + 1
            writer.writerow([row[0], columnsStr, row[2], f"{row[3]:.6f}", f"{row[4]:.6f}"])
            i = i + 1
    imgPath = outputCsvPath.replace('.csv', '.png')
    plt.figure(figsize=(10, 6))
    xVals = []
    withoutVals = []
    withVals = []
    i = 0
    while i < len(results):
        xVals.append(i)
        withoutVals.append(results[i][3])
        withVals.append(results[i][4])
        i = i + 1
    plt.bar(xVals, withoutVals, label='Без индекса')
    plt.bar(xVals, withVals, bottom=withoutVals, label='С индексом')
    labels = []
    i = 0
    while i < len(results):
        labels.append(results[i][0])
        i = i + 1
    plt.xticks(xVals, labels, rotation=45)
    plt.ylabel('Time (s)')
    plt.title('Index Performance')
    plt.legend()
    plt.tight_layout()
    plt.savefig(imgPath)
    plt.close()
