import csv
import timeit
from typing import List, Dict, Any

from lib.db.connection import getDbSession


def runCustomDbResearch(
    researchConfig: Dict[str, Any],
    outputCsvPath: str
) -> None:
    queries = researchConfig.get('researchQueries', [])
    results = []

    index = 0
    while index < len(queries):
        entry = queries[index]
        name = entry.get('name')
        sqlText = entry.get('sql')
        repeats = entry.get('repeats', 1)
        if not name or not sqlText:
            index = index + 1
            continue
        def _runOnce():
            with getDbSession() as session:
                session.execute(sqlText)
                session.all()
        timer = timeit.Timer(_runOnce)
        times = timer.repeat(repeat=repeats, number=1)
        bestTime = min(times)
        results.append((name, repeats, bestTime))
        index = index + 1

    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'repeats', 'best_time_seconds'])
        i = 0
        while i < len(results):
            row = results[i]
            writer.writerow([row[0], row[1], f"{row[2]:.6f}"])
            i = i + 1
