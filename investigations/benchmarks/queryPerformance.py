import csv
import timeit
import os
import re
from typing import Dict, Any, List

from lib.db.connection import getDbConnection
from lib.visualization.plots import PlotBuilder

DEFAULT_COUNTS = [100, 1000, 5000]
ENCODING_UTF8 = 'utf-8'


def _subst(sqlText: str, params: Dict[str, Any]) -> str:
    def repl(m):
        key = m.group(1)
        if key not in params:
            raise KeyError('Missing value for :' + key)
        val = params[key]
        if isinstance(val, str):
            return "'" + val + "'"
        return str(val)
    pattern = re.compile(r'(?<!:):([A-Za-z_]\w*)')
    return pattern.sub(repl, sqlText)


def measureQueryPerformance(queriesConfig: List[Dict[str, Any]], outputCsvPath: str, outputImageDir: str = None) -> None:
    results = []
    idx = 0
    while idx < len(queriesConfig):
        entry = queriesConfig[idx]
        queryName = entry.get('name')
        sqlText = entry.get('sql') or entry.get('sqlTemplate')
        counts = entry.get('counts', DEFAULT_COUNTS)
        repeats = int(entry.get('repeats', 1))
        setupSql = entry.get('setupSql')
        cleanupSql = entry.get('cleanupSql')
        baseParams = entry.get('parameters', {})
        if not queryName or not sqlText:
            idx += 1
            continue
        series = []
        j = 0
        while j < len(counts):
            currentCount = counts[j]
            runParams = dict(baseParams)
            runParams['count'] = currentCount
            if setupSql:
                ssql = _subst(setupSql, runParams)
                with getDbConnection() as (conn, cur):
                    cur.execute(ssql)
            qsql = _subst(sqlText, runParams)
            csql = _subst(cleanupSql, runParams) if cleanupSql else None
            times = []
            repIndex = 0
            while repIndex < repeats:
                def run():
                    with getDbConnection() as (conn, cur):
                        cur.execute(qsql)
                t = timeit.Timer(run).timeit(number=1)
                times.append(t)
                if csql:
                    with getDbConnection() as (conn, cur):
                        cur.execute(csql)
                repIndex += 1
            best = times[0]
            k = 1
            while k < len(times):
                if times[k] < best:
                    best = times[k]
                k += 1
            total = 0.0
            k = 0
            while k < len(times):
                total += times[k]
                k += 1
            avg = total / len(times)
            series.append((currentCount, best, avg, repeats))
            j += 1
            idxSetupClean = None
        results.append((queryName, series))
        idx += 1
    outDir = os.path.dirname(outputCsvPath)
    if outDir != '' and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, mode='w', newline='', encoding=ENCODING_UTF8) as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(['name', 'rows', 'repeats', 'best_time_seconds', 'avg_time_seconds'])
        i = 0
        while i < len(results):
            name = results[i][0]
            series = results[i][1]
            s = 0
            while s < len(series):
                row = series[s]
                writer.writerow([name, row[0], row[3], f"{row[1]:.6f}", f"{row[2]:.6f}"])
                s += 1
            i += 1
    if outputImageDir:
        if not os.path.isdir(outputImageDir):
            os.makedirs(outputImageDir, exist_ok=True)
        builder = PlotBuilder(outputImageDir)
        i = 0
        while i < len(results):
            name = results[i][0]
            series = results[i][1]
            xs = []
            ys = []
            k = 0
            while k < len(series):
                xs.append(series[k][0])
                ys.append(series[k][1])
                k += 1
            builder.buildChart({name: (xs, ys)}, name, 'Rows', 'Time (s)', name.replace(' ', '_'), True)
            i += 1
