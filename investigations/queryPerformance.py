import csv
import timeit
import os
import matplotlib.pyplot as plt
import re
from typing import Dict, Any, List
from timeit import default_timer as timer

from lib.databaseConnection import getDbConnection


def _subst(sqlText: str, params: Dict[str, Any]) -> str:
    # прямое встраивание параметров вместо execute params
    def repl(m):
        key = m.group(1)
        if key not in params:
            raise KeyError(f"Missing value for placeholder :{key} in SQL: {sqlText}")
        val = params[key]
        if isinstance(val, str):
            return f"'{val}'"
        return str(val)
    pattern = re.compile(r'(?<!:):([A-Za-z_]\w*)')
    return pattern.sub(repl, sqlText)


def measureQueryPerformance(
    queriesConfig: List[Dict[str, Any]],
    outputCsvPath: str,
    outputImageDir: str = None
) -> None:
    results = []
    # Вспомогательная функция: выполнить SQL с триггерами отключенными
    def exec_no_triggers(statement: str, fetch: bool = False):
        with getDbConnection() as (conn, cur):
            cur.execute("SET session_replication_role = 'replica';")
            cur.execute(statement)
            if fetch:
                return cur.fetchall()
            return None

    idx = 0
    while idx < len(queriesConfig):
        entry = queriesConfig[idx]
        name = entry.get('name')
        sqlText = entry.get('sql') or entry.get('sqlTemplate')
        counts = entry.get('counts', [100, 1000, 5000])
        repeats = entry.get('repeats', 1)
        setupSql = entry.get('setupSql')
        cleanupSql = entry.get('cleanupSql')
        baseParams = entry.get('parameters', {})
        if not name or not sqlText:
            idx = idx + 1
            continue
        series = []
        j = 0
        while j < len(counts):
            n = counts[j]
            runParams = dict(baseParams)
            runParams['count'] = n
            # подготавливаем данные один раз
            if setupSql:
                ssql = _subst(setupSql, runParams)
                exec_no_triggers(ssql)
            # измеряем вручную: выполняем запрос и сразу очищаем данные после замера
            qsql = _subst(sqlText, runParams)
            csql = _subst(cleanupSql, runParams) if cleanupSql else None
            times = []
            irep = 0
            while irep < repeats:
                start = timer()
                # само измеряемое действие
                exec_no_triggers(qsql)
                end = timer()
                times.append(end - start)
                # очистка данных
                if csql:
                    exec_no_triggers(csql)
                irep = irep + 1
            best = min(times)
            series.append((n, best))
            j = j + 1
        results.append((name, series))
        idx = idx + 1

    outDir = os.path.dirname(outputCsvPath)
    if outDir != "" and not os.path.isdir(outDir):
        os.makedirs(outDir, exist_ok=True)
    with open(outputCsvPath, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'rows', 'best_time_seconds'])
        i = 0
        while i < len(results):
            name = results[i][0]
            series = results[i][1]
            k = 0
            while k < len(series):
                writer.writerow([name, series[k][0], f"{series[k][1]:.6f}"])
                k = k + 1
            i = i + 1

    if outputImageDir:
        if not os.path.isdir(outputImageDir):
            os.makedirs(outputImageDir, exist_ok=True)
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
                k = k + 1
            plt.figure(figsize=(10, 6))
            plt.plot(xs, ys, marker='o')
            plt.title(name)
            plt.xlabel('Rows')
            plt.ylabel('Time (s)')
            plt.grid(True)
            safeName = name.replace(' ', '_')
            plt.savefig(os.path.join(outputImageDir, safeName + '.png'))
            plt.close()
            i = i + 1
    # deleted session capacity trigger, no need to restore
