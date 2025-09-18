import os
import argparse
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.databaseConnection import getDbConnection
from lib.tableModels import getCreateTablesSql

from generationSpeed import measureGenerationSpeed
from queryPerformance import measureQueryPerformance
from joinAnalysis import analyzeJoinPerformance
from indexPerformance import measureIndexPerformance
from customDbResearch import runCustomDbResearch


def runBenchmarks(configPath: str) -> None:
    with getDbConnection() as (conn, cur):
        table_names = [
            "ticket", "session", "hall", "cinema", "movie_review",
            "favorite_movies", "viewer_profile", "movie", "viewer"
        ]
        index = 0
        while index < len(table_names):
            table = table_names[index]
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            index = index + 1
        ddls = getCreateTablesSql()
        idx = 0
        while idx < len(ddls):
            cur.execute(ddls[idx])
            idx = idx + 1
        index = 0
        while index < len(table_names):
            table = table_names[index]
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT FROM pg_class WHERE relname = '""" + table + """_id_seq'
                    ) THEN
                        PERFORM setval('""" + table + """_id_seq', 1, false);
                    END IF;
                END $$;
            """)
            index = index + 1

    with open(configPath, 'r', encoding='utf-8') as configFile:
        config = json.load(configFile)

    resultsDir = config.get('resultsDirectory', 'benchmarkResults')
    if not os.path.isdir(resultsDir):
        os.makedirs(resultsDir, exist_ok=True)

    tablesConfig = config.get('tables', {})
    generationResultsPath = os.path.join(resultsDir, 'generation_speed.csv')
    generationImagePath = os.path.join(resultsDir, 'generation_speed.png')
    print("Measuring data generation speed →", generationResultsPath, "and image →", generationImagePath)
    measureGenerationSpeed(
        tablesConfig=tablesConfig,
        outputCsvPath=generationResultsPath,
        outputImagePath=generationImagePath
    )

    queriesConfig = config.get('queries', [])
    queryResultsPath = os.path.join(resultsDir, 'query_performance.csv')
    queryImageDir = os.path.join(resultsDir, 'query_images')
    print("Measuring query performance →", queryResultsPath, "and images →", queryImageDir)
    measureQueryPerformance(
        queriesConfig=queriesConfig,
        outputCsvPath=queryResultsPath,
        outputImageDir=queryImageDir
    )

    joinConfig = config.get('joinSettings', [])
    joinResultsPath = os.path.join(resultsDir, 'join_analysis.csv')
    print("Analyzing JOIN performance →", joinResultsPath)
    analyzeJoinPerformance(
        joinConfig=joinConfig,
        outputCsvPath=joinResultsPath
    )

    indexConfig = config.get('indexSettings', [])
    indexResultsPath = os.path.join(resultsDir, 'index_performance.csv')
    print("Testing index impact →", indexResultsPath)
    measureIndexPerformance(
        indexConfig=indexConfig,
        outputCsvPath=indexResultsPath
    )

    customConfig = config.get('customSettings', {})
    customResultsPath = os.path.join(resultsDir, 'custom_db_research.csv')
    print("Running custom DB research →", customResultsPath)
    runCustomDbResearch(
        researchConfig=customConfig,
        outputCsvPath=customResultsPath
    )

    print("All benchmarks completed. Results saved in:", resultsDir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run full suite of database performance benchmarks."
    )
    defaultConfigPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    parser.add_argument(
        '-c', '--config',
        type=str,
        default=defaultConfigPath,
        help="Path to JSON config file with benchmark parameters."
    )
    args = parser.parse_args()
    runBenchmarks(args.config)
