import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.queryPerformance import measureQueryPerformance
from lib.db.models import recreateAllTables
from lib.data.generators import RandomDataGenerator
from lib.db.connection import getDbConnection

def _prepareBaseData():
    gen = RandomDataGenerator()
    # Базовые наборы (небольшие) чтобы были FK-родители
    with getDbConnection() as (conn, cur):
        gen._generateCinemasAndHalls(cur, cinemasCount=5, hallsPerCinema=2)
        gen._generateMovies(cur, 10)
        gen._generateViewers(cur, 10)


def testJoinQueries():
    print("Recreating database (with indexes)...")
    recreateAllTables(withIndexes=True)
    print("Generating base parent data for FK ...")
    _prepareBaseData()

    configPath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'paramsSettings.json')
    if not os.path.exists(configPath):
        print('Config not found:', configPath)
        return
    with open(configPath, 'r', encoding='utf-8') as f:
        config = json.load(f)
    allQueries = config.get('queries', [])
    joinQueries = []
    for q in allQueries:
        name = q.get('name', '')
        # эвристика: берём все запросы где есть 'join' в имени
        if 'join' in name.lower():
            joinQueries.append(q)
    if len(joinQueries) == 0:
        print('No join queries detected in configuration.')
        return
    print('Detected join queries:', [q.get('name') for q in joinQueries])
    resultsDir = os.path.join(os.path.dirname(__file__))
    csvPath = os.path.join(resultsDir, 'test_join_results.csv')
    imgDir = os.path.join(resultsDir, 'test_join_images')
    measureQueryPerformance(joinQueries, csvPath, imgDir)
    if os.path.exists(csvPath):
        with open(csvPath, 'r', encoding='utf-8') as f:
            content = f.read()
        print('Join benchmark CSV size:', len(content), 'bytes')
        print('First 300 chars:\n', content[:300])
    else:
        print('Join benchmark CSV not created')

if __name__ == '__main__':
    testJoinQueries()
