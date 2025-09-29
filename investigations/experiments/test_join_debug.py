import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from joinAnalysis import analyzeJoinPerformance
from lib.tableModels import recreateAllTables
from lib.randomDataGenerator import RandomDataGenerator

def testJoinAnalysis():
    print("Recreating database...")
    recreateAllTables(verbose=False)

    print("Generating test data...")
    generator = RandomDataGenerator()
    generator.generateData(100, 100, 5, 2, 3, 0.1, 0.1, 0.1)

    print("Loading join configuration...")
    configPath = os.path.join(os.path.dirname(__file__), 'paramsSettings.json')
    with open(configPath, 'r', encoding='utf-8') as f:
        config = json.load(f)

    joinConfig = config.get('joinSettings', [])
    print(f"Found {len(joinConfig)} join configurations")

    if len(joinConfig) > 0:
        print("First join config:", joinConfig[0])

    testCsvPath = os.path.join(os.path.dirname(__file__), 'test_join_results.csv')
    print(f"Running JOIN analysis, output to: {testCsvPath}")

    analyzeJoinPerformance(joinConfig, testCsvPath)

    print("Checking if file was created...")
    if os.path.exists(testCsvPath):
        with open(testCsvPath, 'r', encoding='utf-8') as f:
            content = f.read()
            print(f"File size: {len(content)} characters")
            print("File content:")
            print(content)
    else:
        print("File was NOT created!")

if __name__ == '__main__':
    testJoinAnalysis()
