import sys
import os
import importlib
import traceback


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TEST_MODULES = []

TEST_MODULES.append('tests.manual.test_simpledb_manual')
TEST_MODULES.append('tests.manual.test_timing_manual')
TEST_MODULES.append('tests.manual.test_db_models_manual')
TEST_MODULES.append('tests.manual.test_data_manager_manual')
TEST_MODULES.append('tests.manual.test_generator_manual')
TEST_MODULES.append('tests.manual.test_sandbox_backup_manual')
TEST_MODULES.append('tests.manual.test_plots_manual')
TEST_MODULES.append('tests.manual.test_simpledb_sql_manual')
TEST_MODULES.append('tests.manual.test_sql_parser_manual')
TEST_MODULES.append('tests.manual.test_main_manual')


def run_all():
    passedCount = 0
    failedCount = 0
    totalCount = 0
    i = 0
    while i < len(TEST_MODULES):
        name = TEST_MODULES[i]
        print('=== RUN', name)
        try:
            mod = importlib.import_module(name)
            runFunc = getattr(mod, 'run', None)
            if runFunc is None:
                print('[SKIP] нет функции run() в', name)
            else:
                runFunc()
                print('[OK]', name)
                passedCount = passedCount + 1
        except Exception as e:
            failedCount = failedCount + 1
            print('[FAIL]', name, str(e))
            traceback.print_exc()
        totalCount = totalCount + 1
        i = i + 1
    print('=== SUMMARY total', totalCount, 'passed', passedCount, 'failed', failedCount)
    if failedCount > 0:
        raise SystemExit(1)


if __name__ == '__main__':
    run_all()
