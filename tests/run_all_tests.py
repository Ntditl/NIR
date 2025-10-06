import pytest
import sys
import os

SEPARATOR_LINE_WIDTH = 80

def run_all_tests():
    print("=" * SEPARATOR_LINE_WIDTH)
    print("ЗАПУСК ВСЕХ ТЕСТОВ ДЛЯ ПУНКТА 4 ТЗ")
    print("=" * SEPARATOR_LINE_WIDTH)

    current_dir = os.path.dirname(os.path.abspath(__file__))

    test_files = [
        os.path.join(current_dir, 'test_models.py'),
        os.path.join(current_dir, 'test_generators.py'),
        os.path.join(current_dir, 'test_dataManager.py'),
        os.path.join(current_dir, 'test_backupManager.py'),
        os.path.join(current_dir, 'test_sandboxManager.py'),
        os.path.join(current_dir, 'test_researchUtils.py'),
        os.path.join(current_dir, 'test_plots.py')
    ]

    print("\nЗапуск тестов для проверки пункта 4 ТЗ:")
    print("4a. Создание таблиц (test_models.py)")
    print("4b. Генерация данных (test_generators.py)")
    print("4c. Песочница (test_sandboxManager.py)")
    print("4d. Сохранение данных (включено в test_generators.py)")
    print("4e. Удаление/замена данных (test_dataManager.py)")
    print("4f. Бэкап и восстановление (test_backupManager.py)")
    print("4g. Автоматический commit (включено во все тесты)")
    print("4h. Измерение времени timeit (test_researchUtils.py)")
    print("4i. Построение графиков (test_plots.py)")
    print("\n" + "=" * SEPARATOR_LINE_WIDTH + "\n")

    exit_code = pytest.main(['-v', '--tb=short'] + test_files)

    if exit_code == 0:
        print("\n" + "=" * SEPARATOR_LINE_WIDTH)
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("=" * SEPARATOR_LINE_WIDTH)
    else:
        print("\n" + "=" * SEPARATOR_LINE_WIDTH)
        print("❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")
        print("=" * SEPARATOR_LINE_WIDTH)

    return exit_code

if __name__ == '__main__':
    sys.exit(run_all_tests())
