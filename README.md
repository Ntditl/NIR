Проект исследований производительности БД (PostgreSQL)

Требования
- Python 3.13+
- Установить psycopg2 и matplotlib (остальные сторонние библиотеки не используются)
- PostgreSQL, база и пользователь из configsettings.txt

Настройка
1) Файл configsettings.txt в корне должен содержать строки:
   user=postgres
   password=postgres
   host=localhost
   port=5432
   dbname=your_db

2) Убедитесь, что база создана в кодировке UTF-8.

Быстрый старт (Windows cmd)
- Запуск полного бенчмарка:
  python investigations\runBenchmarks.py -c investigations\paramsSettings.json

- Проверки (простые исполняемые тесты):
  python tests\testDatabaseConnection.py
  python tests\testTableModels.py
  python tests\testTimingUtils.py
  python tests\testDataGenerator.py
  python tests\teskBackupManager.py

Примечания
- Схема создаётся и дропается из кода (lib/tableModels.py).
- Даты/время — TIMESTAMPTZ, строки — UTF-8, телефон хранится как VARCHAR.
- Для восстановления бэкапов временно отключается триггер списания мест на ticket.

