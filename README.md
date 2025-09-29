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

---
## Соответствие требованиям (tzRead2.md)

Ниже указано, какой файл/модуль покрывает каждый подпункт задания и как реализованы ключевые концепции.

### 1. Выбор СУБД
PostgreSQL — используется везде через модуль подключения `lib/db/connection.py`.

### 2. Схема БД и типы связей
Файл: `lib/db/models.py`
- Таблицы: cinema, hall, movie, session, viewer, viewer_profile, movie_review, ticket, favorite_movies.
- Связи:
  - 1–1: viewer_profile → viewer (UNIQUE FK)
  - 1–M: cinema→hall, hall→session, movie→session, viewer→movie_review, movie→movie_review, session→ticket, viewer→ticket
  - M–M: viewer ↔ favorite_movies ↔ movie (таблица favorite_movies)
- Типы данных: INT / BIGSERIAL, VARCHAR / TEXT, DATE, TIMESTAMPTZ, BOOLEAN, NUMERIC — покрывают требование «числа, строки, даты».

### 3. Структура проекта
- `lib/` — основная логика и инфраструктура.
- `tests/` — исполняемые проверки (не полноценные фреймворк-тесты, но запускаемый код).
- `investigations/` — код бенчмарков и исследований.

### 4. Вспомогательные функции / классы
| ��одпункт | Реализация |
|---------|------------|
| 4.a Создание таблиц | `lib/db/models.py` (createAllTables / recreateAllTables) |
| 4.b Генерация данных | `lib/data/generators.py` + бенчмарк `generationSpeed.py` |
| 4.c Песочница | `lib/managers/sandboxManager.py` (клонирование структуры + данных + последовательностей + FK адаптация) |
| 4.d Сохранение сгенерированных данных | Методы `_generate*` в `generators.py` вставляют напрямую через курсор |
| 4.e Удаление / замена данных | Локальные SQL TRUNCATE / DELETE в скриптах + `sandboxManager.dropSandboxSchema()` |
| 4.f Бэкап / восстановление | `lib/managers/backupManager.py` (копирование в файл и обратная загрузка) |
| 4.g Автоматический commit/close | Контекстный менеджер в `lib/db/connection.py` (with getDbConnection()) |
| 4.h Тайминг запросов через timeit | Все бенчмарки: `queryPerformance.py`, `generationSpeed.py`, `indexCinema.py`, `simpleDbIndexBench.py` используют timeit / repeat; вручную start-end НЕ применяется |
| 4.i Построение графиков | `lib/visualization/plots.py` (PlotBuilder: разные цвета, стили, маркеры, легенда, сохранение PNG/SVG) |

### 5. Исследования
#### 5.a Быстрый запуск и изменение параметров
- Центральный конфиг: `investigations/paramsSettings.json` (таблицы, counts, запросы, simpleDb настройки).
- `investigations/runBenchmarks.py` — единая точка запуска.

#### 5.b Время генерации данных
Файл: `investigations/benchmarks/generationSpeed.py`
- Для одиночных таблиц и FK-групп (fkGroups в paramsSettings.json).
- Ось X: количество генерируемых строк; OY: время (best).
- Данные не вставляются в настоящую БД (in-memory структуры).

#### 5.c Время выполнения запросов
Файл: `investigations/benchmarks/queryPerformance.py`
- На каждую «основную» таблицу: минимум 3 запроса (SELECT, INSERT, DELETE) — см. секцию `queries` в paramsSettings.json.
- Для всех типов связей добавлены JOIN-серии (имена с `join`), покрывая подп. 5.c.6.
- Ось X: counts (число строк, вставляемых / выбираемых логически / удаляемых). Генерация (setupSql) вне тайминга.
- Каждый запрос → отдельный график (одна серия) через PlotBuilder.
Примечание: SELECT выполнены в форме `SELECT COUNT(*) ...` (измеряется время обработки диапазона; при необходимости легко заменить на фактическую выборку).

#### 6. Эффективность индексов (PostgreSQL)
Файл: `investigations/benchmarks/indexCinema.py`
- 6.a PK vs no PK: таблицы viewer_t1_pk / viewer_t2_nopk → SELECT (=,<) и INSERT.
- 6.b Строковый индекс: movie_t3_idx / movie_t4_no_idx → равенство, LIKE prefix%, LIKE %substr%, INSERT.
- 6.c Полнотекстовый индекс: movie_ft_t5 (GIN) / movie_ft_t6_no_idx → поиск одного и нескольких слов, INSERT.
Дополнительно: `measureGenericIndexSet` (замена старого indexPerformance) — сравнение произвольных многоколонных конфигураций по env `GENERIC_INDEX_CONFIG`.

#### 7. Собственная СУБД
Файлы: `lib/simpledb.py`, бенчмарк `investigations/benchmarks/simpleDbIndexBench.py`
- 7.a Хранение в двоичном виде:
  - Файлы: *.data (строки), *.rows (смещения 8байт), *.lens (длины), *.mask (активность), *.schema.json, *.col.index (индексы INT).
  - Формат строки: [2 байта count][catalog offsets][payload]; INT = 8 байт LE; VARCHAR(n) = 4 байта длина + UTF-8.
- 7.b SQL поддержка:
  - CREATE TABLE (INT, VARCHAR(n))
  - INSERT INTO ... VALUES (...)
  - SELECT * / SELECT col1,col2 ... [WHERE col = value]
  - DELETE * FROM table (полное удаление) и DELETE FROM table WHERE col = value
- 7.c Индексы на INT: файл `.col.index` хранит map value -> [rowId], используется в select/delete для ускорения.
- 7.d/e/f Графики (SELECT/DELETE string/int; INSERT; сравнение с и без индекса) в `simpleDbIndexBench.py` + CSV (best/avg).

#