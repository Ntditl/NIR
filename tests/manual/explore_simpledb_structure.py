import os
import struct
from lib.simpledb.database import SimpleDatabase

def printBytes(data, label=""):
    if label:
        print(f"\n{label}")
    print("Hex:", " ".join(f"{b:02x}" for b in data))
    print("Dec:", " ".join(f"{b:3d}" for b in data))
    printable = ""
    for b in data:
        if 32 <= b <= 126:
            printable += chr(b)
        else:
            printable += "."
    print("Str:", printable)

def exploreSimpleDB():
    dataDir = "C:\\Users\\Ntditl\\PycharmProjects\\scienceResearche\\lib\\simpledb_data_explore"

    if os.path.exists(dataDir):
        import shutil
        shutil.rmtree(dataDir)

    db = SimpleDatabase(dataDir)

    print("="*80)
    print("СОЗДАЁМ ТАБЛИЦУ: users")
    print("="*80)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(20))")

    print("\n" + "="*80)
    print("СОЗДАЁМ ИНДЕКС НА users.id")
    print("="*80)
    db.execute("CREATE INDEX ON users(id)")
    print("Индекс создан на колонке id")

    print("\n" + "="*80)
    print("ВСТАВЛЯЕМ ДАННЫЕ В users")
    print("="*80)
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    print("Вставили: id=1, name='Bob'")

    db.execute("INSERT INTO users (id, name) VALUES (2, 'Alice')")
    print("Вставили: id=2, name='Alice'")

    print("\n" + "="*80)
    print("СОЗДАЁМ ТАБЛИЦУ: products")
    print("="*80)
    db.execute("CREATE TABLE products (product_id INT, title VARCHAR(30), price INT)")

    print("\n" + "="*80)
    print("ВСТАВЛЯЕМ ДАННЫЕ В products")
    print("="*80)
    db.execute("INSERT INTO products (product_id, title, price) VALUES (10, 'Laptop', 1000)")
    print("Вставили: product_id=10, title='Laptop', price=1000")

    db.execute("INSERT INTO products (product_id, title, price) VALUES (20, 'Mouse', 25)")
    print("Вставили: product_id=20, title='Mouse', price=25")

    db.closeAll()

    print("\n\n" + "="*80)
    print("АНАЛИЗ СОЗДАННЫХ ФАЙЛОВ")
    print("="*80)

    files = os.listdir(dataDir)
    files.sort()

    print(f"\nВсего файлов создано: {len(files)}")
    for f in files:
        print(f"  - {f}")

    print("\n\n" + "#"*80)
    print("# ТАБЛИЦА: users")
    print("#"*80)

    print("\n" + "="*80)
    print("ФАЙЛ 1: users.schema.json")
    print("="*80)
    schemaPath = os.path.join(dataDir, "users.schema.json")
    with open(schemaPath, 'r', encoding='utf-8') as f:
        content = f.read()
    print(content)

    print("\n" + "="*80)
    print("ФАЙЛ 2: users.data (бинарный файл с данными)")
    print("="*80)
    dataPath = os.path.join(dataDir, "users.data")
    fileSize = os.path.getsize(dataPath)
    print(f"Размер файла: {fileSize} байт")

    with open(dataPath, 'rb') as f:
        allData = f.read()

    print("\n--- ВЕСЬ ФАЙЛ users.data ---")
    printBytes(allData, "Все байты файла:")

    print("\n" + "="*80)
    print("ФАЙЛ 3: users.id.index (индексный файл)")
    print("="*80)
    indexPath = os.path.join(dataDir, "users.id.index")
    if os.path.exists(indexPath):
        fileSize = os.path.getsize(indexPath)
        print(f"Размер файла: {fileSize} байт")

        with open(indexPath, 'rb') as f:
            indexData = f.read()

        print("\n--- ВЕСЬ ФАЙЛ users.id.index ---")
        printBytes(indexData, "Все байты индекса:")

        print("\nИНДЕКС ХРАНИТ СЛОВАРЬ: {значение_колонки: [список_rowId]}")
        print("Пример: {1: [0], 2: [1]} означает:")
        print("  - id=1 находится в строке 0")
        print("  - id=2 находится в строке 1")
    else:
        print("Индексный файл не найден")

    print("\n\n" + "-"*80)
    print("РАЗБОР СТРОКИ 0: id=1, name='Bob'")
    print("-"*80)

    pos = 0

    print("\n[HEADER строки 0] (7 байт)")
    header = allData[pos:pos+7]
    printBytes(header)

    active = header[0]
    total_len = struct.unpack('<I', header[1:5])[0]
    count = struct.unpack('<H', header[5:7])[0]

    print(f"  ACTIVE: {active} ({'активна' if active == 1 else 'удалена'})")
    print(f"  TOTAL_LEN: {total_len} байт (общая длина строки)")
    print(f"  COUNT: {count} (количество колонок)")
    pos += 7

    print("\n[DIRECTORY строки 0] (8 байт = 2 колонки * 4 байта)")
    dirSize = count * 4
    directory = allData[pos:pos+dirSize]
    printBytes(directory)

    offset0 = struct.unpack('<I', directory[0:4])[0]
    offset1 = struct.unpack('<I', directory[4:8])[0]

    print(f"  offset колонки 0 (id): {offset0}")
    print(f"  offset колонки 1 (name): {offset1}")
    pos += dirSize

    print("\n[PAYLOAD строки 0]")
    payloadStart = pos
    payloadEnd = total_len
    payload = allData[payloadStart:payloadEnd]
    printBytes(payload, "Весь payload:")

    print("\n  Колонка 0 (id=1, INT):")
    col0Data = allData[offset0:offset0+4]
    printBytes(col0Data, "    Байты:")
    id_value = struct.unpack('<I', col0Data)[0]
    print(f"    Значение: {id_value}")

    print("\n  Колонка 1 (name='Bob', VARCHAR):")
    col1Start = offset1
    name_len = allData[col1Start]
    name_bytes = allData[col1Start+1:col1Start+1+name_len]
    printBytes(allData[col1Start:col1Start+1+name_len], "    Байты:")
    print(f"    Длина строки: {name_len}")
    print(f"    Значение: '{name_bytes.decode('utf-8')}'")

    print("\n\n" + "-"*80)
    print("РАЗБОР СТРОКИ 1: id=2, name='Alice'")
    print("-"*80)

    pos = total_len

    print("\n[HEADER строки 1] (7 байт)")
    header = allData[pos:pos+7]
    printBytes(header)

    active = header[0]
    total_len_row1 = struct.unpack('<I', header[1:5])[0]
    count = struct.unpack('<H', header[5:7])[0]

    print(f"  ACTIVE: {active}")
    print(f"  TOTAL_LEN: {total_len_row1} байт")
    print(f"  COUNT: {count}")
    pos += 7

    print("\n[DIRECTORY строки 1] (8 байт)")
    directory = allData[pos:pos+8]
    printBytes(directory)

    offset0 = struct.unpack('<I', directory[0:4])[0]
    offset1 = struct.unpack('<I', directory[4:8])[0]

    print(f"  offset колонки 0 (id): {offset0}")
    print(f"  offset колонки 1 (name): {offset1}")
    pos += 8

    print("\n[PAYLOAD строки 1]")
    payloadStart = pos
    row1Start = total_len
    payloadEnd = row1Start + total_len_row1
    payload = allData[payloadStart:payloadEnd]
    printBytes(payload, "Весь payload:")

    print("\n  Колонка 0 (id=2, INT):")
    col0Data = allData[offset0:offset0+4]
    printBytes(col0Data, "    Байты:")
    id_value = struct.unpack('<I', col0Data)[0]
    print(f"    Значение: {id_value}")

    print("\n  Колонка 1 (name='Alice', VARCHAR):")
    col1Start = offset1
    name_len = allData[col1Start]
    name_bytes = allData[col1Start+1:col1Start+1+name_len]
    printBytes(allData[col1Start:col1Start+1+name_len], "    Байты:")
    print(f"    Длина строки: {name_len}")
    print(f"    Значение: '{name_bytes.decode('utf-8')}'")

    print("\n\n" + "#"*80)
    print("# ТАБЛИЦА: products")
    print("#"*80)

    print("\n" + "="*80)
    print("ФАЙЛ 4: products.schema.json")
    print("="*80)
    schemaPath = os.path.join(dataDir, "products.schema.json")
    with open(schemaPath, 'r', encoding='utf-8') as f:
        content = f.read()
    print(content)

    print("\n" + "="*80)
    print("ФАЙЛ 5: products.data (бинарный файл с данными)")
    print("="*80)
    dataPath = os.path.join(dataDir, "products.data")
    fileSize = os.path.getsize(dataPath)
    print(f"Размер файла: {fileSize} байт")

    with open(dataPath, 'rb') as f:
        allData = f.read()

    print("\n--- ВЕСЬ ФАЙЛ products.data ---")
    printBytes(allData, "Все байты файла:")

    print("\n\n" + "-"*80)
    print("РАЗБОР СТРОКИ 0: product_id=10, title='Laptop', price=1000")
    print("-"*80)

    pos = 0

    print("\n[HEADER строки 0] (7 байт)")
    header = allData[pos:pos+7]
    printBytes(header)

    active = header[0]
    total_len = struct.unpack('<I', header[1:5])[0]
    count = struct.unpack('<H', header[5:7])[0]

    print(f"  ACTIVE: {active}")
    print(f"  TOTAL_LEN: {total_len} байт")
    print(f"  COUNT: {count} (3 колонки)")
    pos += 7

    print("\n[DIRECTORY строки 0] (12 байт = 3 колонки * 4 байта)")
    dirSize = count * 4
    directory = allData[pos:pos+dirSize]
    printBytes(directory)

    offset0 = struct.unpack('<I', directory[0:4])[0]
    offset1 = struct.unpack('<I', directory[4:8])[0]
    offset2 = struct.unpack('<I', directory[8:12])[0]

    print(f"  offset колонки 0 (product_id): {offset0}")
    print(f"  offset колонки 1 (title): {offset1}")
    print(f"  offset колонки 2 (price): {offset2}")
    pos += dirSize

    print("\n[PAYLOAD строки 0]")
    payloadStart = pos
    payloadEnd = total_len
    payload = allData[payloadStart:payloadEnd]
    printBytes(payload, "Весь payload:")

    print("\n  Колонка 0 (product_id=10, INT):")
    col0Data = allData[offset0:offset0+4]
    printBytes(col0Data, "    Байты:")
    product_id = struct.unpack('<I', col0Data)[0]
    print(f"    Значение: {product_id}")

    print("\n  Колонка 1 (title='Laptop', VARCHAR):")
    col1Start = offset1
    title_len = allData[col1Start]
    title_bytes = allData[col1Start+1:col1Start+1+title_len]
    printBytes(allData[col1Start:col1Start+1+title_len], "    Байты:")
    print(f"    Длина строки: {title_len}")
    print(f"    Значение: '{title_bytes.decode('utf-8')}'")

    print("\n  Колонка 2 (price=1000, INT):")
    col2Data = allData[offset2:offset2+4]
    printBytes(col2Data, "    Байты:")
    price = struct.unpack('<I', col2Data)[0]
    print(f"    Значение: {price}")

    print("\n\n" + "-"*80)
    print("РАЗБОР СТРОКИ 1: product_id=20, title='Mouse', price=25")
    print("-"*80)

    pos = total_len

    print("\n[HEADER строки 1] (7 байт)")
    header = allData[pos:pos+7]
    printBytes(header)

    active = header[0]
    total_len_row1 = struct.unpack('<I', header[1:5])[0]
    count = struct.unpack('<H', header[5:7])[0]

    print(f"  ACTIVE: {active}")
    print(f"  TOTAL_LEN: {total_len_row1} байт")
    print(f"  COUNT: {count}")
    pos += 7

    print("\n[DIRECTORY строки 1] (12 байт)")
    directory = allData[pos:pos+12]
    printBytes(directory)

    offset0 = struct.unpack('<I', directory[0:4])[0]
    offset1 = struct.unpack('<I', directory[4:8])[0]
    offset2 = struct.unpack('<I', directory[8:12])[0]

    print(f"  offset колонки 0 (product_id): {offset0}")
    print(f"  offset колонки 1 (title): {offset1}")
    print(f"  offset колонки 2 (price): {offset2}")

    print("\n  Колонка 0 (product_id=20, INT):")
    col0Data = allData[offset0:offset0+4]
    printBytes(col0Data, "    Байты:")
    product_id = struct.unpack('<I', col0Data)[0]
    print(f"    Значение: {product_id}")

    print("\n  Колонка 1 (title='Mouse', VARCHAR):")
    col1Start = offset1
    title_len = allData[col1Start]
    title_bytes = allData[col1Start+1:col1Start+1+title_len]
    printBytes(allData[col1Start:col1Start+1+title_len], "    Байты:")
    print(f"    Длина строки: {title_len}")
    print(f"    Значение: '{title_bytes.decode('utf-8')}'")

    print("\n  Колонка 2 (price=25, INT):")
    col2Data = allData[offset2:offset2+4]
    printBytes(col2Data, "    Байты:")
    price = struct.unpack('<I', col2Data)[0]
    print(f"    Значение: {price}")

    print("\n\n" + "="*80)
    print("ИТОГОВАЯ СТРУКТУРА ФАЙЛОВ")
    print("="*80)
    print("""
ТАБЛИЦА users:
  - users.schema.json    (схема таблицы в JSON)
  - users.data           (2 строки данных в бинарном виде)

ТАБЛИЦА products:
  - products.schema.json (схема таблицы в JSON)
  - products.data        (2 строки данных в бинарном виде)

СТРУКТУРА СТРОКИ В .data ФАЙЛЕ:
┌─────────────────────────────────────────────────────────────┐
│                      ОДНА СТРОКА                             │
├──────────────┬──────────────┬──────────────┬────────────────┤
│   HEADER     │  DIRECTORY   │   PAYLOAD    │                │
│   (7 байт)   │  (N*4 байт)  │  (M байт)    │                │
└──────────────┴──────────────┴──────────────┴────────────────┘

HEADER (7 байт):
  - ACTIVE (1 байт): 0x01=активна, 0x00=удалена
  - TOTAL_LEN (4 байта): общая длина строки
  - COUNT (2 байта): количество колонок

DIRECTORY (N*4 байт):
  - Для каждой колонки: offset (4 байта) до её данных в PAYLOAD

PAYLOAD (M байт):
  - INT: 4 байта (little-endian)
  - VARCHAR: 1 байт (длина) + N байт (строка UTF-8)
    """)

if __name__ == '__main__':
    exploreSimpleDB()
