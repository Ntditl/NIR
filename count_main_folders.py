import os

def countLinesInMainFolders():
    projectPath = os.path.dirname(os.path.abspath(__file__))

    mainFolders = ['lib', 'investigations', 'tests']
    totalStats = {'total_lines': 0, 'code_lines': 0, 'files': 0}
    folderStats = {}

    for folderName in mainFolders:
        folderPath = os.path.join(projectPath, folderName)
        if not os.path.exists(folderPath):
            print(f"Папка {folderName} не найдена")
            continue

        folderStats[folderName] = {'total_lines': 0, 'code_lines': 0, 'files': 0}

        for root, dirs, files in os.walk(folderPath):
            if '__pycache__' in root:
                continue

            for file in files:
                if file.endswith('.py'):
                    filePath = os.path.join(root, file)
                    try:
                        with open(filePath, 'r', encoding='utf-8', errors='ignore') as f:
                            lines = f.readlines()
                            lineCount = len(lines)

                            codeLines = 0
                            for line in lines:
                                stripped = line.strip()
                                if stripped and not stripped.startswith('#'):
                                    codeLines += 1

                            folderStats[folderName]['total_lines'] += lineCount
                            folderStats[folderName]['code_lines'] += codeLines
                            folderStats[folderName]['files'] += 1

                            totalStats['total_lines'] += lineCount
                            totalStats['code_lines'] += codeLines
                            totalStats['files'] += 1

                    except Exception as e:
                        print(f"Ошибка чтения файла {filePath}: {e}")

    print("="*50)
    print("СТАТИСТИКА ПО ОСНОВНЫМ ПАПКАМ ПРОЕКТА")
    print("="*50)

    for folderName in mainFolders:
        if folderName in folderStats:
            stats = folderStats[folderName]
            print(f"{folderName}:")
            print(f"  Файлов: {stats['files']}")
            print(f"  Всего строк: {stats['total_lines']}")
            print(f"  Строк кода: {stats['code_lines']}")
            print()

    print("="*50)
    print("ИТОГО (lib + investigations + tests):")
    print(f"  Файлов: {totalStats['files']}")
    print(f"  Всего строк: {totalStats['total_lines']}")
    print(f"  Строк кода: {totalStats['code_lines']}")
    print("="*50)

if __name__ == "__main__":
    countLinesInMainFolders()
