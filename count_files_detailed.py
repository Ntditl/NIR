import os

def countLinesByFiles():
    projectPath = os.path.dirname(os.path.abspath(__file__))

    mainFolders = ['lib', 'investigations', 'tests']

    for folderName in mainFolders:
        folderPath = os.path.join(projectPath, folderName)
        if not os.path.exists(folderPath):
            print(f"Папка {folderName} не найдена")
            continue

        print("="*60)
        print(f"ДЕТАЛИЗАЦИЯ ПО ФАЙЛАМ В ПАПКЕ: {folderName.upper()}")
        print("="*60)

        fileStats = []
        totalLines = 0
        totalCodeLines = 0
        totalFiles = 0

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

                            relativePath = os.path.relpath(filePath, folderPath)
                            fileStats.append({
                                'path': relativePath,
                                'total_lines': lineCount,
                                'code_lines': codeLines
                            })

                            totalLines += lineCount
                            totalCodeLines += codeLines
                            totalFiles += 1

                    except Exception as e:
                        print(f"Ошибка чтения файла {filePath}: {e}")

        # Сортируем файлы по количеству строк кода (по убыванию)
        fileStats.sort(key=lambda x: x['code_lines'], reverse=True)

        for fileInfo in fileStats:
            print(f"{fileInfo['code_lines']:3d} строк кода ({fileInfo['total_lines']:3d} всего) - {fileInfo['path']}")

        print(f"\nИТОГО по папке {folderName}:")
        print(f"  Файлов: {totalFiles}")
        print(f"  Всего строк: {totalLines}")
        print(f"  Строк кода: {totalCodeLines}")
        print()

if __name__ == "__main__":
    countLinesByFiles()
