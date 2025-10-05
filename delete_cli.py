import os

def deleteCliFile():
    projectPath = os.path.dirname(os.path.abspath(__file__))
    cliPath = os.path.join(projectPath, 'lib', 'simpledb', 'cli.py')

    if os.path.exists(cliPath):
        try:
            os.remove(cliPath)
            print("Удален файл: cli.py")
            return True
        except Exception as e:
            print(f"Ошибка удаления cli.py: {e}")
            return False
    else:
        print("Файл cli.py не найден")
        return False

if __name__ == "__main__":
    deleteCliFile()
