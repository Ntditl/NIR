import os

class TableFiles:
    def __init__(self, baseDir, tableName):
        self.baseDir = baseDir
        self.tableName = tableName
    def schemaPath(self):
        return os.path.join(self.baseDir, self.tableName + '.schema.json')
    def dataPath(self):
        return os.path.join(self.baseDir, self.tableName + '.data')
    def rowsPath(self):
        return os.path.join(self.baseDir, self.tableName + '.rows')
    def maskPath(self):
        return os.path.join(self.baseDir, self.tableName + '.mask')
    def lensPath(self):
        return os.path.join(self.baseDir, self.tableName + '.lens')
    def indexPath(self, colName):
        return os.path.join(self.baseDir, self.tableName + '.' + colName + '.index')

