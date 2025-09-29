class Schema:
    def __init__(self, tableName, columns):
        self.tableName = tableName
        self.columns = columns
    def toDict(self):
        return {"table": self.tableName, "columns": self.columns}
    @staticmethod
    def fromDict(d):
        return Schema(d["table"], d["columns"])

