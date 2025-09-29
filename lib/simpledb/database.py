import os
import json
from .constants import DATA_DIR
from .schema import Schema
from .paths import TableFiles
from .engine.table_engine import TableEngine
from .indexes import IntIndex
from .parser.sql_parser import parseSql

class SimpleDatabase:
    def __init__(self, dataDir=DATA_DIR):
        self.dataDir = os.path.abspath(dataDir)
        if not os.path.isdir(self.dataDir):
            os.makedirs(self.dataDir, exist_ok=True)
        self.tables = {}

    def _loadSchema(self, tableName):
        files = TableFiles(self.dataDir, tableName)
        p = files.schemaPath()
        if not os.path.isfile(p):
            return None
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return Schema.fromDict(data)

    def _getEngine(self, tableName):
        eng = self.tables.get(tableName)
        if eng is None:
            sch = self._loadSchema(tableName)
            if sch is None:
                return None
            files = TableFiles(self.dataDir, tableName)
            eng = TableEngine(files, sch)
            eng.open()
            self.tables[tableName] = eng
        return eng

    def createTable(self, tableName, columns):
        files = TableFiles(self.dataDir, tableName)
        sch = Schema(tableName, columns)
        eng = TableEngine(files, sch)
        eng.create()
        self.tables[tableName] = eng
        return True

    def createIndex(self, tableName, colName):
        eng = self._getEngine(tableName)
        if eng is None:
            return False
        foundIndex = -1
        for columnIndex in range(len(eng.schema.columns)):
            c = eng.schema.columns[columnIndex]
            if c['name'] == colName and c['type'] == 'INT':
                c['index'] = True
                foundIndex = columnIndex
                break
        if foundIndex < 0:
            return False
        with open(eng.files.schemaPath(), 'w', encoding='utf-8') as f:
            json.dump(eng.schema.toDict(), f, ensure_ascii=False)
        idx = IntIndex(eng.files.indexPath(colName))
        total = eng.rowCount()
        for rid in range(total):
            if eng._isRowActive(rid):
                data = eng._readRowData(rid)
                off = int.from_bytes(data[2 + foundIndex*4: 2 + foundIndex*4 + 4], 'little')
                valBytes = data[off:off+8]
                if len(valBytes) == 8:
                    v = int.from_bytes(valBytes, 'little')
                    idx.add(v, rid)
        idx.save()
        eng.indexes[colName] = idx
        return True

    def dropDataDir(self):
        if os.path.isdir(self.dataDir):
            names = os.listdir(self.dataDir)
            for fileName in names:
                path = os.path.join(self.dataDir, fileName)
                try:
                    os.remove(path)
                except Exception:
                    pass

    def closeAll(self):
        for name in list(self.tables.keys()):
            try:
                self.tables[name].close()
            except Exception:
                pass

    def execute(self, sql):
        cmd = parseSql(sql)
        t = cmd.get('type')
        d = cmd.get('data', {})
        if t == 'create_table':
            return [] if self.createTable(d['table'], d['columns']) else []
        if t == 'create_index':
            self.createIndex(d['table'], d['column'])
            return []
        if t == 'insert':
            eng = self._getEngine(d['table'])
            if eng is None:
                return []
            row = {}
            cols = d['columns']
            vals = d['values']
            for j in range(len(cols)):
                row[cols[j]] = vals[j]
            eng.insertRow(row)
            return []
        if t == 'select':
            eng = self._getEngine(d['table'])
            if eng is None:
                return []
            return eng.select(d['columns'], d['where'])
        if t == 'delete_all':
            eng = self._getEngine(d['table'])
            if eng is not None:
                eng.deleteAll()
            return []
        if t == 'delete_where':
            eng = self._getEngine(d['table'])
            if eng is not None:
                w = d.get('where')
                if w:
                    eng.deleteWhere(w[0], w[1])
            return []
        return []
