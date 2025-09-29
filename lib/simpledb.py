import os
import json
import struct

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'simpledb_data')
MAX_UINT64 = (1 << 64) - 1

class Schema:
    def __init__(self, tableName, columns):
        self.tableName = tableName
        self.columns = columns
    def toDict(self):
        return {"table": self.tableName, "columns": self.columns}
    @staticmethod
    def fromDict(d):
        return Schema(d["table"], d["columns"])

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

class IntIndex:
    def __init__(self, path):
        self.path = path
        self.map = {}
        if os.path.isfile(self.path):
            with open(self.path, 'rb') as f:
                data = f.read(4)
                if len(data) == 4:
                    count = struct.unpack('<I', data)[0]
                    i = 0
                    while i < count:
                        keyBytes = f.read(8)
                        if len(keyBytes) < 8:
                            break
                        key = struct.unpack('<Q', keyBytes)[0]
                        lnBytes = f.read(4)
                        if len(lnBytes) < 4:
                            break
                        ln = struct.unpack('<I', lnBytes)[0]
                        ids = []
                        j = 0
                        while j < ln:
                            idBytes = f.read(8)
                            if len(idBytes) < 8:
                                break
                            rid = struct.unpack('<Q', idBytes)[0]
                            ids.append(rid)
                            j = j + 1
                        self.map[key] = ids
                        i = i + 1
    def add(self, key, rowId):
        arr = self.map.get(key)
        if arr is None:
            arr = []
            self.map[key] = arr
        arr.append(rowId)
    def removeRowIds(self, key, rowIdsSet):
        arr = self.map.get(key)
        if arr is None:
            return
        newArr = []
        i = 0
        while i < len(arr):
            if arr[i] not in rowIdsSet:
                newArr.append(arr[i])
            i = i + 1
        self.map[key] = newArr
    def getRowIds(self, key):
        arr = self.map.get(key)
        if arr is None:
            return []
        return list(arr)
    def save(self):
        items = list(self.map.items())
        with open(self.path, 'wb') as f:
            f.write(struct.pack('<I', len(items)))
            i = 0
            while i < len(items):
                key = items[i][0]
                ids = items[i][1]
                f.write(struct.pack('<Q', int(key)))
                f.write(struct.pack('<I', len(ids)))
                j = 0
                while j < len(ids):
                    f.write(struct.pack('<Q', int(ids[j])))
                    j = j + 1
                i = i + 1

class TableEngine:
    def __init__(self, files: TableFiles, schema: Schema):
        self.files = files
        self.schema = schema
        self.indexes = {}
        if not os.path.isdir(files.baseDir):
            os.makedirs(files.baseDir, exist_ok=True)
    def create(self):
        with open(self.files.schemaPath(), 'w', encoding='utf-8') as f:
            json.dump(self.schema.toDict(), f, ensure_ascii=False)
        with open(self.files.dataPath(), 'wb') as f:
            pass
        with open(self.files.rowsPath(), 'wb') as f:
            pass
        with open(self.files.maskPath(), 'wb') as f:
            pass
        with open(self.files.lensPath(), 'wb') as f:
            pass
    def open(self):
        for col in self.schema.columns:
            if col.get('index') == True and col['type'] == 'INT':
                self.indexes[col['name']] = IntIndex(self.files.indexPath(col['name']))
            else:
                p = self.files.indexPath(col['name'])
                if os.path.isfile(p):
                    self.indexes[col['name']] = IntIndex(p)
    def close(self):
        for name in self.indexes:
            self.indexes[name].save()
    def _appendMask(self, active):
        with open(self.files.maskPath(), 'ab') as f:
            b = 1 if active else 0
            f.write(struct.pack('<B', b))
    def _appendRowOffset(self, offset):
        with open(self.files.rowsPath(), 'ab') as f:
            f.write(struct.pack('<Q', int(offset)))
    def _appendRowLength(self, length):
        with open(self.files.lensPath(), 'ab') as f:
            f.write(struct.pack('<I', int(length)))
    def _getRowOffset(self, rowId):
        with open(self.files.rowsPath(), 'rb') as f:
            f.seek(rowId * 8)
            data = f.read(8)
            if len(data) < 8:
                return None
            return struct.unpack('<Q', data)[0]
    def _getRowLength(self, rowId):
        if not os.path.isfile(self.files.lensPath()):
            return None
        with open(self.files.lensPath(), 'rb') as f:
            f.seek(rowId * 4)
            data = f.read(4)
            if len(data) < 4:
                return None
            return struct.unpack('<I', data)[0]
    def _isRowActive(self, rowId):
        with open(self.files.maskPath(), 'rb') as f:
            f.seek(rowId)
            data = f.read(1)
            if len(data) < 1:
                return False
            return struct.unpack('<B', data)[0] == 1
    def _setRowInactive(self, rowId):
        with open(self.files.maskPath(), 'r+b') as f:
            f.seek(rowId)
            f.write(struct.pack('<B', 0))
    def _packValue(self, colType, colMax, value):
        if colType == 'INT':
            n = int(value)
            if n < 0:
                raise ValueError('INT negative')
            if n > MAX_UINT64:
                raise ValueError('INT overflow')
            return struct.pack('<Q', n)
        s = str(value)
        b = s.encode('utf-8')
        if colMax is not None:
            if len(b) > int(colMax):
                b = b[:int(colMax)]
        ln = len(b)
        return struct.pack('<I', ln) + b
    def _unpackValue(self, colType, data, offset):
        if colType == 'INT':
            v = struct.unpack('<Q', data[offset:offset+8])[0]
            return v, offset + 8
        ln = struct.unpack('<I', data[offset:offset+4])[0]
        start = offset + 4
        end = start + ln
        s = data[start:end].decode('utf-8', errors='ignore')
        return s, end
    def insertRow(self, valuesDict):
        cols = self.schema.columns
        count = len(cols)
        dirSize = count * 4
        payload = b''
        offsets = []
        i = 0
        while i < count:
            offsets.append(0)
            i = i + 1
        i = 0
        while i < count:
            col = cols[i]
            name = col['name']
            ctype = col['type']
            cmax = col.get('max')
            val = valuesDict.get(name)
            packed = self._packValue(ctype, cmax, val)
            offsets[i] = 2 + dirSize + len(payload)
            payload = payload + packed
            i = i + 1
        header = struct.pack('<H', count)
        dirBytes = b''
        i = 0
        while i < len(offsets):
            dirBytes = dirBytes + struct.pack('<I', offsets[i])
            i = i + 1
        rowBytes = header + dirBytes + payload
        with open(self.files.dataPath(), 'ab') as f:
            startOffset = f.tell()
            f.write(rowBytes)
        self._appendRowOffset(startOffset)
        self._appendRowLength(len(rowBytes))
        self._appendMask(True)
        rowId = self.rowCount() - 1
        i = 0
        while i < count:
            col = cols[i]
            if col['type'] == 'INT' and self.indexes.get(col['name']) is not None:
                v = int(valuesDict.get(col['name']))
                self.indexes[col['name']].add(v, rowId)
            i = i + 1
        return rowId
    def rowCount(self):
        if not os.path.isfile(self.files.rowsPath()):
            return 0
        sz = os.path.getsize(self.files.rowsPath())
        return sz // 8
    def _readRowData(self, rowId):
        off = self._getRowOffset(rowId)
        if off is None:
            return None
        length = self._getRowLength(rowId)
        if length is not None:
            with open(self.files.dataPath(), 'rb') as f:
                f.seek(off)
                data = f.read(length)
                if len(data) < length:
                    return None
                return data
        with open(self.files.dataPath(), 'rb') as f:
            f.seek(off)
            head = f.read(2)
            if len(head) < 2:
                return None
            count = struct.unpack('<H', head)[0]
            dirSize = count * 4
            dirData = f.read(dirSize)
            total = head + dirData
            buf = f.read()
            total = total + buf
            return total
    def readRow(self, rowId):
        if not self._isRowActive(rowId):
            return None
        data = self._readRowData(rowId)
        if data is None:
            return None
        count = struct.unpack('<H', data[:2])[0]
        cols = self.schema.columns
        res = []
        i = 0
        while i < count:
            off = struct.unpack('<I', data[2 + i*4: 2 + i*4 + 4])[0]
            dtype = cols[i]['type']
            v, _ = self._unpackValue(dtype, data, off)
            res.append(v)
            i = i + 1
        return res
    def readColumns(self, rowId, colIndexes):
        if not self._isRowActive(rowId):
            return None
        data = self._readRowData(rowId)
        if data is None:
            return None
        cols = self.schema.columns
        res = []
        j = 0
        while j < len(colIndexes):
            idx = colIndexes[j]
            off = struct.unpack('<I', data[2 + idx*4: 2 + idx*4 + 4])[0]
            dtype = cols[idx]['type']
            v, _ = self._unpackValue(dtype, data, off)
            res.append(v)
            j = j + 1
        return res
    def deleteAll(self):
        with open(self.files.dataPath(), 'wb') as f:
            pass
        with open(self.files.rowsPath(), 'wb') as f:
            pass
        with open(self.files.maskPath(), 'wb') as f:
            pass
        with open(self.files.lensPath(), 'wb') as f:
            pass
        for name in list(self.indexes.keys()):
            self.indexes[name].map = {}
            self.indexes[name].save()
    def deleteWhere(self, colName, value):
        colIdx = -1
        i = 0
        while i < len(self.schema.columns):
            if self.schema.columns[i]['name'] == colName:
                colIdx = i
                break
            i = i + 1
        if colIdx < 0:
            return 0
        deleted = 0
        col = self.schema.columns[colIdx]
        if col['type'] == 'INT' and self.indexes.get(colName) is not None:
            rowIds = self.indexes[colName].getRowIds(int(value))
            s = set(rowIds)
            j = 0
            while j < len(rowIds):
                rid = rowIds[j]
                if self._isRowActive(rid):
                    self._setRowInactive(rid)
                    deleted = deleted + 1
                j = j + 1
            self.indexes[colName].removeRowIds(int(value), s)
            return deleted
        total = self.rowCount()
        rid = 0
        while rid < total:
            if self._isRowActive(rid):
                data = self._readRowData(rid)
                if data is not None:
                    off = struct.unpack('<I', data[2 + colIdx*4: 2 + colIdx*4 + 4])[0]
                    dtype = col['type']
                    v, _ = self._unpackValue(dtype, data, off)
                    if dtype == 'INT':
                        if int(v) == int(value):
                            self._setRowInactive(rid)
                            deleted = deleted + 1
                    else:
                        if str(v) == str(value):
                            self._setRowInactive(rid)
                            deleted = deleted + 1
            rid = rid + 1
        return deleted
    def select(self, colNames, where):
        selIdx = []
        if colNames == ['*']:
            i = 0
            while i < len(self.schema.columns):
                selIdx.append(i)
                i = i + 1
        else:
            i = 0
            while i < len(colNames):
                name = colNames[i]
                j = 0
                while j < len(self.schema.columns):
                    if self.schema.columns[j]['name'] == name:
                        selIdx.append(j)
                        break
                    j = j + 1
                i = i + 1
        rowIds = None
        if where is not None:
            wcol = where[0]
            wval = where[1]
            idx = -1
            i = 0
            while i < len(self.schema.columns):
                if self.schema.columns[i]['name'] == wcol:
                    idx = i
                    break
                i = i + 1
            if idx >= 0:
                col = self.schema.columns[idx]
                if col['type'] == 'INT' and self.indexes.get(wcol) is not None:
                    rowIds = self.indexes[wcol].getRowIds(int(wval))
        res = []
        if rowIds is not None:
            k = 0
            while k < len(rowIds):
                rid = rowIds[k]
                if self._isRowActive(rid):
                    vals = self.readColumns(rid, selIdx)
                    if vals is not None:
                        res.append(tuple(vals))
                k = k + 1
            return res
        total = self.rowCount()
        rid = 0
        while rid < total:
            if self._isRowActive(rid):
                if where is None:
                    vals = self.readColumns(rid, selIdx)
                    if vals is not None:
                        res.append(tuple(vals))
                else:
                    wcol = where[0]
                    wval = where[1]
                    idx = -1
                    i = 0
                    while i < len(self.schema.columns):
                        if self.schema.columns[i]['name'] == wcol:
                            idx = i
                            break
                        i = i + 1
                    if idx >= 0:
                        data = self._readRowData(rid)
                        off = struct.unpack('<I', data[2 + idx*4: 2 + idx*4 + 4])[0]
                        dtype = self.schema.columns[idx]['type']
                        v, _ = self._unpackValue(dtype, data, off)
                        ok = False
                        if dtype == 'INT':
                            if int(v) == int(wval):
                                ok = True
                        else:
                            if str(v) == str(wval):
                                ok = True
                        if ok:
                            vals = self.readColumns(rid, selIdx)
                            if vals is not None:
                                res.append(tuple(vals))
            rid = rid + 1
        return res

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
            d = json.load(f)
            return Schema.fromDict(d)
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
        i = 0
        found = False
        while i < len(eng.schema.columns):
            c = eng.schema.columns[i]
            if c['name'] == colName and c['type'] == 'INT':
                found = True
                c['index'] = True
                break
            i = i + 1
        if not found:
            return False
        with open(eng.files.schemaPath(), 'w', encoding='utf-8') as f:
            json.dump(eng.schema.toDict(), f, ensure_ascii=False)
        idx = IntIndex(eng.files.indexPath(colName))
        total = eng.rowCount()
        rid = 0
        while rid < total:
            if eng._isRowActive(rid):
                data = eng._readRowData(rid)
                off = struct.unpack('<I', data[2 + i*4: 2 + i*4 + 4])[0]
                v, _ = eng._unpackValue('INT', data, off)
                idx.add(int(v), rid)
            rid = rid + 1
        idx.save()
        eng.indexes[colName] = idx
        return True
    def dropDataDir(self):
        if os.path.isdir(self.dataDir):
            names = os.listdir(self.dataDir)
            k = 0
            while k < len(names):
                path = os.path.join(self.dataDir, names[k])
                try:
                    os.remove(path)
                except Exception:
                    pass
                k = k + 1
    def execute(self, sql):
        s = sql.strip()
        low = s.lower()
        if low.startswith('create table'):
            start = s.find('(')
            end = s.rfind(')')
            head = s[:start].strip()
            parts = head.split()
            tableName = parts[-1]
            colsDef = s[start+1:end]
            parts = colsDef.split(',')
            cols = []
            i = 0
            while i < len(parts):
                p = parts[i].strip()
                ps = p.split()
                name = ps[0]
                t = ps[1].upper()
                if t.startswith('VARCHAR'):
                    l = t[t.find('(')+1:t.find(')')]
                    cols.append({"name": name, "type": "VARCHAR", "max": int(l)})
                else:
                    cols.append({"name": name, "type": "INT"})
                i = i + 1
            self.createTable(tableName, cols)
            return []
        if low.startswith('create index'):
            p1 = low.find('on')
            rest = s[p1+2:].strip()
            name = rest.split('(')[0].strip()
            col = rest[rest.find('(')+1:rest.find(')')].strip()
            self.createIndex(name, col)
            return []
        if low.startswith('insert into'):
            after = s[len('insert into'):].strip()
            tname = after.split()[0]
            colsPart = after[after.find('(')+1:after.find(')')]
            cols = []
            for seg in colsPart.split(','):
                cols.append(seg.strip())
            posVals = low.find('values')
            if posVals < 0:
                return []
            valsPart = s[posVals+6:].strip()
            if valsPart.startswith('(') and valsPart.endswith(')'):
                valsPart = valsPart[1:-1]
            valsRaw = []
            buf = ''
            inside = False
            i = 0
            while i < len(valsPart):
                ch = valsPart[i]
                if ch == '"':
                    inside = not inside
                    buf = buf + ch
                elif ch == ',' and not inside:
                    valsRaw.append(buf.strip())
                    buf = ''
                else:
                    buf = buf + ch
                i = i + 1
            if len(buf) > 0:
                valsRaw.append(buf.strip())
            values = []
            i = 0
            while i < len(valsRaw):
                v = valsRaw[i]
                if v.startswith('"') and v.endswith('"'):
                    values.append(v[1:-1])
                else:
                    values.append(int(v))
                i = i + 1
            eng = self._getEngine(tname)
            row = {}
            j = 0
            while j < len(cols):
                row[cols[j]] = values[j]
                j = j + 1
            eng.insertRow(row)
            return []
        if low.startswith('select'):
            pfrom = low.find(' from ')
            colsPart = s[6:pfrom].strip()
            tname = s[pfrom+6:].strip()
            where = None
            pwhere = tname.lower().find(' where ')
            if pwhere >= 0:
                wherePart = tname[pwhere+7:].strip()
                tname = tname[:pwhere].strip()
                eqPos = wherePart.find('=')
                if eqPos >= 0:
                    left = wherePart[:eqPos].strip()
                    right = wherePart[eqPos+1:].strip()
                    if right.startswith('"') and right.endswith('"') and len(right) >= 2:
                        right = right[1:-1]
                        where = (left, right)
                    else:
                        where = (left, int(right))
            colNames = []
            if colsPart == '*':
                colNames = ['*']
            else:
                parts = colsPart.split(',')
                i = 0
                while i < len(parts):
                    colNames.append(parts[i].strip())
                    i = i + 1
            eng = self._getEngine(tname)
            res = eng.select(colNames, where)
            return res
        if low.startswith('delete *'):
            after = s[len('delete *'):].strip()
            if after.lower().startswith('from'):
                tname = after[4:].strip()
                eng = self._getEngine(tname)
                eng.deleteAll()
                return []
        if low.startswith('delete from'):
            after = s[len('delete from'):].strip()
            tname = after.split()[0]
            where = after[after.lower().find('where')+5:].strip()
            eqPos = where.find('=')
            if eqPos >= 0:
                left = where[:eqPos].strip()
                right = where[eqPos+1:].strip()
                if right.startswith('"') and right.endswith('"') and len(right) >= 2:
                    rightVal = right[1:-1]
                    eng = self._getEngine(tname)
                    eng.deleteWhere(left, rightVal)
                    return []
                else:
                    eng = self._getEngine(tname)
                    eng.deleteWhere(left, int(right))
                    return []
            return []
        return []
