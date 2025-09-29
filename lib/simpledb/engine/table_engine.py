import os
import json
import struct
from ..schema import Schema
from ..paths import TableFiles
from ..indexes import IntIndex
from ..rowcodec import packValue, unpackValue

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
        open(self.files.dataPath(), 'wb').close()
        open(self.files.rowsPath(), 'wb').close()
        open(self.files.maskPath(), 'wb').close()
        open(self.files.lensPath(), 'wb').close()

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
            f.write(struct.pack('<B', 1 if active else 0))

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

    def rowCount(self):
        if not os.path.isfile(self.files.rowsPath()):
            return 0
        return os.path.getsize(self.files.rowsPath()) // 8

    def insertRow(self, valuesDict):
        cols = self.schema.columns
        count = len(cols)
        dirSize = count * 4
        payload = b''
        offsets = [0] * count
        i = 0
        while i < count:
            col = cols[i]
            name = col['name']
            ctype = col['type']
            cmax = col.get('max')
            val = valuesDict.get(name)
            packed = packValue(ctype, cmax, val)
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
            rest = f.read()
            return head + dirData + rest

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
            v, _ = unpackValue(dtype, data, off)
            res.append(v)
            j = j + 1
        return res

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
            v, _ = unpackValue(dtype, data, off)
            res.append(v)
            i = i + 1
        return res

    def deleteAll(self):
        open(self.files.dataPath(), 'wb').close()
        open(self.files.rowsPath(), 'wb').close()
        open(self.files.maskPath(), 'wb').close()
        open(self.files.lensPath(), 'wb').close()
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
                    v, _ = unpackValue(dtype, data, off)
                    match = False
                    if dtype == 'INT':
                        if int(v) == int(value):
                            match = True
                    else:
                        if str(v) == str(value):
                            match = True
                    if match:
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
                        v, _ = unpackValue(dtype, data, off)
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
