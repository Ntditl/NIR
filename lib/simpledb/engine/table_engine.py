import os
import json
import struct
from ..schema import Schema
from ..paths import TableFiles
from ..indexes import IntIndex, StrIndex
from ..rowcodec import packValue, unpackValue

BASE_FIXED_HEADER = 1 + 4 + 2

class TableEngine:
    def __init__(self, files: TableFiles, schema: Schema):
        self.files = files
        self.schema = schema
        self.indexes = {}
        self.rowOffsets = []
        self.rowLengths = []
        if not os.path.isdir(files.baseDir):
            os.makedirs(files.baseDir, exist_ok=True)

    def create(self):
        with open(self.files.schemaPath(), 'w', encoding='utf-8') as f:
            json.dump(self.schema.toDict(), f, ensure_ascii=False)
        open(self.files.dataPath(), 'wb').close()
        self.rowOffsets = []
        self.rowLengths = []

    def open(self):
        size = 0
        if os.path.isfile(self.files.dataPath()):
            size = os.path.getsize(self.files.dataPath())
        self.rowOffsets = []
        self.rowLengths = []
        with open(self.files.dataPath(), 'rb') as f:
            pos = 0
            while pos < size:
                f.seek(pos)
                header = f.read(BASE_FIXED_HEADER)
                if len(header) < BASE_FIXED_HEADER:
                    break
                total_len = struct.unpack('<I', header[1:5])[0]
                if total_len <= BASE_FIXED_HEADER:
                    break
                self.rowOffsets.append(pos)
                self.rowLengths.append(total_len)
                pos += total_len
        for col in self.schema.columns:
            if col.get('index'):
                p = self.files.indexPath(col['name'])
                if os.path.isfile(p):
                    if col['type'] == 'INT':
                        self.indexes[col['name']] = IntIndex(p)
                    else:
                        self.indexes[col['name']] = StrIndex(p)
                else:
                    self._rebuildSingleIndex(col['name'])
            else:
                p = self.files.indexPath(col['name'])
                if os.path.isfile(p):
                    if col['type'] == 'INT':
                        self.indexes[col['name']] = IntIndex(p)
                    else:
                        self.indexes[col['name']] = StrIndex(p)

    def _rebuildSingleIndex(self, colName):
        targetIdx = -1
        targetType = None
        for i, c in enumerate(self.schema.columns):
            if c['name'] == colName:
                if c['type'] in ('INT', 'VARCHAR'):
                    targetIdx = i
                    targetType = c['type']
                break
        if targetIdx < 0:
            return
        path = self.files.indexPath(colName)
        idx = IntIndex(path) if targetType == 'INT' else StrIndex(path)
        total = self.rowCount()
        for rid in range(total):
            if self._isRowActive(rid):
                data = self._readRowBytes(rid)
                val = self._readColumnValueRaw(data, targetIdx)
                if val is not None:
                    if targetType == 'INT':
                        idx.add(int(val), rid)
                    else:
                        idx.add(str(val), rid)
        idx.save()
        self.indexes[colName] = idx

    def close(self):
        for name in self.indexes:
            self.indexes[name].save()

    def rowCount(self):
        return len(self.rowOffsets)

    def _isRowActive(self, rowId):
        if rowId < 0 or rowId >= self.rowCount():
            return False
        off = self.rowOffsets[rowId]
        with open(self.files.dataPath(), 'rb') as f:
            f.seek(off)
            b = f.read(1)
            if len(b) < 1:
                return False
            return b[0] == 1

    def _setRowInactive(self, rowId):
        if rowId < 0 or rowId >= self.rowCount():
            return
        off = self.rowOffsets[rowId]
        with open(self.files.dataPath(), 'r+b') as f:
            f.seek(off)
            f.write(b'\x00')

    def _readRowBytes(self, rowId):
        if rowId < 0 or rowId >= self.rowCount():
            return None
        off = self.rowOffsets[rowId]
        length = self.rowLengths[rowId]
        with open(self.files.dataPath(), 'rb') as f:
            f.seek(off)
            data = f.read(length)
            if len(data) < length:
                return None
            return data

    def _readHeader(self, data):
        if len(data) < BASE_FIXED_HEADER:
            return None, None, None
        active = data[0]
        total_len = struct.unpack('<I', data[1:5])[0]
        count = struct.unpack('<H', data[5:7])[0]
        return active, total_len, count

    def _columnDirOffset(self, colIndex):
        return BASE_FIXED_HEADER + colIndex * 4

    def _getValueOffset(self, data, colIndex):
        start = self._columnDirOffset(colIndex)
        return struct.unpack('<I', data[start:start+4])[0]

    def _readColumnValueRaw(self, data, colIndex):
        active, total_len, count = self._readHeader(data)
        if active is None or colIndex >= count:
            return None
        off = self._getValueOffset(data, colIndex)
        colDef = self.schema.columns[colIndex]
        dtype = colDef['type']
        v, _ = unpackValue(dtype, data, off)
        return v

    def insertRow(self, valuesDict):
        cols = self.schema.columns
        count = len(cols)
        dirSize = count * 4
        headerBaseLen = BASE_FIXED_HEADER + dirSize
        payload = b''
        offsets = [0] * count
        for i, col in enumerate(cols):
            name = col['name']
            ctype = col['type']
            cmax = col.get('max')
            val = valuesDict.get(name)
            packed = packValue(ctype, cmax, val)
            offsets[i] = headerBaseLen + len(payload)
            payload += packed
        dirBytes = b''.join(struct.pack('<I', off) for off in offsets)
        total_len = headerBaseLen + len(payload)
        rowBytes = b''.join([
            b'\x01',
            struct.pack('<I', total_len),
            struct.pack('<H', count),
            dirBytes,
            payload
        ])
        with open(self.files.dataPath(), 'ab') as f:
            startOffset = f.tell()
            f.write(rowBytes)
        self.rowOffsets.append(startOffset)
        self.rowLengths.append(total_len)
        rowId = self.rowCount() - 1
        for i, col in enumerate(cols):
            if self.indexes.get(col['name']) is not None:
                if col['type'] == 'INT':
                    v = int(valuesDict.get(col['name']))
                    self.indexes[col['name']].add(v, rowId)
                else:
                    v = '' if valuesDict.get(col['name']) is None else str(valuesDict.get(col['name']))
                    self.indexes[col['name']].add(v, rowId)
        return rowId

    def readColumns(self, rowId, colIndexes):
        if not self._isRowActive(rowId):
            return None
        data = self._readRowBytes(rowId)
        if data is None:
            return None
        active, total_len, count = self._readHeader(data)
        res = []
        for idx in colIndexes:
            if idx >= count:
                res.append(None)
                continue
            off = self._getValueOffset(data, idx)
            dtype = self.schema.columns[idx]['type']
            v, _ = unpackValue(dtype, data, off)
            res.append(v)
        return res

    def readRow(self, rowId):
        if not self._isRowActive(rowId):
            return None
        data = self._readRowBytes(rowId)
        if data is None:
            return None
        active, total_len, count = self._readHeader(data)
        res = []
        for i in range(count):
            off = self._getValueOffset(data, i)
            dtype = self.schema.columns[i]['type']
            v, _ = unpackValue(dtype, data, off)
            res.append(v)
        return res

    def deleteAll(self):
        open(self.files.dataPath(), 'wb').close()
        self.rowOffsets = []
        self.rowLengths = []
        for name in list(self.indexes.keys()):
            self.indexes[name].map = {}
            self.indexes[name].save()

    def deleteWhere(self, colName, value):
        colIdx = -1
        for i, c in enumerate(self.schema.columns):
            if c['name'] == colName:
                colIdx = i
                break
        if colIdx < 0:
            return 0
        deleted = 0
        col = self.schema.columns[colIdx]
        if self.indexes.get(colName) is not None:
            key = int(value) if col['type'] == 'INT' else str(value)
            rowIds = self.indexes[colName].getRowIds(key)
            s = set(rowIds)
            for rid in rowIds:
                if self._isRowActive(rid):
                    self._setRowInactive(rid)
                    deleted += 1
            self.indexes[colName].removeRowIds(key, s)
            return deleted
        total = self.rowCount()
        for rid in range(total):
            if self._isRowActive(rid):
                data = self._readRowBytes(rid)
                v = self._readColumnValueRaw(data, colIdx)
                if v is not None:
                    match = False
                    if col['type'] == 'INT':
                        match = int(v) == int(value)
                    else:
                        match = str(v) == str(value)
                    if match:
                        self._setRowInactive(rid)
                        deleted += 1
        return deleted

    def select(self, colNames, where):
        selIdx = []
        if colNames == ['*']:
            for i in range(len(self.schema.columns)):
                selIdx.append(i)
        else:
            for name in colNames:
                for j, c in enumerate(self.schema.columns):
                    if c['name'] == name:
                        selIdx.append(j)
                        break
        rowIds = None
        if where is not None:
            wcol, wval = where[0], where[1]
            idx = -1
            for i, c in enumerate(self.schema.columns):
                if c['name'] == wcol:
                    idx = i
                    break
            if idx >= 0:
                col = self.schema.columns[idx]
                if self.indexes.get(wcol) is not None:
                    key = int(wval) if col['type'] == 'INT' else str(wval)
                    rowIds = self.indexes[wcol].getRowIds(key)
        res = []
        if rowIds is not None:
            for rid in rowIds:
                if self._isRowActive(rid):
                    vals = self.readColumns(rid, selIdx)
                    if vals is not None:
                        res.append(tuple(vals))
            return res
        total = self.rowCount()
        for rid in range(total):
            if self._isRowActive(rid):
                if where is None:
                    vals = self.readColumns(rid, selIdx)
                    if vals is not None:
                        res.append(tuple(vals))
                else:
                    wcol, wval = where[0], where[1]
                    idx = -1
                    for i, c in enumerate(self.schema.columns):
                        if c['name'] == wcol:
                            idx = i
                            break
                    if idx >= 0:
                        data = self._readRowBytes(rid)
                        v = self._readColumnValueRaw(data, idx)
                        ok = False
                        colType = self.schema.columns[idx]['type']
                        if colType == 'INT':
                            ok = int(v) == int(wval)
                        else:
                            ok = str(v) == str(wval)
                        if ok:
                            vals = self.readColumns(rid, selIdx)
                            if vals is not None:
                                res.append(tuple(vals))
        return res
