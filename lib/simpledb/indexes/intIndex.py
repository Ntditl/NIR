import os
import struct

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

