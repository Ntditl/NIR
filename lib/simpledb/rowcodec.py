from .constants import MAX_UINT64
import struct

def packValue(colType, colMax, value):
    if colType == 'INT':
        n = int(value)
        if n < 0:
            raise ValueError('INT negative')
        if n > MAX_UINT64:
            raise ValueError('INT overflow')
        return struct.pack('<Q', n)
    s = '' if value is None else str(value)
    b = s.encode('utf-8')
    if colMax is not None and len(b) > int(colMax):
        b = b[:int(colMax)]
    return struct.pack('<I', len(b)) + b

def unpackValue(colType, data, offset):
    if colType == 'INT':
        v = struct.unpack('<Q', data[offset:offset+8])[0]
        return v, offset + 8
    ln = struct.unpack('<I', data[offset:offset+4])[0]
    start = offset + 4
    end = start + ln
    s = data[start:end].decode('utf-8', errors='ignore')
    return s, end

