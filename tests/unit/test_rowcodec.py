from lib.simpledb.rowcodec import packValue, unpackValue
from lib.simpledb.constants import MAX_UINT64

def testIntPack():
    b = packValue('INT', None, 123)
    v, off = unpackValue('INT', b, 0)
    if v != 123:
        raise RuntimeError('int roundtrip failed')
    b2 = packValue('INT', None, MAX_UINT64)
    v2, _ = unpackValue('INT', b2, 0)
    if v2 != MAX_UINT64:
        raise RuntimeError('max uint64 failed')

def testStrPack():
    s = 'Привет'
    b = packValue('VARCHAR', 100, s)
    v, off = unpackValue('VARCHAR', b, 0)
    if v != s:
        raise RuntimeError('unicode roundtrip failed')
    long = 'абв'*50
    b2 = packValue('VARCHAR', 30, long)
    v2, _ = unpackValue('VARCHAR', b2, 0)
    if len(v2.encode('utf-8')) > 30:
        raise RuntimeError('truncate failed')

def main():
    testIntPack()
    testStrPack()
    print('Проверка rowcodec пройдена')

if __name__ == '__main__':
    main()

