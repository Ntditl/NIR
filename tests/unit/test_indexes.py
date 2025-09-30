import os
import shutil
from lib.simpledb.indexes import IntIndex, StrIndex

tmpDir = 'test_index_data'


def reset():
    if os.path.isdir(tmpDir):
        shutil.rmtree(tmpDir)
    os.makedirs(tmpDir, exist_ok=True)


def testIntIndex():
    reset()
    path = os.path.join(tmpDir, 'int.idx')
    idx = IntIndex(path)
    for i in range(5):
        idx.add(i, i * 10)
        idx.add(i, i * 10 + 1)
    idx.save()
    idx2 = IntIndex(path)
    for k in range(5):
        rows = idx2.getRowIds(k)
        if len(rows) != 2:
            raise RuntimeError('int index load failed')
    idx2.removeRowIds(2, set(idx2.getRowIds(2)))
    if len(idx2.getRowIds(2)) != 0:
        raise RuntimeError('int index remove failed')


def testStrIndex():
    reset()
    path = os.path.join(tmpDir, 'str.idx')
    sidx = StrIndex(path)
    words = ['alpha','beta','gamma']
    rid = 0
    for w in words:
        sidx.add(w, rid)
        sidx.add(w, rid + 100)
        rid += 1
    sidx.save()
    sidx2 = StrIndex(path)
    for w in words:
        if len(sidx2.getRowIds(w)) != 2:
            raise RuntimeError('str index load failed')
    sidx2.removeRowIds('beta', set(sidx2.getRowIds('beta')))
    if len(sidx2.getRowIds('beta')) != 0:
        raise RuntimeError('str index remove failed')


def main():
    testIntIndex()
    testStrIndex()
    print('Проверка индексов пройдена')

if __name__ == '__main__':
    main()

