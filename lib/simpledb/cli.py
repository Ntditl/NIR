import argparse
import os
import sys
import json

try:
    from .schema import Schema
    from .paths import TableFiles
    from .engine.table_engine import TableEngine
except ImportError:
    if __package__ is None or __package__ == '':
        currentPath = os.path.abspath(__file__)
        projectRoot = os.path.dirname(os.path.dirname(os.path.dirname(currentPath)))
        if projectRoot not in sys.path:
            sys.path.insert(0, projectRoot)
        from lib.simpledb.schema import Schema
        from lib.simpledb.paths import TableFiles
        from lib.simpledb.engine.table_engine import TableEngine

DEFAULT_BASE_DIR = "simpledb_data"

def readLine(prompt):
    try:
        return input(prompt)
    except EOFError:
        return None

def parseColumns(colsSpec):
    parts = colsSpec.strip().split()
    cols = []
    for p in parts:
        segs = p.split(':')
        name = None
        ctype = None
        cmax = None
        cindex = False
        for s in segs:
            if name is None:
                name = s
                continue
            if ctype is None:
                ctype = s.upper()
                continue
            if ctype == 'VARCHAR' and cmax is None and s.isdigit():
                cmax = int(s)
                continue
            if s.lower() == 'index':
                cindex = True
        if ctype is None:
            raise SystemExit('bad column spec')
        col = {"name": name, "type": ctype}
        if ctype == 'VARCHAR' and cmax is not None:
            col['max'] = cmax
        if cindex and ctype == 'INT':
            col['index'] = True
        cols.append(col)
    return cols

def loadSchema(baseDir, tableName):
    files = TableFiles(baseDir, tableName)
    path = files.schemaPath()
    if not os.path.isfile(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        d = json.load(f)
    return Schema.fromDict(d)

def ensureEngine(baseDir, tableName, schema=None):
    files = TableFiles(baseDir, tableName)
    if schema is None:
        schema = loadSchema(baseDir, tableName)
    if schema is None:
        raise SystemExit('schema not found')
    exists = os.path.isfile(files.schemaPath()) and os.path.isfile(files.dataPath())
    eng = TableEngine(files, schema)
    if exists:
        eng.open()
    else:
        eng.create()
        eng.open()
    return eng

def cmdCreate(args):
    cols = parseColumns(args.cols)
    schema = Schema(args.table, cols)
    files = TableFiles(args.dir, args.table)
    if os.path.exists(files.schemaPath()) or os.path.exists(files.dataPath()):
        raise SystemExit('table exists')
    eng = TableEngine(files, schema)
    eng.create()
    print('created')

def parseKeyValues(kvs):
    data = {}
    for kv in kvs:
        if '=' not in kv:
            raise SystemExit('bad kv')
        k, v = kv.split('=', 1)
        data[k] = v
    return data

def cmdInsert(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    eng = ensureEngine(args.dir, args.table, schema)
    data = parseKeyValues(args.values)
    for col in schema.columns:
        if col['name'] not in data:
            raise SystemExit('missing column ' + col['name'])
    eng.insertRow(data)
    eng.close()
    print('ok')

def parseWhere(w):
    if w is None:
        return None
    if '=' not in w:
        raise SystemExit('bad where')
    k, v = w.split('=', 1)
    return (k, v)

def cmdSelect(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    eng = ensureEngine(args.dir, args.table, schema)
    cols = ['*'] if args.cols == '*' else args.cols.split(',')
    where = parseWhere(args.where)
    rows = eng.select(cols, where)
    out = rows
    if args.limit is not None:
        out = rows[:args.limit]
    for r in out:
        print(r)
    eng.close()

def cmdDelete(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    eng = ensureEngine(args.dir, args.table, schema)
    if args.all:
        eng.deleteAll()
        eng.close()
        print('deleted all')
        return
    where = parseWhere(args.where)
    if where is None:
        raise SystemExit('need where or --all')
    k, v = where
    n = eng.deleteWhere(k, v)
    eng.close()
    print('deleted', n)

def cmdDrop(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    files = TableFiles(args.dir, args.table)
    for col in schema.columns:
        p = files.indexPath(col['name'])
        if os.path.isfile(p):
            os.remove(p)
    if os.path.isfile(files.dataPath()):
        os.remove(files.dataPath())
    if os.path.isfile(files.schemaPath()):
        os.remove(files.schemaPath())
    print('dropped')

def cmdSchema(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    print(json.dumps(schema.toDict(), ensure_ascii=False))

def cmdList(args):
    if not os.path.isdir(args.dir):
        return
    for fn in os.listdir(args.dir):
        if fn.endswith('.schema.json'):
            name = fn[:-12]
            print(name)

def cmdRebuildIndex(args):
    schema = loadSchema(args.dir, args.table)
    if schema is None:
        raise SystemExit('no schema')
    eng = ensureEngine(args.dir, args.table, schema)
    eng._rebuildSingleIndex(args.col)
    eng.close()
    print('rebuilt')

def cmdMenu(args):
    baseDir = args.dir
    while True:
        print('1 create')
        print('2 insert')
        print('3 select')
        print('4 delete where')
        print('5 delete all')
        print('6 drop')
        print('7 schema')
        print('8 list')
        print('9 rebuild index')
        print('0 exit')
        choice = readLine('> ')
        if choice is None:
            break
        choice = choice.strip()
        if choice == '0':
            break
        if choice == '1':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            colsSpec = readLine('cols spec: ')
            if colsSpec is None:
                break
            colsSpec = colsSpec.strip()
            try:
                cols = parseColumns(colsSpec)
                schema = Schema(tableName, cols)
                files = TableFiles(baseDir, tableName)
                if os.path.exists(files.schemaPath()) or os.path.exists(files.dataPath()):
                    print('exists')
                else:
                    eng = TableEngine(files, schema)
                    eng.create()
                    print('created')
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '2':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            schema = loadSchema(baseDir, tableName)
            if schema is None:
                print('no schema')
                continue
            pairs = []
            i = 0
            while i < len(schema.columns):
                col = schema.columns[i]
                val = readLine(col['name'] + '=')
                if val is None:
                    break
                pairs.append(col['name'] + '=' + val)
                i = i + 1
            if len(pairs) != len(schema.columns):
                break
            try:
                eng = ensureEngine(baseDir, tableName, schema)
                data = parseKeyValues(pairs)
                eng.insertRow(data)
                eng.close()
                print('ok')
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '3':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            colsInput = readLine('cols (* or comma list): ')
            if colsInput is None:
                break
            colsInput = colsInput.strip()
            whereInput = readLine('where (col=val or empty): ')
            if whereInput is None:
                break
            whereInput = whereInput.strip()
            limitInput = readLine('limit (empty for none): ')
            if limitInput is None:
                break
            limitInput = limitInput.strip()
            limitVal = None
            if limitInput != '':
                try:
                    limitVal = int(limitInput)
                except ValueError:
                    print('bad limit')
                    continue
            try:
                schema = loadSchema(baseDir, tableName)
                if schema is None:
                    print('no schema')
                    continue
                eng = ensureEngine(baseDir, tableName, schema)
                cols = ['*'] if colsInput == '*' or colsInput == '' else colsInput.split(',')
                where = parseWhere(whereInput) if whereInput != '' else None
                rows = eng.select(cols, where)
                out = rows if limitVal is None else rows[:limitVal]
                j = 0
                while j < len(out):
                    print(out[j])
                    j = j + 1
                eng.close()
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '4':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            cond = readLine('where col=val: ')
            if cond is None:
                break
            cond = cond.strip()
            try:
                schema = loadSchema(baseDir, tableName)
                if schema is None:
                    print('no schema')
                    continue
                eng = ensureEngine(baseDir, tableName, schema)
                where = parseWhere(cond)
                if where is None:
                    print('bad where')
                else:
                    k, v = where
                    n = eng.deleteWhere(k, v)
                    print('deleted', n)
                eng.close()
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '5':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            try:
                schema = loadSchema(baseDir, tableName)
                if schema is None:
                    print('no schema')
                    continue
                eng = ensureEngine(baseDir, tableName, schema)
                eng.deleteAll()
                eng.close()
                print('deleted all')
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '6':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            try:
                schema = loadSchema(baseDir, tableName)
                if schema is None:
                    print('no schema')
                    continue
                files = TableFiles(baseDir, tableName)
                for col in schema.columns:
                    p = files.indexPath(col['name'])
                    if os.path.isfile(p):
                        os.remove(p)
                if os.path.isfile(files.dataPath()):
                    os.remove(files.dataPath())
                if os.path.isfile(files.schemaPath()):
                    os.remove(files.schemaPath())
                print('dropped')
            except SystemExit as e:
                print(str(e))
            continue
        if choice == '7':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            schema = loadSchema(baseDir, tableName)
            if schema is None:
                print('no schema')
            else:
                print(json.dumps(schema.toDict(), ensure_ascii=False))
            continue
        if choice == '8':
            if not os.path.isdir(baseDir):
                continue
            for fn in os.listdir(baseDir):
                if fn.endswith('.schema.json'):
                    print(fn[:-12])
            continue
        if choice == '9':
            tableName = readLine('table: ')
            if tableName is None:
                break
            tableName = tableName.strip()
            col = readLine('col: ')
            if col is None:
                break
            col = col.strip()
            try:
                schema = loadSchema(baseDir, tableName)
                if schema is None:
                    print('no schema')
                    continue
                eng = ensureEngine(baseDir, tableName, schema)
                eng._rebuildSingleIndex(col)
                eng.close()
                print('rebuilt')
            except SystemExit as e:
                print(str(e))
            continue
        print('bad choice')

def buildParser():
    p = argparse.ArgumentParser()
    p.add_argument('--dir', default=DEFAULT_BASE_DIR)
    sub = p.add_subparsers(dest='cmd')
    c1 = sub.add_parser('create')
    c1.add_argument('--table', required=True)
    c1.add_argument('--cols', required=True)
    c1.set_defaults(func=cmdCreate)
    c2 = sub.add_parser('insert')
    c2.add_argument('--table', required=True)
    c2.add_argument('values', nargs='+')
    c2.set_defaults(func=cmdInsert)
    c3 = sub.add_parser('select')
    c3.add_argument('--table', required=True)
    c3.add_argument('--cols', default='*')
    c3.add_argument('--where')
    c3.add_argument('--limit', type=int)
    c3.set_defaults(func=cmdSelect)
    c4 = sub.add_parser('delete')
    c4.add_argument('--table', required=True)
    c4.add_argument('--where')
    c4.add_argument('--all', action='store_true')
    c4.set_defaults(func=cmdDelete)
    c5 = sub.add_parser('drop')
    c5.add_argument('--table', required=True)
    c5.set_defaults(func=cmdDrop)
    c6 = sub.add_parser('schema')
    c6.add_argument('--table', required=True)
    c6.set_defaults(func=cmdSchema)
    c7 = sub.add_parser('list')
    c7.set_defaults(func=cmdList)
    c8 = sub.add_parser('rebuild-index')
    c8.add_argument('--table', required=True)
    c8.add_argument('--col', required=True)
    c8.set_defaults(func=cmdRebuildIndex)
    c9 = sub.add_parser('menu')
    c9.set_defaults(func=cmdMenu)
    return p

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = buildParser()
    if len(argv) == 0:
        dummy = argparse.Namespace(dir=DEFAULT_BASE_DIR)
        cmdMenu(dummy)
        return
    args = parser.parse_args(argv)
    if not hasattr(args, 'func'):
        parser.print_help()
        return
    args.func(args)

if __name__ == '__main__':
    main()
