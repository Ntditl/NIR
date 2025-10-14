import json

# Простая структуризация исходного парсинга для совместимости
# Возвращает словарь {type:..., data:{...}}

def parseSql(sqlText: str):
    s = sqlText.strip()
    low = s.lower()
    if low.startswith('create table'):
        start = s.find('(')
        end = s.rfind(')')
        head = s[:start].strip()
        parts = head.split()
        tableName = parts[-1]
        colsDef = s[start+1:end]
        rawCols = colsDef.split(',')
        columns = []
        i = 0
        while i < len(rawCols):
            seg = rawCols[i].strip()
            tokens = seg.split()
            if len(tokens) < 2:
                i = i + 1
                continue
            name = tokens[0]
            t = tokens[1].upper()
            if t.startswith('VARCHAR'):
                ln = t[t.find('(')+1:t.find(')')]
                columns.append({'name': name, 'type': 'VARCHAR', 'max': int(ln)})
            else:
                columns.append({'name': name, 'type': 'INT'})
            i = i + 1
        return {'type': 'create_table', 'data': {'table': tableName, 'columns': columns}}
    if low.startswith('create index'):
        p = low.find('on')
        rest = s[p+2:].strip()
        tableName = rest.split('(')[0].strip()
        col = rest[rest.find('(')+1:rest.find(')')].strip()
        return {'type': 'create_index', 'data': {'table': tableName, 'column': col}}
    if low.startswith('insert into'):
        after = s[len('insert into'):].strip()
        tableName = after.split()[0]
        colsPart = after[after.find('(')+1:after.find(')')]
        cols = []
        for seg in colsPart.split(','):
            cols.append(seg.strip())
        posVals = low.find('values')
        if posVals < 0:
            return {'type': 'noop'}
        valsPart = s[posVals+6:].strip()
        if valsPart.startswith('(') and valsPart.endswith(')'):
            valsPart = valsPart[1:-1]
        rawVals = []
        buf = ''
        inside = False
        quoteChar = None
        i = 0
        while i < len(valsPart):
            ch = valsPart[i]
            if ch in ('"', "'"):
                if quoteChar is None:
                    quoteChar = ch
                    inside = True
                    buf = buf + ch
                elif quoteChar == ch:
                    inside = False
                    quoteChar = None
                    buf = buf + ch
                else:
                    buf = buf + ch
            elif ch == ',' and not inside:
                rawVals.append(buf.strip())
                buf = ''
            else:
                buf = buf + ch
            i = i + 1
        if len(buf) > 0:
            rawVals.append(buf.strip())
        values = []
        i = 0
        while i < len(rawVals):
            v = rawVals[i]
            if (v.startswith('"') and v.endswith('"') and len(v) >= 2) or (v.startswith("'") and v.endswith("'") and len(v) >= 2):
                values.append(v[1:-1])
            else:
                values.append(int(v))
            i = i + 1
        return {'type': 'insert', 'data': {'table': tableName, 'columns': cols, 'values': values}}
    if low.startswith('select'):
        pfrom = low.find(' from ')
        colsPart = s[6:pfrom].strip()
        tableSegment = s[pfrom+6:].strip()
        where = None
        pwhere = tableSegment.lower().find(' where ')
        if pwhere >= 0:
            wherePart = tableSegment[pwhere+7:].strip()
            tableName = tableSegment[:pwhere].strip()
            eqPos = wherePart.find('=')
            if eqPos >= 0:
                left = wherePart[:eqPos].strip()
                right = wherePart[eqPos+1:].strip()
                if (right.startswith('"') and right.endswith('"') and len(right) >= 2) or (right.startswith("'") and right.endswith("'") and len(right) >= 2):
                    where = (left, right[1:-1])
                else:
                    try:
                        where = (left, int(right))
                    except Exception:
                        where = (left, right)
        else:
            tableName = tableSegment.strip()
        if colsPart == '*':
            colNames = ['*']
        else:
            colNames = []
            parts = colsPart.split(',')
            i = 0
            while i < len(parts):
                colNames.append(parts[i].strip())
                i = i + 1
        return {'type': 'select', 'data': {'table': tableName, 'columns': colNames, 'where': where}}
    if low.startswith('delete *'):
        after = s[len('delete *'):].strip()
        if after.lower().startswith('from'):
            tableName = after[4:].strip()
            return {'type': 'delete_all', 'data': {'table': tableName}}
        return {'type': 'noop'}
    if low.startswith('delete from'):
        after = s[len('delete from'):].strip()
        tableName = after.split()[0]
        pwhere = after.lower().find('where')
        if pwhere < 0:
            return {'type': 'noop'}
        wherePart = after[pwhere+5:].strip()
        eqPos = wherePart.find('=')
        if eqPos >= 0:
            left = wherePart[:eqPos].strip()
            right = wherePart[eqPos+1:].strip()
            if (right.startswith('"') and right.endswith('"') and len(right) >= 2) or (right.startswith("'") and right.endswith("'") and len(right) >= 2):
                return {'type': 'delete_where', 'data': {'table': tableName, 'where': (left, right[1:-1])}}
            try:
                return {'type': 'delete_where', 'data': {'table': tableName, 'where': (left, int(right))}}
            except Exception:
                return {'type': 'delete_where', 'data': {'table': tableName, 'where': (left, right)}}
        return {'type': 'noop'}
    return {'type': 'noop'}
