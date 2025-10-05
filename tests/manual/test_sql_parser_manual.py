from lib.simpledb.parser.sql_parser import parseSql

def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('sql_parser_manual start')
    c = parseSql('CREATE TABLE t1 (id INT, name VARCHAR(16))')
    check(c['type'] == 'create_table', 'create_table type')
    ins = parseSql('INSERT INTO t1 (id,name) VALUES (1,"Alice")')
    check(ins['type'] == 'insert' and ins['data']['values'][0] == 1, 'insert parse')
    selAll = parseSql('SELECT * FROM t1')
    check(selAll['type'] == 'select' and selAll['data']['columns'][0] == '*', 'select * parse')
    selWhere = parseSql('SELECT id,name FROM t1 WHERE id=1')
    check(selWhere['data']['where'] == ('id', 1), 'select where parse')
    cind = parseSql('CREATE INDEX ON t1(id)')
    check(cind['type'] == 'create_index', 'create index type')
    delAll = parseSql('DELETE * FROM t1')
    check(delAll['type'] == 'delete_all', 'delete all type')
    delWhere = parseSql('DELETE FROM t1 WHERE id=2')
    check(delWhere['type'] == 'delete_where' and delWhere['data']['where'][1] == 2, 'delete where parse')
    print('sql_parser_manual ok')

if __name__ == '__main__':
    run()

