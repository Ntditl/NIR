import pytest
import os
import shutil
from lib.simpledb.database import SimpleDatabase
from lib.simpledb.parser.sqlParser import parseSql


def test_parse_create_table():
    result = parseSql("CREATE TABLE users (id INT, name VARCHAR(50))")
    assert result['type'] == 'create_table'
    assert result['data']['table'] == 'users'
    assert len(result['data']['columns']) == 2
    assert result['data']['columns'][0]['name'] == 'id'
    assert result['data']['columns'][0]['type'] == 'INT'
    assert result['data']['columns'][1]['name'] == 'name'
    assert result['data']['columns'][1]['type'] == 'VARCHAR'
    assert result['data']['columns'][1]['max'] == 50


def test_parse_insert_with_single_quotes():
    result = parseSql("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    assert result['type'] == 'insert'
    assert result['data']['table'] == 'users'
    assert result['data']['columns'] == ['id', 'name']
    assert result['data']['values'] == [1, 'Bob']


def test_parse_insert_with_double_quotes():
    result = parseSql('INSERT INTO users (id, name) VALUES (2, "Alice")')
    assert result['type'] == 'insert'
    assert result['data']['table'] == 'users'
    assert result['data']['columns'] == ['id', 'name']
    assert result['data']['values'] == [2, 'Alice']


def test_parse_insert_mixed_values():
    result = parseSql("INSERT INTO products (id, title, price) VALUES (10, 'Laptop', 1000)")
    assert result['type'] == 'insert'
    assert result['data']['values'] == [10, 'Laptop', 1000]


def test_parse_select_all():
    result = parseSql("SELECT * FROM users")
    assert result['type'] == 'select'
    assert result['data']['table'] == 'users'
    assert result['data']['columns'] == ['*']
    assert result['data']['where'] is None


def test_parse_select_with_where_int():
    result = parseSql("SELECT * FROM users WHERE id=5")
    assert result['type'] == 'select'
    assert result['data']['where'] == ('id', 5)


def test_parse_select_with_where_string_single_quotes():
    result = parseSql("SELECT * FROM users WHERE name='Bob'")
    assert result['type'] == 'select'
    assert result['data']['where'] == ('name', 'Bob')


def test_parse_select_with_where_string_double_quotes():
    result = parseSql('SELECT * FROM users WHERE name="Alice"')
    assert result['type'] == 'select'
    assert result['data']['where'] == ('name', 'Alice')


def test_parse_delete_where_single_quotes():
    result = parseSql("DELETE FROM users WHERE name='Bob'")
    assert result['type'] == 'delete_where'
    assert result['data']['table'] == 'users'
    assert result['data']['where'] == ('name', 'Bob')


def test_parse_create_index():
    result = parseSql("CREATE INDEX ON users(id)")
    assert result['type'] == 'create_index'
    assert result['data']['table'] == 'users'
    assert result['data']['column'] == 'id'


@pytest.fixture(scope='function')
def temp_db_dir():
    testDir = 'C:\\Users\\Ntditl\\PycharmProjects\\scienceResearche\\temp_simpledb_test'
    if os.path.exists(testDir):
        shutil.rmtree(testDir)
    os.makedirs(testDir)
    yield testDir
    if os.path.exists(testDir):
        shutil.rmtree(testDir)


def test_simpledb_create_and_insert_single_quotes(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Bob')")

    results = db.execute("SELECT * FROM users")
    assert len(results) == 1
    assert results[0][0] == 1
    assert results[0][1] == 'Bob'

    db.closeAll()


def test_simpledb_create_and_insert_double_quotes(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")
    db.execute('INSERT INTO users (id, name) VALUES (2, "Alice")')

    results = db.execute("SELECT * FROM users")
    assert len(results) == 1
    assert results[0][0] == 2
    assert results[0][1] == 'Alice'

    db.closeAll()


def test_simpledb_multiple_inserts_mixed_quotes(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    db.execute('INSERT INTO users (id, name) VALUES (2, "Alice")')
    db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")

    results = db.execute("SELECT * FROM users")
    assert len(results) == 3
    assert results[0][1] == 'Bob'
    assert results[1][1] == 'Alice'
    assert results[2][1] == 'Charlie'

    db.closeAll()


def test_simpledb_select_where_string(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    db.execute("INSERT INTO users (id, name) VALUES (2, 'Alice')")

    results = db.execute("SELECT * FROM users WHERE name='Bob'")
    assert len(results) == 1
    assert results[0][1] == 'Bob'

    db.closeAll()


def test_simpledb_delete_where_string(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    db.execute("INSERT INTO users (id, name) VALUES (2, 'Alice')")

    db.execute("DELETE FROM users WHERE name='Bob'")

    results = db.execute("SELECT * FROM users")
    assert len(results) == 1
    assert results[0][1] == 'Alice'

    db.closeAll()


def test_simpledb_three_columns_with_strings(temp_db_dir):
    db = SimpleDatabase(temp_db_dir)
    db.execute("CREATE TABLE products (product_id INT, title VARCHAR(100), price INT)")
    db.execute("INSERT INTO products (product_id, title, price) VALUES (10, 'Laptop', 1000)")
    db.execute("INSERT INTO products (product_id, title, price) VALUES (20, 'Mouse', 25)")

    results = db.execute("SELECT * FROM products")
    assert len(results) == 2
    assert results[0][1] == 'Laptop'
    assert results[0][2] == 1000
    assert results[1][1] == 'Mouse'
    assert results[1][2] == 25

    db.closeAll()
