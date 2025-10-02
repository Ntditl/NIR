from lib.db.models import recreateAllTables
from lib.db.connection import getDbConnection
from lib.data.generators import RandomDataGenerator


def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def count_rows(table):
    with getDbConnection() as (conn, cur):
        cur.execute('SELECT COUNT(*) FROM ' + table)
        return cur.fetchone()[0]

def run():
    print('generator_manual start')
    recreateAllTables(True)
    gen = RandomDataGenerator()
    gen.generateData(5, 4, 2, 1, 1, 0.3, 0.3, 0.3)
    v = count_rows('viewer')
    m = count_rows('movie')
    c = count_rows('cinema')
    h = count_rows('hall')
    check(v >= 5, 'viewer count')
    check(m >= 4, 'movie count')
    check(c >= 2, 'cinema count')
    check(h >= 2, 'hall count derived')
    # generateTable individual
    gen.generateTable('viewer', 3)
    v2 = count_rows('viewer')
    check(v2 >= 3, 'generateTable viewer append')
    gen.generateTable('favorite_movies', 5)
    fav = count_rows('favorite_movies')
    check(fav >= 5, 'favorite_movies generated')
    gen.generateTable('movie_review', 4)
    rev = count_rows('movie_review')
    check(rev >= 4, 'movie_review generated')
    gen.generateTable('session', 2)
    sess = count_rows('session')
    check(sess >= 2, 'session generated')
    gen.generateTable('ticket', 2)
    t = count_rows('ticket')
    check(t >= 2, 'ticket generated')
    print('generator_manual ok')

if __name__ == '__main__':
    run()

