import importlib
from lib.db.models import recreateAllTables
from lib.db.connection import getDbConnection

# Принудительно перезагружаем модуль генератора для получения обновленной версии
import lib.data.generators
importlib.reload(lib.data.generators)
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

    gen.generateViewers(5)
    v = count_rows('viewer')
    check(v >= 5, 'viewer count')

    gen.generateMovies(4)
    m = count_rows('movie')
    check(m >= 4, 'movie count')

    gen.generateViewerProfiles(3)
    vp = count_rows('viewer_profile')
    check(vp >= 3, 'viewer_profile count')

    gen.generateFavoriteMovies(5)
    fav = count_rows('favorite_movies')
    check(fav >= 5, 'favorite_movies generated')

    gen.generateViewers(3)
    v2 = count_rows('viewer')
    check(v2 >= 8, 'viewer append')

    print('generator_manual OK')

if __name__ == '__main__':
    run()
