from lib.data.generators import RandomDataGenerator
from lib.db.models import getTableNames
from lib.db.connection import getDbConnection


def countRows(tableName):
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM " + tableName)
        return cur.fetchone()[0]


def checkCounts(n, hallsPerCinema, sessionsPerHall):
    counts = {}
    names = getTableNames()
    i = 0
    while i < len(names):
        t = names[i]
        counts[t] = countRows(t)
        i = i + 1
    if counts.get("viewer", 0) != n:
        raise RuntimeError("несовпадение количества viewer")
    if counts.get("movie", 0) != n:
        raise RuntimeError("несовпадение количества movie")
    if counts.get("viewer_profile", 0) != n:
        raise RuntimeError("несовпадение количества viewer_profile")
    if counts.get("cinema", 0) != n:
        raise RuntimeError("несовпадение количества cinema")
    if counts.get("hall", 0) != n * hallsPerCinema:
        raise RuntimeError("несовпадение количества hall")
    if counts.get("session", 0) != n * hallsPerCinema * sessionsPerHall:
        raise RuntimeError("несовпадение количества session")


def checkForeignKeys():
    with getDbConnection() as (conn, cur):
        cur.execute("SELECT COUNT(*) FROM viewer_profile vp LEFT JOIN viewer v ON vp.viewer_id = v.viewer_id WHERE v.viewer_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в viewer_profile")
        cur.execute("SELECT COUNT(*) FROM favorite_movies fm LEFT JOIN viewer v ON fm.viewer_id = v.viewer_id LEFT JOIN movie m ON fm.movie_id = m.movie_id WHERE v.viewer_id IS NULL OR m.movie_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в favorite_movies")
        cur.execute("SELECT COUNT(*) FROM movie_review mr LEFT JOIN viewer v ON mr.viewer_id = v.viewer_id LEFT JOIN movie m ON mr.movie_id = m.movie_id WHERE v.viewer_id IS NULL OR m.movie_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в movie_review")
        cur.execute("SELECT COUNT(*) FROM hall h LEFT JOIN cinema c ON h.cinema_id = c.cinema_id WHERE c.cinema_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в hall")
        cur.execute("SELECT COUNT(*) FROM session s LEFT JOIN movie m ON s.movie_id = m.movie_id LEFT JOIN hall h ON s.hall_id = h.hall_id WHERE m.movie_id IS NULL OR h.hall_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в session")
        cur.execute("SELECT COUNT(*) FROM ticket t LEFT JOIN session s ON t.session_id = s.session_id LEFT JOIN viewer v ON t.viewer_id = v.viewer_id WHERE s.session_id IS NULL OR v.viewer_id IS NULL")
        if cur.fetchone()[0] != 0:
            raise RuntimeError("висячая ссылка в ticket")


def main():
    n = 3
    hallsPerCinema = 2
    sessionsPerHall = 2
    gen = RandomDataGenerator()
    gen.generateData(
        viewersCount=n,
        moviesCount=n,
        cinemasCount=n,
        hallsPerCinema=hallsPerCinema,
        sessionsPerHall=sessionsPerHall,
        favoriteRate=0.5,
        reviewRate=0.5,
        ticketRate=0.5
    )
    checkCounts(n, hallsPerCinema, sessionsPerHall)
    checkForeignKeys()
    print("Проверки randomDataGenerator пройдены")

if __name__ == "__main__":
    main()
