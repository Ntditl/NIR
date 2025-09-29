from lib.backupManager import BackupManager
from lib.tableModels import getTableNames, getCreateTablesSql, getDropTablesSql
from lib.databaseConnection import getDbConnection
import os
import csv


def recreateSchema():
    with getDbConnection() as (conn, cur):
        drops = getDropTablesSql()
        i = 0
        while i < len(drops):
            cur.execute(drops[i])
            i = i + 1
        creates = getCreateTablesSql()
        i = 0
        while i < len(creates):
            cur.execute(creates[i])
            i = i + 1


def insertMinimalData():
    with getDbConnection() as (conn, cur):
        cur.execute("INSERT INTO viewer (first_name, last_name, email, phone_number) VALUES ('Test','User','test@test.com','+000111') RETURNING viewer_id")
        viewerId = cur.fetchone()[0]
        cur.execute("INSERT INTO movie (title, genre, duration_minutes, release_date, rating, age_restriction) VALUES ('Movie','Comedy',120, '2023-01-01','PG-13', 13) RETURNING movie_id")
        movieId = cur.fetchone()[0]
        cur.execute("INSERT INTO cinema (name, address, phone_number, city) VALUES ('Cine','Addr','+0123','City') RETURNING cinema_id")
        cinemaId = cur.fetchone()[0]
        cur.execute("INSERT INTO hall (cinema_id, hall_name, seat_count, base_ticket_price) VALUES (%s,'Hall 1',100, 10.50) RETURNING hall_id", (cinemaId,))
        hallId = cur.fetchone()[0]
        cur.execute("INSERT INTO session (movie_id, hall_id, session_datetime, available_seats, final_price) VALUES (%s,%s,'2025-01-01 12:00:00+00',50, 12.00) RETURNING session_id", (movieId, hallId))
        sessionId = cur.fetchone()[0]
        cur.execute("INSERT INTO viewer_profile (viewer_id, male_gender, nickname, birth_date) VALUES (%s, true, 'nick', '2000-01-01')", (viewerId,))
        cur.execute("INSERT INTO favorite_movies (viewer_id, movie_id) VALUES (%s, %s)", (viewerId, movieId))
        cur.execute("INSERT INTO movie_review (movie_id, viewer_id, rating, comment) VALUES (%s, %s, 8, 'ok')", (movieId, viewerId))
        cur.execute("INSERT INTO ticket (session_id, viewer_id) VALUES (%s, %s)", (sessionId, viewerId))


def verifyBackupFiles(directoryPath):
    names = getTableNames()
    i = 0
    while i < len(names):
        path = os.path.join(directoryPath, names[i] + ".csv")
        if not os.path.exists(path):
            raise RuntimeError("backup file missing for " + names[i])
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            if header is None:
                raise RuntimeError("csv header missing for " + names[i])
        i = i + 1


def checkCountsEqual():
    with getDbConnection() as (conn, cur):
        names = getTableNames()
        i = 0
        while i < len(names):
            cur.execute("SELECT COUNT(*) FROM " + names[i])
            count = cur.fetchone()[0]
            if count != 1 and names[i] not in ['favorite_movies','movie_review','ticket']:
                raise RuntimeError("unexpected count in " + names[i])
            i = i + 1


def main():
    recreateSchema()
    insertMinimalData()
    backupDir = os.path.join(os.path.dirname(__file__), 'tmp_backup')
    if not os.path.isdir(backupDir):
        os.makedirs(backupDir, exist_ok=True)
    mgr = BackupManager(backupDir)
    mgr.backupAllTables()
    verifyBackupFiles(backupDir)
    recreateSchema()
    mgr.restoreAllTables()
    checkCountsEqual()
    print('Проверки backupManager пройдены')

if __name__ == '__main__':
    main()
