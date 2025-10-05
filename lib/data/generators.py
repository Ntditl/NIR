import random
import string
from datetime import date, datetime, timedelta
from decimal import Decimal
from lib.db.connection import getDbConnection

UNICODE_EXTRA_CHARS = "阿测试漢字مرحبا"
EMAIL_NAME_LEN = 6
EMAIL_DOMAIN_LEN = 5
EMAIL_MAX_ATTEMPTS = 50

class RandomDataGenerator:
    def __init__(self):
        self.generatedHallIds = []
        self.usedEmails = set()
        self.schemaPrefix = ""

    def setSchemaPrefix(self, prefix: str):
        self.schemaPrefix = prefix

    def _getTableName(self, tableName: str):
        return self.schemaPrefix + tableName

    def _randomString(self, length=8, includeUnicode=False):
        letters = string.ascii_letters
        if includeUnicode:
            letters += UNICODE_EXTRA_CHARS
        return ''.join(random.choice(letters) for _ in range(length))

    def _randomPhone(self):
        return f"+7{random.randint(1000000000, 9999999999)}"

    def _randomEmail(self):
        attempts = 0
        while attempts < EMAIL_MAX_ATTEMPTS:
            name = self._randomString(EMAIL_NAME_LEN)
            domain = self._randomString(EMAIL_DOMAIN_LEN)
            email = f"{name}@{domain}.com"
            if email.lower() not in self.usedEmails:
                self.usedEmails.add(email.lower())
                return email
            attempts += 1
        return f"user{random.randint(10000, 99999)}@example.com"

    def _randomDate(self, startYear=1990, endYear=2023):
        year = random.randint(startYear, endYear)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        return date(year, month, day)

    def _randomDateTime(self, daysAhead=30):
        base = datetime.now()
        delta = timedelta(days=random.randint(0, daysAhead))
        return base + delta

    def _ensureEmailsLoaded(self, cur):
        if len(self.usedEmails) == 0:
            cur.execute("SELECT email FROM " + self._getTableName("viewer"))
            rows = cur.fetchall()
            for r in rows:
                val = r[0]
                if isinstance(val, str):
                    self.usedEmails.add(val.lower())

    def generateViewers(self, count):
        with getDbConnection() as (conn, cur):
            self._ensureEmailsLoaded(cur)
            first_names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Helen"]
            last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
            for i in range(count):
                first_name = random.choice(first_names)
                last_name = random.choice(last_names) + str(i)
                email = self._randomEmail()
                phone_number = self._randomPhone()
                cur.execute(f"""
                    INSERT INTO {self._getTableName('viewer')} (first_name, last_name, email, phone_number) 
                    VALUES (%s, %s, %s, %s)
                """, (first_name, last_name, email, phone_number))

    def generateMovies(self, count):
        with getDbConnection() as (conn, cur):
            genres = ["Action", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi"]
            ratings = ["G", "PG", "PG-13", "R", "NC-17"]
            for i in range(count):
                title = f"Movie {i} " + self._randomString(5, True)
                genre = random.choice(genres)
                duration_minutes = random.choice([90, 120, 150, 180])
                release_date = self._randomDate(2000, 2023)
                rating = random.choice(ratings)
                age_restriction = random.choice([0, 13, 16, 18])
                cur.execute(f"""
                    INSERT INTO {self._getTableName('movie')} 
                    (title, genre, duration_minutes, release_date, rating, age_restriction) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (title, genre, duration_minutes, release_date, rating, age_restriction))

    def generateCinemas(self, count):
        with getDbConnection() as (conn, cur):
            cities = ["Moscow", "St.Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan"]
            for i in range(count):
                name = f"Cinema {i} " + self._randomString(4, True)
                address = f"Address {i} " + self._randomString(6)
                phone_number = self._randomPhone()
                city = random.choice(cities)
                cur.execute(f"""
                    INSERT INTO {self._getTableName('cinema')} (name, address, phone_number, city) 
                    VALUES (%s, %s, %s, %s)
                """, (name, address, phone_number, city))

    def generateHalls(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT cinema_id FROM {self._getTableName('cinema')}")
            cinemas = [row[0] for row in cur.fetchall()]
            if not cinemas:
                self.generateCinemas(1)
                cur.execute(f"SELECT cinema_id FROM {self._getTableName('cinema')}")
                cinemas = [row[0] for row in cur.fetchall()]

            for i in range(count):
                cinema_id = random.choice(cinemas)
                hall_name = f"Hall {i+1}"
                seat_count = random.choice([50, 100, 150, 200])
                base_ticket_price = Decimal(str(random.uniform(200.0, 800.0)))
                cur.execute(f"""
                    INSERT INTO {self._getTableName('hall')} (cinema_id, hall_name, seat_count, base_ticket_price) 
                    VALUES (%s, %s, %s, %s)
                """, (cinema_id, hall_name, seat_count, base_ticket_price))

                cur.execute(f"SELECT hall_id FROM {self._getTableName('hall')} WHERE cinema_id = %s ORDER BY hall_id DESC LIMIT 1", (cinema_id,))
                result = cur.fetchone()
                if result:
                    self.generatedHallIds.append(result[0])

    def generateSessions(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
            movies = [row[0] for row in cur.fetchall()]

            if self.generatedHallIds:
                halls = self.generatedHallIds
            else:
                cur.execute(f"SELECT hall_id FROM {self._getTableName('hall')}")
                halls = [row[0] for row in cur.fetchall()]

            if not movies:
                self.generateMovies(5)
                cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
                movies = [row[0] for row in cur.fetchall()]

            if not halls:
                self.generateHalls(3)
                halls = self.generatedHallIds

            for i in range(count):
                movie_id = random.choice(movies)
                hall_id = random.choice(halls)
                session_datetime = self._randomDateTime()

                # Получаем seat_count для этого зала
                cur.execute(f"SELECT seat_count FROM {self._getTableName('hall')} WHERE hall_id = %s", (hall_id,))
                seat_count = cur.fetchone()[0]
                available_seats = random.randint(0, seat_count)
                final_price = Decimal(str(random.uniform(200.0, 800.0)))

                cur.execute(f"""
                    INSERT INTO {self._getTableName('session')} 
                    (movie_id, hall_id, session_datetime, available_seats, final_price) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (movie_id, hall_id, session_datetime, available_seats, final_price))

    def generateTickets(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT session_id FROM {self._getTableName('session')}")
            sessions = [row[0] for row in cur.fetchall()]
            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
            viewers = [row[0] for row in cur.fetchall()]

            if not sessions:
                self.generateSessions(5)
                cur.execute(f"SELECT session_id FROM {self._getTableName('session')}")
                sessions = [row[0] for row in cur.fetchall()]

            if not viewers:
                self.generateViewers(10)
                cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
                viewers = [row[0] for row in cur.fetchall()]

            added = 0
            attempts = 0
            max_attempts = count * 3

            while added < count and attempts < max_attempts:
                session_id = random.choice(sessions)
                viewer_id = random.choice(viewers)
                attempts += 1

                # Проверяем уникальность пары session_id, viewer_id
                cur.execute(f"""
                    SELECT COUNT(*) FROM {self._getTableName('ticket')} 
                    WHERE session_id = %s AND viewer_id = %s
                """, (session_id, viewer_id))

                if cur.fetchone()[0] == 0:
                    cur.execute(f"""
                        INSERT INTO {self._getTableName('ticket')} 
                        (session_id, viewer_id) VALUES (%s, %s)
                    """, (session_id, viewer_id))
                    added += 1

    def generateMovieReviews(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
            movies = [row[0] for row in cur.fetchall()]
            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
            viewers = [row[0] for row in cur.fetchall()]

            if not movies:
                self.generateMovies(5)
                cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
                movies = [row[0] for row in cur.fetchall()]

            if not viewers:
                self.generateViewers(10)
                cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
                viewers = [row[0] for row in cur.fetchall()]

            reviews = ["Great movie!", "Not bad", "Amazing!", "Could be better", "Fantastic!"]
            for i in range(count):
                movie_id = random.choice(movies)
                viewer_id = random.choice(viewers)
                rating = random.randint(1, 10)
                comment = random.choice(reviews) + " " + self._randomString(8, True)
                cur.execute(f"""
                    INSERT INTO {self._getTableName('movie_review')} 
                    (movie_id, viewer_id, rating, comment) 
                    VALUES (%s, %s, %s, %s)
                """, (movie_id, viewer_id, rating, comment))

    def generateFavoriteMovies(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
            movies = [row[0] for row in cur.fetchall()]
            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
            viewers = [row[0] for row in cur.fetchall()]

            if not movies:
                self.generateMovies(5)
                cur.execute(f"SELECT movie_id FROM {self._getTableName('movie')}")
                movies = [row[0] for row in cur.fetchall()]

            if not viewers:
                self.generateViewers(10)
                cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
                viewers = [row[0] for row in cur.fetchall()]

            added = 0
            attempts = 0
            max_attempts = count * 3

            while added < count and attempts < max_attempts:
                movie_id = random.choice(movies)
                viewer_id = random.choice(viewers)
                attempts += 1

                cur.execute(f"""
                    SELECT COUNT(*) FROM {self._getTableName('favorite_movies')} 
                    WHERE movie_id = %s AND viewer_id = %s
                """, (movie_id, viewer_id))

                if cur.fetchone()[0] == 0:
                    cur.execute(f"""
                        INSERT INTO {self._getTableName('favorite_movies')} 
                        (movie_id, viewer_id) VALUES (%s, %s)
                    """, (movie_id, viewer_id))
                    added += 1

    def generateViewerProfiles(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
            viewers = [row[0] for row in cur.fetchall()]

            if not viewers:
                self.generateViewers(count)
                cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
                viewers = [row[0] for row in cur.fetchall()]

            for i, viewer_id in enumerate(viewers[:count]):
                # Проверяем, нет ли уже профиля для этого viewer_id
                cur.execute(f"SELECT COUNT(*) FROM {self._getTableName('viewer_profile')} WHERE viewer_id = %s", (viewer_id,))
                if cur.fetchone()[0] > 0:
                    continue

                male_gender = random.choice([True, False])
                nickname = self._randomString(10, True)
                birth_date = self._randomDate(1950, 2005)
                cur.execute(f"""
                    INSERT INTO {self._getTableName('viewer_profile')} 
                    (viewer_id, male_gender, nickname, birth_date) 
                    VALUES (%s, %s, %s, %s)
                """, (viewer_id, male_gender, nickname, birth_date))

    def _generateMovieData(self):
        genres = ["Action", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi"]
        ratings = ["G", "PG", "PG-13", "R", "NC-17"]
        return {
            'title': f"Movie {random.randint(1, 1000)} " + self._randomString(5, True),
            'genre': random.choice(genres),
            'duration_minutes': random.choice([90, 120, 150, 180]),
            'release_date': self._randomDate(2000, 2023),
            'rating': random.choice(ratings),
            'age_restriction': random.choice([0, 13, 16, 18])
        }

    def generateData(self, viewersCount, moviesCount, cinemasCount, hallsPerCinema, sessionsPerHall, favoriteRate, reviewRate, ticketRate):
        with getDbConnection() as (conn, cur):
            self._ensureEmailsLoaded(cur)
            self.generateViewers(viewersCount)
            self.generateMovies(moviesCount)
            self.generateViewerProfiles(viewersCount)
            self.generateCinemas(cinemasCount)
            self.generateHalls(cinemasCount * hallsPerCinema)
            self.generateSessions(len(self.generatedHallIds) * sessionsPerHall)

            if favoriteRate > 0:
                self.generateFavoriteMovies(int(viewersCount * moviesCount * favoriteRate))
            if reviewRate > 0:
                self.generateMovieReviews(int(moviesCount * reviewRate))
            if ticketRate > 0:
                cur.execute(f"SELECT COUNT(*) FROM {self._getTableName('session')}")
                sessionCount = cur.fetchone()[0]
                self.generateTickets(int(sessionCount * ticketRate))

    def generateTable(self, tableName: str, rowsCount: int):
        if rowsCount < 0:
            rowsCount = 0

        if tableName == 'viewer':
            self.generateViewers(rowsCount)
        elif tableName == 'movie':
            self.generateMovies(rowsCount)
        elif tableName == 'cinema':
            self.generateCinemas(rowsCount)
        elif tableName == 'hall':
            self.generateHalls(rowsCount)
        elif tableName == 'session':
            self.generateSessions(rowsCount)
        elif tableName == 'ticket':
            self.generateTickets(rowsCount)
        elif tableName == 'movie_review':
            self.generateMovieReviews(rowsCount)
        elif tableName == 'favorite_movies':
            self.generateFavoriteMovies(rowsCount)
        elif tableName == 'viewer_profile':
            self.generateViewerProfiles(rowsCount)
        else:
            raise ValueError(f"Unknown table name: {tableName}")
