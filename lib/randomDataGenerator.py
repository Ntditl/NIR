import random
import string
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from lib.databaseConnection import getDbConnection


UNICODE_EXTRA_CHARS = "阿测试漢字مرحبا"

class RandomDataGenerator:
    def __init__(self):
        self.generatedHallIds = []

    def generateData(
        self,
        viewersCount: int,
        moviesCount: int,
        cinemasCount: int,
        hallsPerCinema: int,
        sessionsPerHall: int,
        favoriteRate: float,
        reviewRate: float,
        ticketRate: float
    ):
        with getDbConnection() as (conn, cur):
            self._generateViewers(cur, viewersCount)
            self._generateMovies(cur, moviesCount)
            self._generateViewerProfiles(cur)
            self._generateCinemasAndHalls(cur, cinemasCount, hallsPerCinema)
            self._generateSessions(cur, sessionsPerHall)
            self._generateFavoriteMovies(cur, favoriteRate)
            self._generateMovieReviews(cur, reviewRate)
            self._generateTickets(cur, ticketRate)

    def _randomString(self, length=10):
        letters = string.ascii_letters + UNICODE_EXTRA_CHARS
        s = []
        i = 0
        while i < length:
            s.append(random.choice(letters))
            i = i + 1
        value = "".join(s)
        if len(value) > 0:
            return value
        return "X"

    def _randomEmail(self):
        namePart = self._randomString(6)
        domainPart = self._randomString(5)
        return namePart.lower() + "@" + domainPart.lower() + ".com"

    def _randomPhone(self):
        country = random.randint(1, 999)
        local = random.randint(1000000, 9999999)
        countryStr = str(country)
        while len(countryStr) < 3:
            countryStr = "0" + countryStr
        localStr = str(local)
        while len(localStr) < 7:
            localStr = "0" + localStr
        return "+" + countryStr + localStr

    def _generateViewers(self, cur, count):
        viewersData = []
        i = 0
        while i < count:
            firstName = self._randomString()
            lastName = self._randomString()
            email = self._randomEmail()
            phone = self._randomPhone()
            viewersData.append((firstName, lastName, email, phone))
            i = i + 1
        cur.executemany(
            "INSERT INTO viewer (first_name, last_name, email, phone_number) VALUES (%s, %s, %s, %s)",
            viewersData
        )

    def _generateMovies(self, cur, count):
        allowedRatings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        moviesData = []
        i = 0
        while i < count:
            year = random.randint(1980, date.today().year)
            dt = date(year, random.randint(1, 12), random.randint(1, 28))
            durationMinutes = random.randint(60, 180)
            ratingIndex = random.randint(0, len(allowedRatings) - 1)
            ratingValue = allowedRatings[ratingIndex]
            ageRestriction = random.choice([0, 6, 12, 16, 18])
            moviesData.append((
                self._randomString(),
                self._randomString(7),
                durationMinutes,
                dt,
                ratingValue,
                ageRestriction
            ))
            i = i + 1
        cur.executemany(
            """
            INSERT INTO movie (title, genre, duration_minutes, release_date, rating, age_restriction)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            moviesData
        )

    def _generateViewerProfiles(self, cur):
        cur.execute("SELECT viewer_id FROM viewer")
        viewerIds = cur.fetchall()
        profilesData = []
        i = 0
        while i < len(viewerIds):
            viewerId = viewerIds[i][0]
            birth = date(
                random.randint(1940, 2015),
                random.randint(1, 12),
                random.randint(1, 28)
            )
            male = random.randint(0, 1) == 1
            nickname = self._randomString()
            profilesData.append((viewerId, male, nickname, birth))
            i = i + 1
        cur.executemany(
            """
            INSERT INTO viewer_profile (viewer_id, male_gender, nickname, birth_date)
            VALUES (%s, %s, %s, %s)
            """,
            profilesData
        )

    def _generateCinemasAndHalls(self, cur, cinemasCount, hallsPerCinema):
        hallIds = []
        i = 0
        while i < cinemasCount:
            cur.execute(
                """
                INSERT INTO cinema (name, address, phone_number, city)
                VALUES (%s, %s, %s, %s) RETURNING cinema_id
                """,
                (
                    self._randomString(),
                    self._randomString(),
                    self._randomPhone(),
                    self._randomString()
                )
            )
            cinemaId = cur.fetchone()[0]
            j = 0
            while j < hallsPerCinema:
                basePriceCents = random.randint(500, 2000)
                basePrice = Decimal(basePriceCents) / Decimal(100)
                cur.execute(
                    """
                    INSERT INTO hall (cinema_id, hall_name, seat_count, base_ticket_price)
                    VALUES (%s, %s, %s, %s) RETURNING hall_id
                    """,
                    (
                        cinemaId,
                        self._randomString(),
                        random.randint(50, 200),
                        basePrice
                    )
                )
                hallIds.append(cur.fetchone()[0])
                j = j + 1
            i = i + 1
        self.generatedHallIds = hallIds

    def _generateSessions(self, cur, sessionsPerHall):
        cur.execute("SELECT movie_id FROM movie")
        movieIds = cur.fetchall()
        sessionsData = []
        i = 0
        while i < len(getattr(self, 'generatedHallIds', [])):
            hallId = self.generatedHallIds[i]
            j = 0
            while j < sessionsPerHall:
                dt = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
                available = random.randint(0, 150)
                priceCents = random.randint(500, 2000)
                finalPrice = Decimal(priceCents) / Decimal(100)
                movieIndex = random.randint(0, len(movieIds) - 1)
                movieId = movieIds[movieIndex][0]
                sessionsData.append((
                    movieId,
                    hallId,
                    dt,
                    available,
                    finalPrice
                ))
                j = j + 1
            i = i + 1
        if len(sessionsData) > 0:
            cur.executemany(
                """
                INSERT INTO session (movie_id, hall_id, session_datetime, available_seats, final_price)
                VALUES (%s, %s, %s, %s, %s)
                """,
                sessionsData
            )

    def _generateFavoriteMovies(self, cur, rate):
        cur.execute("SELECT viewer_id FROM viewer")
        viewerIds = cur.fetchall()
        cur.execute("SELECT movie_id FROM movie")
        movieIds = cur.fetchall()
        favoritesData = []
        i = 0
        while i < len(viewerIds):
            vid = viewerIds[i][0]
            j = 0
            while j < len(movieIds):
                mid = movieIds[j][0]
                r = random.random()
                if r < rate:
                    favoritesData.append((vid, mid))
                j = j + 1
            i = i + 1
        if len(favoritesData) > 0:
            cur.executemany(
                "INSERT INTO favorite_movies (viewer_id, movie_id) VALUES (%s, %s)",
                favoritesData
            )

    def _generateMovieReviews(self, cur, rate):
        cur.execute("SELECT viewer_id FROM viewer")
        viewerIds = cur.fetchall()
        cur.execute("SELECT movie_id FROM movie")
        movieIds = cur.fetchall()
        reviewsData = []
        i = 0
        while i < len(viewerIds):
            vid = viewerIds[i][0]
            j = 0
            while j < len(movieIds):
                mid = movieIds[j][0]
                r = random.random()
                if r < rate:
                    reviewsData.append((
                        mid,
                        vid,
                        random.randint(1, 10),
                        self._randomString(20)
                    ))
                j = j + 1
            i = i + 1
        if len(reviewsData) > 0:
            cur.executemany(
                """
                INSERT INTO movie_review (movie_id, viewer_id, rating, comment)
                VALUES (%s, %s, %s, %s)
                """,
                reviewsData
            )

    def _generateTickets(self, cur, rate):
        cur.execute("SELECT viewer_id FROM viewer")
        viewerIds = cur.fetchall()
        cur.execute("SELECT session_id, available_seats FROM session")
        sessionRows = cur.fetchall()
        pairs = []
        i = 0
        while i < len(sessionRows):
            sid = sessionRows[i][0]
            available = sessionRows[i][1]
            if available > 0:
                numToSell = int(len(viewerIds) * rate)
                if numToSell > available:
                    numToSell = available
                if numToSell > len(viewerIds):
                    numToSell = len(viewerIds)
                if numToSell > 0:
                    chosen = set()
                    k = 0
                    while k < numToSell:
                        idx = random.randint(0, len(viewerIds) - 1)
                        vid = viewerIds[idx][0]
                        if (sid, vid) not in chosen:
                            chosen.add((sid, vid))
                            pairs.append((sid, vid))
                            k = k + 1
            i = i + 1
        if len(pairs) > 0:
            cur.executemany(
                "INSERT INTO ticket (session_id, viewer_id) VALUES (%s, %s)",
                pairs
            )
