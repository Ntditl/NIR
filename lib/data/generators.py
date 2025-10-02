import random
import string
from datetime import date, datetime, timedelta, timezone
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
            self._ensureEmailsLoaded(cur)
            self._generateViewers(cur, viewersCount)
            self._generateMovies(cur, moviesCount)
            self._generateViewerProfiles(cur)
            self._generateCinemasAndHalls(cur, cinemasCount, hallsPerCinema)
            self._generateSessions(cur, sessionsPerHall)
            self._generateFavoriteMovies(cur, favoriteRate)
            self._generateMovieReviews(cur, reviewRate)
            self._generateTickets(cur, ticketRate)

    def _ensureEmailsLoaded(self, cur):
        if len(self.usedEmails) == 0:
            cur.execute("SELECT email FROM " + self._getTableName("viewer"))
            rows = cur.fetchall()
            for r in rows:
                val = r[0]
                if isinstance(val, str):
                    self.usedEmails.add(val.lower())

    def generateTable(self, tableName: str, rowsCount: int):
        if rowsCount < 0:
            rowsCount = 0
        with getDbConnection() as (conn, cur):
            if tableName == 'viewer':
                self._ensureEmailsLoaded(cur)
                self._generateViewers(cur, rowsCount)
                return
            if tableName == 'movie':
                self._generateMovies(cur, rowsCount)
                return
            if tableName == 'cinema':
                self._generateCinemas(cur, rowsCount)
                return
            if tableName == 'hall':
                cur.execute("SELECT cinema_id FROM " + self._getTableName("cinema"))
                cinemaRows = cur.fetchall()
                if len(cinemaRows) == 0:
                    self._generateCinemas(cur, 1)
                    cur.execute("SELECT cinema_id FROM " + self._getTableName("cinema"))
                    cinemaRows = cur.fetchall()
                self._generateHalls(cur, rowsCount, [r[0] for r in cinemaRows])
                return
            if tableName == 'viewer_profile':
                cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                viewerRows = cur.fetchall()
                if len(viewerRows) == 0:
                    self._generateViewers(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                    viewerRows = cur.fetchall()
                cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer_profile"))
                existing = set([r[0] for r in cur.fetchall()])
                toInsert = []
                added = 0
                for vr in viewerRows:
                    if added >= rowsCount:
                        break
                    vid = vr[0]
                    if vid in existing:
                        continue
                    birth = date(random.randint(1940, 2015), random.randint(1, 12), random.randint(1, 28))
                    male = random.randint(0, 1) == 1
                    nickname = self._randomString()
                    toInsert.append((vid, male, nickname, birth))
                    added = added + 1
                if len(toInsert) > 0:
                    cur.executemany(
                        "INSERT INTO " + self._getTableName("viewer_profile") + " (viewer_id, male_gender, nickname, birth_date) VALUES (%s, %s, %s, %s)",
                        toInsert
                    )
                return
            if tableName == 'favorite_movies':
                cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                viewers = [r[0] for r in cur.fetchall()]
                if len(viewers) == 0:
                    self._generateViewers(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                    viewers = [r[0] for r in cur.fetchall()]
                cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                movies = [r[0] for r in cur.fetchall()]
                if len(movies) == 0:
                    self._generateMovies(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                    movies = [r[0] for r in cur.fetchall()]
                cur.execute("SELECT viewer_id, movie_id FROM " + self._getTableName("favorite_movies"))
                existingPairs = set([(r[0], r[1]) for r in cur.fetchall()])
                newRows = []
                attempts = 0
                maxAttempts = rowsCount * 10 if rowsCount > 0 else 0
                while len(newRows) < rowsCount and attempts < maxAttempts:
                    vid = random.choice(viewers)
                    mid = random.choice(movies)
                    pair = (vid, mid)
                    if pair not in existingPairs:
                        existingPairs.add(pair)
                        newRows.append(pair)
                    attempts = attempts + 1
                if len(newRows) > 0:
                    cur.executemany("INSERT INTO " + self._getTableName("favorite_movies") + " (viewer_id, movie_id) VALUES (%s, %s)", newRows)
                return
            if tableName == 'movie_review':
                cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                viewers = [r[0] for r in cur.fetchall()]
                if len(viewers) == 0:
                    self._generateViewers(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                    viewers = [r[0] for r in cur.fetchall()]
                cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                movies = [r[0] for r in cur.fetchall()]
                if len(movies) == 0:
                    self._generateMovies(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                    movies = [r[0] for r in cur.fetchall()]
                newRows = []
                countAdded = 0
                while countAdded < rowsCount:
                    vid = random.choice(viewers)
                    mid = random.choice(movies)
                    ratingVal = random.randint(1, 10)
                    commentVal = self._randomString(20)
                    newRows.append((mid, vid, ratingVal, commentVal))
                    countAdded = countAdded + 1
                if len(newRows) > 0:
                    cur.executemany(
                        "INSERT INTO " + self._getTableName("movie_review") + " (movie_id, viewer_id, rating, comment) VALUES (%s, %s, %s, %s)",
                        newRows
                    )
                return
            if tableName == 'session':
                cur.execute("SELECT hall_id, seat_count FROM " + self._getTableName("hall"))
                hallRows = cur.fetchall()
                if len(hallRows) == 0:
                    self._generateCinemasAndHalls(cur, 1, 1)
                    cur.execute("SELECT hall_id, seat_count FROM " + self._getTableName("hall"))
                    hallRows = cur.fetchall()
                cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                movieRows = cur.fetchall()
                if len(movieRows) == 0:
                    self._generateMovies(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
                    movieRows = cur.fetchall()
                sessionsData = []
                for indexSession in range(rowsCount):
                    hallId, seatCount = random.choice(hallRows)
                    if seatCount < 0:
                        seatCount = 0
                    dtVal = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
                    availableSeats = random.randint(0, seatCount)
                    priceCents = random.randint(500, 2000)
                    finalPriceVal = Decimal(priceCents) / Decimal(100)
                    movieId = random.choice(movieRows)[0]
                    sessionsData.append((movieId, hallId, dtVal, availableSeats, finalPriceVal))
                if len(sessionsData) > 0:
                    cur.executemany(
                        "INSERT INTO " + self._getTableName("session") + " (movie_id, hall_id, session_datetime, available_seats, final_price) VALUES (%s, %s, %s, %s, %s)",
                        sessionsData
                    )
                return
            if tableName == 'ticket':
                cur.execute("SELECT session_id, available_seats FROM " + self._getTableName("session"))
                sessionRows = cur.fetchall()
                if len(sessionRows) == 0:
                    self._generateCinemasAndHalls(cur, 1, 1)
                    self._generateMovies(cur, 1)
                    self._generateSessions(cur, 1)
                    cur.execute("SELECT session_id, available_seats FROM " + self._getTableName("session"))
                    sessionRows = cur.fetchall()
                cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                viewerRows = cur.fetchall()
                if len(viewerRows) == 0:
                    self._generateViewers(cur, rowsCount if rowsCount > 0 else 1)
                    cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
                    viewerRows = cur.fetchall()
                cur.execute("SELECT session_id, viewer_id FROM " + self._getTableName("ticket"))
                existingTickets = set([(r[0], r[1]) for r in cur.fetchall()])
                ticketsToInsert = []
                attempts = 0
                maxAttempts = rowsCount * 20 if rowsCount > 0 else 0
                while len(ticketsToInsert) < rowsCount and attempts < maxAttempts:
                    sessionId, availableSeats = random.choice(sessionRows)
                    if availableSeats <= 0:
                        attempts = attempts + 1
                        continue
                    viewerId = random.choice(viewerRows)[0]
                    keyPair = (sessionId, viewerId)
                    if keyPair in existingTickets:
                        attempts = attempts + 1
                        continue
                    ticketsToInsert.append((sessionId, viewerId))
                    existingTickets.add(keyPair)
                    attempts = attempts + 1
                if len(ticketsToInsert) > 0:
                    cur.executemany("INSERT INTO " + self._getTableName("ticket") + " (session_id, viewer_id) VALUES (%s, %s)", ticketsToInsert)
                return
            raise ValueError('Unsupported table for direct generation: ' + tableName)

    def buildViewers(self, n: int):
        data = []
        for i in range(n):
            emailValue = self._uniqueEmail()
            data.append((self._randomString(), self._randomString(), emailValue, self._randomPhone()))
        return data

    def buildMovies(self, n: int):
        allowedRatings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        data = []
        for i in range(n):
            year = random.randint(1980, date.today().year)
            dt = date(year, random.randint(1, 12), random.randint(1, 28))
            durationMinutes = random.randint(60, 180)
            ratingValue = allowedRatings[random.randint(0, len(allowedRatings) - 1)]
            ageRestriction = random.choice([0, 6, 12, 16, 18])
            data.append((
                self._randomString(),
                self._randomString(7),
                durationMinutes,
                dt,
                ratingValue,
                ageRestriction
            ))
        return data

    def buildCinemas(self, n: int):
        data = []
        for i in range(n):
            data.append((self._randomString(), self._randomString(), self._randomPhone(), self._randomString()))
        return data

    def buildHalls(self, n: int, cinemaIds):
        rows = []
        for i in range(n):
            basePriceCents = random.randint(500, 2000)
            basePrice = Decimal(basePriceCents) / Decimal(100)
            cid = cinemaIds[i % len(cinemaIds)]
            rows.append((cid, self._randomString(), random.randint(50, 200), basePrice))
        return rows

    def buildSessions(self, n: int, hallIds, movieIds):
        rows = []
        for i in range(n):
            hallId = hallIds[i % len(hallIds)]
            movieId = movieIds[i % len(movieIds)]
            dt = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
            curSeats = random.randint(0, 200)
            priceCents = random.randint(500, 2000)
            finalPrice = Decimal(priceCents) / Decimal(100)
            rows.append((movieId, hallId, dt, curSeats, finalPrice))
        return rows

    def buildFavoritePairs(self, rate: float, viewerIds, movieIds):
        pairs = []
        for vid in viewerIds:
            for mid in movieIds:
                if random.random() < rate:
                    pairs.append((vid, mid))
        return pairs

    def buildReviews(self, rate: float, viewerIds, movieIds):
        rows = []
        for vid in viewerIds:
            for mid in movieIds:
                if random.random() < rate:
                    rows.append((mid, vid, random.randint(1, 10), self._randomString(20)))
        return rows

    def buildTickets(self, rate: float, viewerIds, sessionIds, capacityMap):
        rows = []
        for sid in sessionIds:
            available = capacityMap.get(sid, 0)
            numToSell = int(len(viewerIds) * rate)
            if numToSell > available:
                numToSell = available
            if numToSell > len(viewerIds):
                numToSell = len(viewerIds)
            if numToSell <= 0:
                continue
            chosen = set()
            for k in range(numToSell):
                attempts = 0
                while attempts < numToSell * 2:
                    idx = random.randint(0, len(viewerIds) - 1)
                    vid = viewerIds[idx]
                    key = (sid, vid)
                    if key not in chosen:
                        chosen.add(key)
                        rows.append((sid, vid))
                        break
                    attempts = attempts + 1
        return rows

    def _randomString(self, length=10):
        letters = string.ascii_letters + UNICODE_EXTRA_CHARS
        s = []
        for i in range(length):
            s.append(random.choice(letters))
        value = "".join(s)
        if len(value) > 0:
            return value
        return "X"

    def _randomEmail(self):
        return self._uniqueEmail()

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

    def _uniqueEmail(self):
        attempts = 0
        while attempts < EMAIL_MAX_ATTEMPTS:
            namePart = self._randomString(EMAIL_NAME_LEN).lower()
            domainPart = self._randomString(EMAIL_DOMAIN_LEN).lower()
            emailValue = namePart + '@' + domainPart + '.com'
            lowered = emailValue.lower()
            if lowered not in self.usedEmails:
                self.usedEmails.add(lowered)
                return emailValue
            attempts = attempts + 1
        suffix = 0
        baseName = 'user'
        baseDomain = 'mail'
        while True:
            emailValue = baseName + str(suffix) + '@' + baseDomain + '.com'
            lowered = emailValue.lower()
            if lowered not in self.usedEmails:
                self.usedEmails.add(lowered)
                return emailValue
            suffix = suffix + 1

    def _generateViewers(self, cur, count):
        viewersData = []
        for i in range(count):
            firstName = self._randomString()
            lastName = self._randomString()
            email = self._uniqueEmail()
            phone = self._randomPhone()
            viewersData.append((firstName, lastName, email, phone))
        if len(viewersData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("viewer") + " (first_name, last_name, email, phone_number) VALUES (%s, %s, %s, %s)",
                viewersData
            )

    def _generateMovies(self, cur, count):
        allowedRatings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        moviesData = []
        for i in range(count):
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
        if len(moviesData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("movie") + " (title, genre, duration_minutes, release_date, rating, age_restriction) VALUES (%s, %s, %s, %s, %s, %s)",
                moviesData
            )

    def _generateCinemas(self, cur, count):
        cinemaData = []
        for i in range(count):
            cinemaData.append((
                self._randomString(),
                self._randomString(),
                self._randomPhone(),
                self._randomString()
            ))
        if len(cinemaData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("cinema") + " (name, address, phone_number, city) VALUES (%s, %s, %s, %s)",
                cinemaData
            )

    def _generateHalls(self, cur, count, cinemaIds):
        hallsData = []
        if len(cinemaIds) == 0:
            return
        for i in range(count):
            basePriceCents = random.randint(500, 2000)
            basePrice = Decimal(basePriceCents) / Decimal(100)
            cinemaId = cinemaIds[i % len(cinemaIds)]
            hallsData.append((
                cinemaId,
                self._randomString(),
                random.randint(50, 200),
                basePrice
            ))
        if len(hallsData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("hall") + " (cinema_id, hall_name, seat_count, base_ticket_price) VALUES (%s, %s, %s, %s)",
                hallsData
            )

    def _generateViewerProfiles(self, cur):
        cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
        viewerIds = cur.fetchall()
        profilesData = []
        for i in range(len(viewerIds)):
            viewerId = viewerIds[i][0]
            birth = date(
                random.randint(1940, 2015),
                random.randint(1, 12),
                random.randint(1, 28)
            )
            male = random.randint(0, 1) == 1
            nickname = self._randomString()
            profilesData.append((viewerId, male, nickname, birth))
        if len(profilesData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("viewer_profile") + " (viewer_id, male_gender, nickname, birth_date) VALUES (%s, %s, %s, %s)",
                profilesData
            )

    def _generateCinemasAndHalls(self, cur, cinemasCount, hallsPerCinema):
        hallIds = []
        for i in range(cinemasCount):
            cur.execute(
                "INSERT INTO " + self._getTableName("cinema") + " (name, address, phone_number, city) VALUES (%s, %s, %s, %s) RETURNING cinema_id",
                (
                    self._randomString(),
                    self._randomString(),
                    self._randomPhone(),
                    self._randomString()
                )
            )
            cinemaId = cur.fetchone()[0]
            for j in range(hallsPerCinema):
                basePriceCents = random.randint(500, 2000)
                basePrice = Decimal(basePriceCents) / Decimal(100)
                cur.execute(
                    "INSERT INTO " + self._getTableName("hall") + " (cinema_id, hall_name, seat_count, base_ticket_price) VALUES (%s, %s, %s, %s) RETURNING hall_id",
                    (
                        cinemaId,
                        self._randomString(),
                        random.randint(50, 200),
                        basePrice
                    )
                )
                hallIds.append(cur.fetchone()[0])
        self.generatedHallIds = hallIds

    def _generateSessions(self, cur, sessionsPerHall):
        cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
        movieIds = cur.fetchall()
        sessionsData = []
        for i in range(len(getattr(self, 'generatedHallIds', []))):
            hallId = self.generatedHallIds[i]
            cur.execute("SELECT seat_count FROM " + self._getTableName("hall") + " WHERE hall_id = %s", (hallId,))
            seatRow = cur.fetchone()
            if seatRow is None:
                continue
            seatCount = seatRow[0]
            for j in range(sessionsPerHall):
                dt = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
                if seatCount < 0:
                    seatCount = 0
                available = random.randint(0, seatCount)
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
        if len(sessionsData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("session") + " (movie_id, hall_id, session_datetime, available_seats, final_price) VALUES (%s, %s, %s, %s, %s)",
                sessionsData
            )

    def _generateFavoriteMovies(self, cur, rate):
        cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
        viewerIds = cur.fetchall()
        cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
        movieIds = cur.fetchall()
        favoritesData = []
        for i in range(len(viewerIds)):
            vid = viewerIds[i][0]
            for j in range(len(movieIds)):
                mid = movieIds[j][0]
                r = random.random()
                if r < rate:
                    favoritesData.append((vid, mid))
        if len(favoritesData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("favorite_movies") + " (viewer_id, movie_id) VALUES (%s, %s)",
                favoritesData
            )

    def _generateMovieReviews(self, cur, rate):
        cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
        viewerIds = cur.fetchall()
        cur.execute("SELECT movie_id FROM " + self._getTableName("movie"))
        movieIds = cur.fetchall()
        reviewsData = []
        for i in range(len(viewerIds)):
            vid = viewerIds[i][0]
            for j in range(len(movieIds)):
                mid = movieIds[j][0]
                r = random.random()
                if r < rate:
                    reviewsData.append((
                        mid,
                        vid,
                        random.randint(1, 10),
                        self._randomString(20)
                    ))
        if len(reviewsData) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("movie_review") + " (movie_id, viewer_id, rating, comment) VALUES (%s, %s, %s, %s)",
                reviewsData
            )

    def _generateTickets(self, cur, rate):
        cur.execute("SELECT viewer_id FROM " + self._getTableName("viewer"))
        viewerIds = cur.fetchall()
        cur.execute("SELECT session_id, available_seats FROM " + self._getTableName("session"))
        sessionRows = cur.fetchall()
        pairs = []
        for i in range(len(sessionRows)):
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
                    for k in range(numToSell):
                        attempts = 0
                        while attempts < numToSell * 2:
                            idx = random.randint(0, len(viewerIds) - 1)
                            vid = viewerIds[idx][0]
                            if (sid, vid) not in chosen:
                                chosen.add((sid, vid))
                                pairs.append((sid, vid))
                                break
                            attempts = attempts + 1
        if len(pairs) > 0:
            cur.executemany(
                "INSERT INTO " + self._getTableName("ticket") + " (session_id, viewer_id) VALUES (%s, %s)",
                pairs
            )
