import random
import string
from datetime import date, datetime, timedelta
from lib.db.connection import getDbConnection

UNICODE_EXTRA_CHARS = "阿测试漢字مرحبا"
EMAIL_NAME_LEN = 6
EMAIL_DOMAIN_LEN = 5
EMAIL_MAX_ATTEMPTS = 50

class RandomDataGenerator:
    def __init__(self):
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
                birth_date = self._randomDate(1970, 2005)
                cur.execute(f"""
                    INSERT INTO {self._getTableName('viewer')} (first_name, last_name, email, birth_date) 
                    VALUES (%s, %s, %s, %s)
                """, (first_name, last_name, email, birth_date))

    def generateViewerProfiles(self, count):
        with getDbConnection() as (conn, cur):
            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
            viewers = [row[0] for row in cur.fetchall()]

            if not viewers:
                self.generateViewers(count)
                cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer')}")
                viewers = [row[0] for row in cur.fetchall()]

            cur.execute(f"SELECT viewer_id FROM {self._getTableName('viewer_profile')}")
            existing_profiles = set(row[0] for row in cur.fetchall())

            themes = ["dark", "light", "blue", "green", "red"]
            added = 0
            for viewer_id in viewers:
                if added >= count:
                    break
                if viewer_id not in existing_profiles:
                    nickname = f"user_{viewer_id}_{self._randomString(4)}"
                    avatar_path = f"/avatars/{viewer_id}.jpg"
                    theme = random.choice(themes)
                    registration_date = self._randomDate(2015, 2023)
                    cur.execute(f"""
                        INSERT INTO {self._getTableName('viewer_profile')} 
                        (viewer_id, nickname, avatar_path, theme, registration_date) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (viewer_id, nickname, avatar_path, theme, registration_date))
                    added += 1

    def generateMovies(self, count):
        with getDbConnection() as (conn, cur):
            genres = ["Action", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi"]
            movie_titles = [
                "The Dark Knight", "Inception", "Pulp Fiction", "The Matrix",
                "Fight Club", "Interstellar", "The Godfather", "Forrest Gump",
                "The Shawshank Redemption", "Goodfellas", "Star Wars", "Gladiator"
            ]
            for i in range(count):
                base_title = random.choice(movie_titles)
                title = f"{base_title} {i}" if i > 0 else base_title
                genre = random.choice(genres)
                duration_minutes = random.randint(90, 180)
                release_date = self._randomDate(2000, 2023)
                cur.execute(f"""
                    INSERT INTO {self._getTableName('movie')} 
                    (title, genre, duration_minutes, release_date) 
                    VALUES (%s, %s, %s, %s)
                """, (title, genre, duration_minutes, release_date))

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
                viewer_id = random.choice(viewers)
                movie_id = random.choice(movies)
                attempts += 1

                cur.execute(f"""
                    SELECT COUNT(*) FROM {self._getTableName('favorite_movies')} 
                    WHERE viewer_id = %s AND movie_id = %s
                """, (viewer_id, movie_id))

                if cur.fetchone()[0] == 0:
                    added_date = self._randomDate(2020, 2023)
                    cur.execute(f"""
                        INSERT INTO {self._getTableName('favorite_movies')} 
                        (viewer_id, movie_id, added_date) VALUES (%s, %s, %s)
                    """, (viewer_id, movie_id, added_date))
                    added += 1
