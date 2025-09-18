TABLE_NAMES = [
    "viewer", "movie", "viewer_profile", "favorite_movies",
    "movie_review", "cinema", "hall", "session", "ticket"
]

def getCreateTablesSql():
    return [
        """
        CREATE TABLE cinema (
            cinema_id BIGSERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            address VARCHAR(255) NOT NULL,
            phone_number VARCHAR(32) NOT NULL,
            city VARCHAR(100) NOT NULL
        );
        """,
        """
        CREATE TABLE hall (
            hall_id BIGSERIAL PRIMARY KEY,
            cinema_id BIGINT NOT NULL REFERENCES cinema(cinema_id) ON DELETE CASCADE,
            hall_name VARCHAR(100) NOT NULL,
            seat_count INT NOT NULL CHECK (seat_count > 0),
            base_ticket_price NUMERIC(10,2) NOT NULL CHECK (base_ticket_price >= 0)
        );
        """,
        """
        CREATE TABLE movie (
            movie_id BIGSERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            genre VARCHAR(100) NOT NULL,
            duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
            release_date DATE NOT NULL,
            rating VARCHAR(10) NOT NULL,
            age_restriction INT NOT NULL CHECK (age_restriction >= 0)
        );
        """,
        """
        CREATE TABLE session (
            session_id BIGSERIAL PRIMARY KEY,
            movie_id BIGINT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
            hall_id BIGINT NOT NULL REFERENCES hall(hall_id) ON DELETE CASCADE,
            session_datetime TIMESTAMPTZ NOT NULL,
            available_seats INT NOT NULL CHECK (available_seats >= 0),
            final_price NUMERIC(10,2) NOT NULL CHECK (final_price >= 0)
        );
        """,
        """
        CREATE TABLE viewer (
            viewer_id BIGSERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone_number VARCHAR(32) NOT NULL
        );
        """,
        """
        CREATE TABLE viewer_profile (
            profile_id BIGSERIAL PRIMARY KEY,
            viewer_id BIGINT NOT NULL UNIQUE REFERENCES viewer(viewer_id) ON DELETE CASCADE,
            male_gender BOOLEAN NOT NULL,
            nickname VARCHAR(100),
            birth_date DATE
        );
        """,
        """
        CREATE TABLE favorite_movies (
            favorite_id BIGSERIAL PRIMARY KEY,
            viewer_id BIGINT NOT NULL REFERENCES viewer(viewer_id) ON DELETE CASCADE,
            movie_id BIGINT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
            UNIQUE (viewer_id, movie_id)
        );
        """,
        """
        CREATE TABLE movie_review (
            review_id BIGSERIAL PRIMARY KEY,
            movie_id BIGINT NOT NULL REFERENCES movie(movie_id) ON DELETE CASCADE,
            viewer_id BIGINT NOT NULL REFERENCES viewer(viewer_id) ON DELETE CASCADE,
            rating INT NOT NULL CHECK (rating BETWEEN 1 AND 10),
            comment TEXT
        );
        """,
        """
        CREATE TABLE ticket (
            ticket_id BIGSERIAL PRIMARY KEY,
            session_id BIGINT NOT NULL REFERENCES session(session_id) ON DELETE CASCADE,
            viewer_id BIGINT NOT NULL REFERENCES viewer(viewer_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE INDEX ON hall (cinema_id);
        """,
        """
        CREATE INDEX ON session (movie_id);
        """,
        """
        CREATE INDEX ON session (hall_id);
        """,
        """
        CREATE INDEX ON viewer_profile (viewer_id);
        """,
        """
        CREATE INDEX ON favorite_movies (viewer_id);
        """,
        """
        CREATE INDEX ON favorite_movies (movie_id);
        """,
        """
        CREATE INDEX ON movie_review (movie_id);
        """,
        """
        CREATE INDEX ON movie_review (viewer_id);
        """,
        """
        CREATE INDEX ON ticket (session_id);
        """,
        """
        CREATE INDEX ON ticket (viewer_id);
        """,
        """
        CREATE UNIQUE INDEX viewer_email_lower_uq ON viewer (lower(email));
        """,
        """
        ALTER TABLE movie
          ADD CONSTRAINT movie_rating_chk CHECK (rating IN ('G','PG','PG-13','R','NC-17'));
        """,
        """
        CREATE UNIQUE INDEX ticket_session_viewer_uq ON ticket (session_id, viewer_id);
        """,
        """
        CREATE OR REPLACE FUNCTION check_session_capacity() RETURNS trigger AS $$
        DECLARE max_seats INT;
        BEGIN
          SELECT seat_count INTO max_seats FROM hall WHERE hall_id = NEW.hall_id;
          IF NEW.available_seats > max_seats THEN
            RAISE EXCEPTION 'available_seats % exceeds hall seat_count %', NEW.available_seats, max_seats;
          END IF;
          RETURN NEW;
        END $$ LANGUAGE plpgsql;
        """,
        """
        DROP TRIGGER IF EXISTS trg_session_capacity ON session;
        """,
        """
        CREATE TRIGGER trg_session_capacity
        BEFORE INSERT OR UPDATE OF available_seats, hall_id
        ON session
        FOR EACH ROW EXECUTE FUNCTION check_session_capacity();
        """,
        """
        CREATE OR REPLACE FUNCTION reserve_seat_on_ticket() RETURNS trigger AS $$
        BEGIN
          UPDATE session
             SET available_seats = available_seats - 1
           WHERE session_id = NEW.session_id
             AND available_seats > 0;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'No available seats for session %', NEW.session_id;
          END IF;
          RETURN NEW;
        END $$ LANGUAGE plpgsql;
        """,
        """
        DROP TRIGGER IF EXISTS trg_ticket_reserve ON ticket;
        """,
        """
        CREATE TRIGGER trg_ticket_reserve
        BEFORE INSERT ON ticket
        FOR EACH ROW EXECUTE FUNCTION reserve_seat_on_ticket();
        """
    ]

def getDropTablesSql():
    sqlList = []
    index = len(TABLE_NAMES) - 1
    while index >= 0:
        name = TABLE_NAMES[index]
        sqlList.append(f"DROP TABLE IF EXISTS {name} CASCADE;")
        index = index - 1
    return sqlList

def getTableNames():
    return TABLE_NAMES
