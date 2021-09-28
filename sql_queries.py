import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Schemas

create_dw_schema = "CREATE SCHEMA IF NOT EXISTS dw;"
create_stg_schema = "CREATE SCHEMA IF NOT EXISTS stg;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg.events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg.songs;"
songplay_table_drop = "DROP TABLE IF EXISTS dw.songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS dw.dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dw.dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dw.dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dw.dim_time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS stg.events (
    event_id IDENTITY(1,1),
    artist VARCHAR(100),
    auth VARCHAR(50),
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR(25),
    location VARCHAR(100),
    method VARCHAR(10),
    page VARCHAR(50),
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR(100),
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR(100),
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg.songs (
    num_songs integer,
    artist_id VARCHAR(50),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(100),
    artist_name VARCHAR(100),
    song_id VARCHAR(50),
    title VARCHAR(100),
    duration FLOAT,
    year INTEGER
);
""")

songplay_table_create = ("""

CREATE TABLE IF NOT EXISTS dw.songplays (
    songplay_id IDENTITY(1,1) NOT NULL PRIMARY KEY,
    start_time INTEGER,
    user_id INTEGER,
    level VARCHAR(10),
    song_id INTEGER,
    artist_id INTEGER,
    session_id INTEGER,
    location VARCHAR(50),
    user_agent VARCHAR(100)
);


""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS dw.dim_users (
    user_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender CHAR,
    level VARCHAR(10);
);

""")

song_table_create = ("""

CREATE TABLE IF NOT EXISTS dw.dim_songs (
    song_id VARCHAR(50),
    title VARCHAR(100),
    artist_id VARCHAR(50),
    year INTEGER,
    duration INTEGER
);

""")

artist_table_create = ("""

CREATE TABLE IF NOT EXISTS dw.dim_artist (
    artist_id VARCHAR(50),
    name VARCHAR(100),
    location VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

""")

time_table_create = ("""


CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_id IDENTITY(1,1) PRIMARY KEY, 
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);

""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg.events FROM '{}' 
    CREDENTIALS 'aws_iam_role={}'
    gzip region '{}';
""").format()

staging_songs_copy = ("""
    COPY stg.songs FROM '{}' 
    CREDENTIALS 'aws_iam_role={}'
    gzip region '{}';
""").format()

# FINAL TABLES

songplay_table_insert = ("""

INSERT INTO dw.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (?, ?, ?, ?, ?,? ,?, ?);

""")

user_table_insert = ("""

INSERT INTO dw.dim_users (user_id, first_name, last_name, gender, level)
        VALUES (?, ?, ?, ?, ?,);
""")

song_table_insert = ("""

INSERT INTO dw.dim_songs (song_id, title, artist_id, year, duration)
    VALUES (?, ?, ?, ?, ?);
""")

artist_table_insert = ("""

INSERT INTO dw.dim_artist (artist_id, name, location, latitude, longitude)
    VALUES (?, ?, ?, ?, ?);
""")

time_table_insert = ("""

INSERT INTO dw.dim_time (start_time, hour, day, week,month,year,weekday)
    VALUES (?, ?, ?, ?, ?,? ,?);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
