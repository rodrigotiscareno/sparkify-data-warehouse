import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs int,
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration double precision,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id int,
    location varchar,
    user_agent varchar   
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int, 
    duration double precision
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude double precision,
    longitude double precision
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time timestamp PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL, 
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT as json {}
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT as json 'auto'
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    
)
SELECT DISTINCT
     TIMESTAMP 'epoch' + (events.ts/1000) * INTERVAL '1 second' as start_time,
     events.userId AS user_id,
     events.level,
     songs.song_id,
     songs.artist_id,
     events.sessionId AS session_id,
     events.location,
     events.userAgent AS user_agent
FROM staging_songs songs
JOIN staging_events events 
ON 
    (songs.title = events.song
AND 
    songs.artist_name = events.artist)
WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT 
    events.userId as user_id,
    events.firstName as first_name,
    events.lastName as last_name,
    events.gender,
    events.level
FROM staging_events events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs
(
    song_id,
    title,
    artist_id,
    year,
    duration)
SELECT DISTINCT 
    songs.song_id,
    songs.title,
    songs.artist_id,
    songs.year,
    songs.duration
FROM staging_songs songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    songs.artist_id,
    songs.artist_name AS name,
    songs.artist_location AS location,
    songs.artist_latitude AS latitude,
    songs.artist_longitude AS longitude
FROM staging_songs songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time
(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (events.ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year,
    EXTRACT(weekday from start_time) AS weekday
FROM staging_events events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
