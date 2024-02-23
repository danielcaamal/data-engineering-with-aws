import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
    event_id                BIGINT IDENTITY(0,1)    NOT NULL,
    artist                  VARCHAR(255)            NULL,
    auth                    VARCHAR(255)            NULL,
    firstName               VARCHAR(255)            NULL,
    gender                  VARCHAR(1)              NULL,
    itemInSession           INTEGER                 NULL,
    lastName                VARCHAR(255)            NULL,
    length                  FLOAT                   NULL,
    level                   VARCHAR(255)            NULL,
    location                VARCHAR(255)            NULL,
    method                  VARCHAR(255)            NULL,
    page                    VARCHAR(255)            NULL,
    registration            FLOAT                   NULL,
    sessionId               INTEGER                 NULL,
    song                    VARCHAR(255)            NULL,
    status                  INTEGER                 NULL,
    ts                      BIGINT                  NULL,
    userAgent               VARCHAR(255)            NULL,
    userId                  INTEGER                 NULL
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs               INTEGER                 NULL,
    artist_id               VARCHAR(18)             NOT NULL SORTKEY DISTKEY,
    artist_latitude         VARCHAR(255)            NULL,
    artist_longitude        VARCHAR(255)            NULL,
    artist_location         VARCHAR(255)            NULL,
    artist_name             VARCHAR(255)            NULL,
    song_id                 VARCHAR(18)             NOT NULL,
    title                   VARCHAR(255)            NULL,
    duration                FLOAT                   NULL,
    year                    INTEGER                 NULL
  );
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id             INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
    start_time              TIMESTAMP               NOT NULL,
    user_id                 INTEGER                 NOT NULL,
    level                   VARCHAR(255)            NULL,
    song_id                 VARCHAR(18)             NOT NULL,
    artist_id               VARCHAR(18)             NOT NULL,
    session_id              INTEGER                 NOT NULL,
    location                VARCHAR(255)            NULL,
    user_agent              VARCHAR(255)            NULL
  );
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id                 INTEGER                 NOT NULL SORTKEY,
    first_name              VARCHAR(255)            NULL,
    last_name               VARCHAR(255)            NULL,
    gender                  VARCHAR(1)              NULL,
    level                   VARCHAR(255)            NULL
  );
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id                 VARCHAR(18)             NOT NULL SORTKEY,
    title                   VARCHAR(255)            NULL,
    artist_id               VARCHAR(18)             NOT NULL,
    year                    INTEGER                 NULL,
    duration                FLOAT                   NULL
  );
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id               VARCHAR(18)             NOT NULL SORTKEY,
    name                    VARCHAR(255)            NULL,
    location                VARCHAR(255)            NULL,
    latitude                VARCHAR(255)            NULL,
    longitude               VARCHAR(255)            NULL
  );
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time              TIMESTAMP PRIMARY KEY   NOT NULL SORTKEY,
    hour                    INTEGER                 NULL,
    day                     INTEGER                 NULL,
    week                    INTEGER                 NULL,
    month                   INTEGER                 NULL,
    year                    INTEGER                 NULL,
    weekday                 INTEGER                 NULL
  );
""")

# STAGING TABLES

staging_events_copy = ("""
  COPY staging_events
  FROM {}
  CREDENTIALS 'aws_iam_role={}'
  FORMAT AS JSON {}
  REGION {};
""").format(
  config.get('S3', 'LOG_DATA'),
  config.get('IAM_ROLE', 'ARN'),
  config.get('S3', 'LOG_JSONPATH'),
  config.get('S3', 'REGION')
)

staging_songs_copy = ("""
  COPY staging_songs 
  FROM {}
  CREDENTIALS 'aws_iam_role={}'
  FORMAT AS JSON 'auto'
  ACCEPTINVCHARS AS '^'
  STATUPDATE ON
  REGION {};
""").format(
  config.get('S3', 'SONG_DATA'),
  config.get('IAM_ROLE', 'ARN'),
  config.get('S3', 'REGION')
)

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (
    start_time,
    user_id,
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
  ) 
  (
    SELECT
      TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
      se.userId                                                   AS user_id,
      se.level                                                    AS level,
      ss.song_id                                                  AS song_id,
      ss.artist_id                                                AS artist_id,
      se.sessionId                                                AS session_id,
      se.location                                                 AS location,
      se.userAgent                                                AS user_agent
    FROM 
      staging_events se
    JOIN 
      staging_songs ss ON 
        se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
  );
""")

user_table_insert = ("""
  INSERT INTO users (
    user_id, 
    first_name, 
    last_name,
    gender,
    level
  )
  (
    SELECT
      DISTINCT 
        se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender AS gender,
        se.level AS level
    FROM
      staging_events se
    WHERE
      se.page = 'NextSong'
  );
""")

song_table_insert = ("""
  INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
  )
  (
    SELECT
      DISTINCT
        ss.song_id    AS song_id,
        ss.title      AS title,
        ss.artist_id  AS artist_id,
        ss.year       AS year,
        ss.duration   AS duration
    FROM
      staging_songs ss
    WHERE
      ss.song_id IS NOT NULL
  );
""")

artist_table_insert = ("""
  INSERT INTO artists (
    artist_id, 
    name, 
    location,
    latitude, 
    longitude
  )
  (
    SELECT
      DISTINCT
        ss.artist_id        AS artist_id,
        ss.artist_name      AS name,
        ss.artist_location  AS location,
        ss.artist_latitude  AS latitude,
        ss.artist_longitude AS longitude
    FROM
      staging_songs ss
    WHERE
      ss.artist_id IS NOT NULL
  );
""")

time_table_insert = ("""
  INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
  )
  (
    SELECT
      DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
        EXTRACT(HOUR FROM start_time)                         AS hour,
        EXTRACT(DAY FROM start_time)                          AS day,
        EXTRACT(WEEK FROM start_time)                         AS week,
        EXTRACT(MONTH FROM start_time)                        AS month,
        EXTRACT(YEAR FROM start_time)                         AS year,
        EXTRACT(DOW FROM start_time)                          AS weekday
    FROM
      staging_events se
    WHERE
      se.page = 'NextSong' AND se.ts IS NOT NULL
  );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
