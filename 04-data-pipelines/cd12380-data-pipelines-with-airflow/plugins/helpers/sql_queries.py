class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    staging_events_table_create = ("""
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
        artist_id               VARCHAR(18)             NOT NULL SORTKEY,
        artist_latitude         VARCHAR(255)            NULL,
        artist_longitude        VARCHAR(255)            NULL,
        artist_location         VARCHAR(255)            NULL,
        artist_name             VARCHAR(255)            NULL,
        song_id                 VARCHAR(18)             NOT NULL DISTKEY,
        title                   VARCHAR(255)            NULL,
        duration                FLOAT                   NULL,
        year                    INTEGER                 NULL
      );
    """)