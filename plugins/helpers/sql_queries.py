class SqlQueries:
    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        firstName text,
        gender text,
        itemInSession int NOT NULL,
        lastName text,
        length real,
        level text NOT NULL,
        location text,
        method text,
        page text,
        registration real,
        sessionId int NOT NULL,
        song text,
        status int,
        ts bigint,
        userAgent text,
        userId int,
        PRIMARY KEY(sessionId, itemInSession)
        )
        """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id text NOT NULL,
        artist_latitude real,
        artist_longitude real,
        artist_location text,
        artist_name text,
        song_id text NOT NULL,
        title text,
        duration real,
        year int,
        PRIMARY KEY(song_id)
        )
    """)
    
    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id text NOT NULL, 
            start_time timestamp NOT NULL,
            user_id int NOT NULL,
            level text,
            song_id text,
            artist_id text,
            session_id int,
            location text,
            user_agent text
        )
    """)
    
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


    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id int PRIMARY KEY,
            first_name text,
            last_name text,
            gender text,
            level text
    )
    """)
    
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id text PRIMARY KEY,
            title text,
            artist_id text NOT NULL,
            year int,
            duration real
        )
    """)
    
    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id text PRIMARY KEY,
            name text,
            location text,
            latitude real,
            longitude real
    )
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)
    
    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time timestamp PRIMARY KEY,
            hour int,
            day int,
            week int,
            month int,
            year int,
            weekday int
        )
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
