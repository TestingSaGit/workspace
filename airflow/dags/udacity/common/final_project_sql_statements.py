class SqlQueries:

    # drop table
    drop_staging_events = "drop table if exists staging_events"
    drop_staging_songs = "drop table if exists staging_songs"
    drop_artists = "drop table if exists artists"
    drop_songs = "drop table if exists songs"
    drop_users = "drop table if exists users"
    drop_songplays = "drop table if exists songplays"
    drop_time = "drop table if exists time"

    # create table statements
    create_staging_log_table = """
        CREATE TABLE if not exists staging_events (
            artist VARCHAR(500),
            auth VARCHAR(15),
            firstName VARCHAR(50),
            gender VARCHAR(5),
            itemInSession INT,
            lastName VARCHAR(256),
            length FLOAT,
            level VARCHAR(20),
            location VARCHAR(60),
            method VARCHAR(5),
            page VARCHAR(30),
            registration FLOAT,
            sessionId INT,
            song VARCHAR(512),
            status INT,
            ts BIGINT,
            userAgent VARCHAR(512),
            userId INT
        )		
    """
	
    create_staging_song_table = """
    CREATE TABLE if not exists staging_songs (
        num_songs INT,
        artist_id VARCHAR(50),
        artist_name VARCHAR(500),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(500),
        song_id VARCHAR(30),
        title VARCHAR(500),
        duration FLOAT,
        year INT
    )
    """
    
    create_songplays_table = """
    CREATE TABLE if not exists songplays (
        songplay_id VARCHAR(50) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        userId INTEGER,
        level VARCHAR(20),
        song_id VARCHAR(30),
        artist_id VARCHAR(50),
        sessionId INTEGER,
        location VARCHAR(60),
        userAgent VARCHAR(512)
    )
    """

    create_artists_table = """
    CREATE TABLE if not exists artists (
        artist_id VARCHAR(50) NOT NULL,
        artist_name VARCHAR(500),
        artist_location VARCHAR(500),
        artist_latitude FLOAT,
        artist_longitude FLOAT
    )   
    """
	
    create_songs_table = """
    CREATE TABLE if not exists songs (
        song_id VARCHAR(30) NOT NULL PRIMARY KEY,
        title VARCHAR(500),
        artist_id VARCHAR(50),
        year INT,
        duration FLOAT
    )
    """

    create_time_table = """
    CREATE TABLE if not exists time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        dayofweek INTEGER
    )
    """
    
    create_users_table = """
    CREATE TABLE if not exists users (
        userId INT NOT NULL PRIMARY KEY,
        firstName VARCHAR(256),
        lastName VARCHAR(256),
        gender VARCHAR(5),
        level VARCHAR(256)
    )
    """
    #insert statements
    songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id,start_time,userid,level,song_id,artist_id,sessionid,location,useragent )
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
    INSERT INTO users (userid,firstname,lastname,gender,level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND userId is not null
    """)

    song_table_insert = ("""
        INSERT INTO songs(song_id,title,artist_id,year,duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id is not null
    """)

    artist_table_insert = ("""
    INSERT INTO artists (artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        where artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time,hour,day,week,month,year,dayofweek)
        SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        WHERE start_time IS NOT NULL
    """)

    ## list to drop or create tables
    drop_table_list = [drop_staging_events, drop_staging_songs,drop_songplays, drop_artists, drop_songs,drop_time,drop_users]
    create_table_list = [create_staging_log_table, create_staging_song_table,create_songplays_table, create_artists_table, create_songs_table,create_time_table,create_users_table]