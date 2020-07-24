import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users cascade"
song_table_drop = "DROP TABLE IF EXISTS song cascade"
artist_table_drop = "DROP TABLE IF EXISTS artist cascade"
time_table_drop = "DROP TABLE IF EXISTS time cascade"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events(
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    iteminSession int,
                                    lastName varchar,
                                    length numeric,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration numeric,
                                    sessionid int,
                                    song varchar,
                                    status int,
                                    ts bigint,
                                    userAgent varchar,
                                    userId int)"""
)

staging_songs_table_create = (""" CREATE TABLE staging_songs(
                                    num_songs int,
                                    artist_id varchar,
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration float,
                                    year int)"""
)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS SONGPLAYS (
                                    SONGPLAY_ID INT PRIMARY KEY IDENTITY ( 1, 1 ),
                                    START_TIME TIMESTAMP REFERENCES TIME (start_time),
                                    USER_ID INT REFERENCES USERS(user_id) , 
                                    LEVEL VARCHAR NOT NULL, 
                                    SONG_ID VARCHAR REFERENCES songs(song_id) , 
                                    ARTIST_ID VARCHAR  REFERENCES artists(artist_id), 
                                    SESSION_ID INT NOT NULL, 
                                    LOCATION VARCHAR NOT NULL, 
                                    USER_AGENT VARCHAR NOT NULL)"""
)

user_table_create = (""" CREATE TABLE IF NOT EXISTS USERS (
                                    USER_ID INT PRIMARY KEY, 
                                    FIRST_NAME VARCHAR NOT NULL, 
                                    LAST_NAME VARCHAR NOT NULL, 
                                    GENDER VARCHAR(1) NOT NULL, 
                                    LEVEL VARCHAR NOT NULL)"""
)

song_table_create = (""" CREATE TABLE IF NOT EXISTS SONGS (
                                    SONG_ID VARCHAR PRIMARY KEY, 
                                    TITLE VARCHAR NOT NULL, 
                                    ARTIST_ID VARCHAR NOT NULL, 
                                    YEAR INT NOT NULL, 
                                    DURATION NUMERIC NOT NULL)"""
)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS ARTISTS(
                                    ARTIST_ID VARCHAR PRIMARY KEY, 
                                    NAME VARCHAR NOT NULL, 
                                    LOCATION VARCHAR , 
                                    LATITUDE NUMERIC , 
                                    LONGITUDE NUMERIC) """
)

time_table_create = (""" CREATE TABLE IF NOT EXISTS TIME(
                                    START_TIME TIMESTAMP PRIMARY KEY, 
                                    HOUR INT NOT NULL, 
                                    DAY INT NOT NULL, 
                                    WEEK INT NOT NULL, 
                                    MONTH INT NOT NULL, 
                                    YEAR INT NOT NULL, 
                                    WEEKDAY INT NOT NULL)"""
)

# STAGING TABLES

staging_events_copy = (""" COPY staging_events FROM 's3://udacity-dend/log_data' 
                            CREDENTIALS 'aws_iam_role={}'
                            format as json 's3://udacity-dend/log_json_path.json'
                            region 'us-west-2'"""
)

staging_songs_copy = (""" COPY staging_songs FROM 's3://udacity-dend/song_data' 
                            CREDENTIALS 'aws_iam_role={}'
                            format as json 'auto'
                            region 'us-west-2'"""
)

# FINAL TABLES

# Inserting in songplays tables by joining both staging_tables 
songplay_table_insert = ("""INSERT into songplays(START_TIME, USER_ID, LEVEL , SONG_ID, ARTIST_ID, SESSION_ID, LOCATION , USER_AGENT)
                                select TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as START_TIME,
                                        e.userId, 
                                        e.level, 
                                        s.song_id,
                                        s.artist_id,
                                        e.sessionid,
                                        e.location,
                                        e.useragent
                                from staging_events e
                                join staging_songs s on ( e.song = s.title and e.artist = s.artist_name and e.length = s.duration)
                                where e.userid is NOt NULL and s.song_id is not null and s.artist_id is not null AND e.page = 'NextSong'"""
)

# Creating users and users_temp table(temporary table)
# Loading data in t users_temp table
# Filtering unique user id from users_temp table and inserting it in users table
# Dropping users_temp table

user_table_insert = (""" INSERT INTO users(USER_ID, FIRST_NAME ,  LAST_NAME ,  GENDER ,  LEVEL)
                         SELECT DISTINCT USERID, FIRSTNAME ,  LASTNAME ,  GENDER ,  LEVEL
                         FROM staging_events
                         WHERE  userid is not null
""")
# Insert qyery for song table from staging_songs table

song_table_insert = (""" INSERT INTO songs(SONG_ID,TITLE,ARTIST_ID,YEAR,DURATION )
                            select song_id, title, artist_id, year, duration
                            from staging_songs
""")

# Insert qyery for artist table from staging_songs table

artist_table_insert = (""" INSERT INTO ARTISTS (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
                            select  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            from staging_songs
""")

# Insert qyery for time table from staging_events table

time_table_insert = ("""INSERT into TIME (START_TIME, HOUR, DAY, WEEK,  MONTH,YEAR,WEEKDAY)
                        SELECT  START_TIME, 
                                date_part(h,START_TIME) AS HOUR,
                                date_part(d,START_TIME) AS DAY,
                                date_part(w,START_TIME) AS WEEK,
                                date_part(mon,START_TIME) AS MONTH,
                                date_part(y,START_TIME) AS YEAR,
                                date_part(dw,START_TIME) AS WEEKDAY
                        from songplays;"""
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
