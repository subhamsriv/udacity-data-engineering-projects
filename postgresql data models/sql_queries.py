# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS SONGPLAYS (
                                    SONGPLAY_ID INT PRIMARY KEY,
                                    START_TIME TIME NOT NULL,
                                    USER_ID INT NOT NULL, 
                                    LEVEL VARCHAR NOT NULL, 
                                    SONG_ID VARCHAR , 
                                    ARTIST_ID VARCHAR , 
                                    SESSION_ID INT NOT NULL, 
                                    LOCATION VARCHAR NOT NULL, 
                                    USER_AGENT VARCHAR NOT NULL)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS USERS (
                                    USER_ID INT PRIMARY KEY, 
                                    FIRST_NAME VARCHAR NOT NULL, 
                                    LAST_NAME VARCHAR NOT NULL, 
                                    GENDER VARCHAR(1) NOT NULL, 
                                    LEVEL VARCHAR NOT NULL)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS SONGS (
                                    SONG_ID VARCHAR PRIMARY KEY, 
                                    TITLE VARCHAR NOT NULL, 
                                    ARTIST_ID VARCHAR NOT NULL, 
                                    YEAR INT NOT NULL, 
                                    DURATION NUMERIC NOT NULL)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS ARTISTS(
                                    ARTIST_ID VARCHAR PRIMARY KEY, 
                                    NAME VARCHAR NOT NULL, 
                                    LOCATION VARCHAR , 
                                    LATITUDE NUMERIC , 
                                    LONGITUDE NUMERIC) 
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS TIME(
                                    START_TIME TIME PRIMARY KEY, 
                                    HOUR INT NOT NULL, 
                                    DAY INT NOT NULL, 
                                    WEEK INT NOT NULL, 
                                    MONTH INT NOT NULL, 
                                    YEAR INT NOT NULL, 
                                    WEEKDAY INT NOT NULL)
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO SONGPLAYS (SONGPLAY_ID,
                                    START_TIME,
                                    USER_ID, 
                                    LEVEL, 
                                    SONG_ID, 
                                    ARTIST_ID, 
                                    SESSION_ID, 
                                    LOCATION, 
                                    USER_AGENT) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT DO NOTHING
""")

user_table_insert = (""" INSERT INTO USERS (USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL)
                                VALUES (%s,%s,%s,%s,%s) ON CONFLICT(USER_ID) DO
                                UPDATE SET LEVEL = EXCLUDED.level
""")

song_table_insert = (""" INSERT INTO SONGS(SONG_ID, TITLE, ARTIST_ID, YEAR, DURATION) 
                                VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")

artist_table_insert = (""" INSERT INTO ARTISTS(ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
                                VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")


time_table_insert = (""" INSERT INTO TIME(START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY)
                                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = (""" SELECT songs.song_id, songs.artist_id FROM songs JOIN artists on songs.artist_id = artists.artist_id 
where songs.title = %s and artists.name = %s and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]