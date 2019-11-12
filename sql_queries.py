# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
"""
Some research references:
DOUBLE PRECISION: https://www.postgresql.org/docs/9.0/datatype-numeric.html
DATA TYPES: https://www.postgresql.org/docs/9.5/datatype.html
    http://www.postgresqltutorial.com/postgresql-data-types/
    http://www.postgresqltutorial.com/postgresql-not-null-constraint/

"""
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                          songplay_id VARCHAR(100)
                        , start_time TIMESTAMP REFERENCES time (start_time)
                        , user_id VARCHAR(255) REFERENCES users (user_id)
                        , level VARCHAR(255) 
                        , song_id VARCHAR(255) REFERENCES songs (song_id) 
                        , artist_id VARCHAR(50) REFERENCES artists (artist_id) 
                        , session_id INTEGER
                        , location VARCHAR(255) 
                        , user_agent VARCHAR(255)
                        , PRIMARY KEY (songplay_id))""")

# log Data is different, first_name to firstName, last_name to lastName

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                          user_id VARCHAR(50) 
                        , firstName VARCHAR(50)   
                        , lastName VARCHAR(50)
                        , gender VARCHAR(50)   
                        , level VARCHAR(50)
                        , PRIMARY KEY (user_id))""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                          song_id VARCHAR(100) 
                        , title VARCHAR(100) 
                        , artist_id VARCHAR(100) 
                        , year INTEGER 
                        , duration DOUBLE PRECISION
                        , PRIMARY KEY(song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                          artist_id VARCHAR(100)
                        , name VARCHAR(100) 
                        , location VARCHAR(255) 
                        , latitude DOUBLE PRECISION 
                        , longitude DOUBLE PRECISION
                        , PRIMARY KEY(artist_id))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                          start_time TIMESTAMP 
                        , hour INTEGER 
                        , day INTEGER 
                        , week INTEGER 
                        , month INTEGER 
                        , year INTEGER 
                        , weekday INTEGER
                        , PRIMARY KEY(start_time))""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(
                          start_time
                        , user_id
                        , level
                        , song_id
                        , artist_id
                        , session_id
                        , location
                        , user_agent) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (
                          user_id 
                        , firstName 
                        , lastName 
                        , gender 
                        , level) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET firstName=users.firstName
                        , lastName=users.lastName, gender=users.gender, level=users.level""")

song_table_insert = ("""INSERT INTO songs (
                          song_id 
                        , title 
                        , artist_id 
                        , year 
                        , duration) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists (
                          artist_id
                        , name 
                        , location 
                        , latitude 
                        , longitude) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING""")

time_table_insert = ("""INSERT INTO time (
                          start_time 
                        , hour 
                        , day 
                        , week 
                        , month 
                        , year 
                        , weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time) DO NOTHING""")

# SELECT THE SONGID AND ARTIST ID for etl.ipynd

song_artist_select = ("""SELECT s.song_id, a.artist_id 
                        FROM songs s
                        INNER JOIN artists a ON a.artist_id = s.artist_id
                        WHERE s.artist_id = a.artist_id  
                            AND s.title = %s
                            AND a.name = %s
                            AND s.duration = %s
                """)


# QUERY LISTS 

create_table_queries = [
      user_table_create
    , song_table_create
    , artist_table_create
    , time_table_create
    , songplay_table_create]

drop_table_queries = [
      user_table_drop
    , song_table_drop
    , artist_table_drop
    , time_table_drop
    , songplay_table_drop]
