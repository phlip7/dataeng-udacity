import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
## create staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stg_events(artist varchar(200), auth varchar(15), firstName varchar(50), gender char(1), 
        itemInSession int, lastName varchar(50), length float, level varchar(5), location varchar(150), method varchar(5),   
        page varchar(20), registration varchar(30), sessionId int, song varchar(200), status int, ts BIGINT,       
        userAgent varchar(200), userId int )
""") 

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stg_songs(
        num_songs int, artist_id varchar(50), artist_latitude float, artist_longitude float, 
        artist_location varchar(200), artist_name varchar(200), song_id varchar(20), 
        title varchar(200), duration float, year int 
    )""")

## create dwh tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id int primary key IDENTITY(1,1)
        , start_time datetime
        , user_id int
        , level varchar(5)
        , song_id varchar(30)
        , artist_id  varchar(30)
        , session_id int
        , location varchar(150)
        , user_agent varchar(200) 
    )""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int
        , first_name varchar(50)
        , last_name varchar(50)
        , gender char(1)
        , level varchar(5) 
    )""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs( 
        song_id varchar(30)
        , title varchar(200)
        , artist_id varchar(30)
        , year int
        , duration float 
    )""")


artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar(30)
        , name varchar(200)
        , location varchar(200)
        , latitude float
        , longitude float
    )""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp
        , hour smallint
        , day smallint
        , week smallint
        , month smallint
        , year smallint
        , weekday smallint
    )""")


# STAGING TABLES LOADING

staging_events_copy = ("""
    COPY stg_events 
    FROM '{}' 
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT as json 'auto'  REGION 'us-west-2'
    """).format( config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    COPY stg_songs 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT as json 'auto'  REGION 'us-west-2'
""").format( config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL DWH TABLES LOADING

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second' AS start_time
        , e.userid
        , e.level
        , s.song_id
        , s.artist_id
        , e.sessionid
        , a.location
        , e.useragent
    FROM songs s 
    INNER JOIN artists a  on s.artist_id = a.artist_id
    INNER JOIN stg_events e on a.name = e.artist
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT distinct userid, firstname, lastname, gender, level
    FROM stg_events
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT distinct song_id, title, artist_id, year, duration
    FROM stg_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM stg_songs
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    WITH todatetime as (
        SELECT ts, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS ts_datetime 
        FROM stg_events
    )
    SELECT ts_datetime
        , date_part(hour, ts_datetime)
        , date_part(day, ts_datetime)
        , date_part(w, ts_datetime)
        , date_part(month, ts_datetime)
        , date_part(year, ts_datetime)
        , date_part(dw, ts_datetime)
    FROM todatetime
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
