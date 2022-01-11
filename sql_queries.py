import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP table  IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop ="DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
    create table events_staging (
                    artist      varchar,
                    
                    auth        varchar 
                                not null,
                    
                    firstName   varchar,
                    
                    gender      char (1),
                    
                    itemInSession int 
                                not null,
                    
                    lastName    varchar,                    
                    
                    length      numeric,
                    
                    level       varchar
                                not null,
                    
                    location    varchar,
                    
                    method      varchar 
                                not null,
                    
                    page        varchar 
                                not null,                    
                    
                    registration numeric,
                    
                    sessionId   int 
                                not null,
                    
                    song        varchar,
                    
                    status      int
                                not null,
                                
                    ts          numeric 
                                not null,
                                
                    userAgent   varchar,
                    userId      int
                )"""

staging_songs_table_create = """
    CREATE TABLE songs_staging 
                (
                    num_songs   int 
                                not null,
                                
                    artist_id   VARCHAR 
                                not null,
                    
                    artist_latitude varchar,
                    
                    artist_longitude varchar,
                    
                    artist_location varchar,
                    
                    artist_name VARCHAR 
                                not null,
                    
                    song_id     VARCHAR 
                                not null,
                    
                    title       VARCHAR 
                                not null,
                    
                    duration    numeric 
                                not null,
                    
                    year        int 
                                not null
                )"""

songplay_table_create = """
    CREATE TABLE songplays 
                ( 
                    songplay_id IDENTITY(0,1) 
                                PRIMARY KEY, 
                                
                    time_id     bigint 
                                NOT NULL 
                                REFERENCES time(time_id),
                                
                    user_id     int 
                                NOT NULL 
                                REFERENCES users(user_id),
                                
                    level       varchar,
                    
                    song_id     varchar  
                                REFERENCES  songs(song_id) 
                                sortkey,  
                                
                    artist_id   varchar  
                                REFERENCES artists(artist_id)
                                DISTKEY,
                                
                    session_id  int,
                    
                    location    varchar,
                    
                    user_agent  varchar
                )
        DISTSTYLE KEY;"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users
                ( 
                    user_id     int 
                                PRIMARY KEY ,
                                                                        
                    first_name  varchar 
                                sortkey, 
                                                            
                    last_name   varchar,
                                                            
                    gender      varchar, 
                                                            
                    level       varchar
                                DISTKEY
                )
        DISTSTYLE KEY;"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs 
                ( 
                    song_id     VARCHAR 
                                PRIMARY KEY ,
                                                                        
                    title       VARCHAR ,
                                                            
                    artist_id   VARCHAR 
                                NOT NULL 
                                REFERENCES artists(artist_id) 
                                SORTKEY, 
                                                                        
                    year        INT
                                DISTKEY  , 
                                                                        
                    duration    numeric
                )
        DISTSTYLE KEY;"""

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
                ( 
                    artist_id   varchar 
                                PRIMARY KEY ,
                                
                    name        varchar,
                    
                    location    varchar 
                                sortkey,
                                
                    latitude    float(7),
                    
                    longitude   float(7)                                
                )
        DISTSTYLE ALL;""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
                ( 
                    time_id     bigint  
                                PRIMARY KEY 
                                sortkey,
                                    
                    start_time  TIMESTAMP
                                NOT NULL ,
                                                            
                    hour        int,
                    
                    day         int,
                    
                    week        int,
                                
                    month       int,
                    
                    year        int
                                DISTKEY,
                                
                    weekday     int
                )
        DISTSTYLE KEY;""")

# STAGING TABLES
staging_events_copy = """
               COPY staging_events
               FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
      TIMEFORMAT AS 'epochmillisecs'
             REGION '{}'
               JSON '{}'           
    TRUNCATECOLUMNS
       BLANKSASNULL
        EMPTYASNULL
        IGNOREHEADER 1 
        MAXERROR     10000;
""".format(
    config.S3_LOG_DATA,
    config.IAM_ROLE_ARN,
    config.AWS_REGION,
    config.S3_LOG_JSON_PATH
)


staging_songs_copy = """
               COPY staging_songs
               FROM '{}/{}'
        CREDENTIALS 'aws_iam_role={}'
      TIMEFORMAT AS 'epochmillisecs'
             REGION '{}'
               JSON 'auto'
    TRUNCATECOLUMNS
       BLANKSASNULL
        EMPTYASNULL
        IGNOREHEADER 1 
        MAXERROR     10000;
""".format(
    config.S3_SONG_DATA,
    config.IAM_ROLE_ARN,
    config.AWS_REGION
)

# FINAL TABLES

songplay_table_insert = """
    insert into songplays 
        (
            time_id, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
    select  
            extract('epoch' from staging_events.ts)::bigint as time_id ,
            staging_events.userId ,
            staging_events.level,
            staging_songs.song_id,
            staging_songs.artist_id,
            staging_events.sessionId,
            staging_events.location,
            staging_events.userAgent 
    FROM 
            staging_events
        JOIN 
            staging_songs
        ON 
            staging_events.artist = staging_songs.artist_name
        AND 
            staging_events.song = staging_songs.title
    WHERE 
        staging_events.page = 'NextSong';"""

user_table_insert = """
    insert into users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
            userId,
            firstName,
            lastName,
            gender,
            level
    FROM 
            staging_events
    WHERE 
            user_id IS NOT NULL;"""

song_table_insert = """
    insert into songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
            song_id,
            title,
            artist_id,
            year,
            duration
    FROM 
            staging_songs
    WHERE 
            song_id IS NOT NULL;"""

artist_table_insert = """
    insert into artists 
         (
            artist_id,
            name, 
            location, 
            latitude, 
            longitude
         )
    SELECT DISTINCT 
            artist_id,
            artist_name ,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM
            staging_songs
    WHERE 
            artist_id IS NOT NULL;"""

time_table_insert = ("""
    insert into time 
        (
            time_id,
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
    SELECT DISTINCT 
            extract('epoch' from ts)::bigint as time_id ,
            ts AS start_time,
            DATE_PART(HOUR, ts) AS hour,
            DATE_PART(DAY, ts) AS day,
            DATE_PART(WEEK, ts) AS week,
            DATE_PART(MONTH, ts) AS month,
            DATE_PART(YEAR, ts) AS year,
            DATE_PART(WEEKDAY, ts) AS weekday
    FROM
            staging_events
    WHERE 
            ts IS NOT NULL;""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert,
                        songplay_table_insert]