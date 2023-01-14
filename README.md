
# Udacity - Sparkify Data Warehouse

Sparkify, a ficitional, music-streaming startup, has experienced an increase in both the number of users and the amount of music in their app and they are now planning to shift their operations and information to the cloud. The company's data, which includes log files containing user activity on the app and information about the songs available on the app, is currently stored in S3 in the form of JSON files.

The purpose of this project is to design an ETL pipeline that retrieves data from S3, temporarily stores it in Redshift staging tables, and then processes it into a series of dimensional tables. These tables will be used by the analytics team to gain insight into what songs are popular among the users of the app.


## Objectives

To build an ETL script comprised of extracting data from S3, copying the data into Redshift staging tables, and inserting into dimension tables optimized for user experience.

## Database Schema Design

The database design follows a star schema structure to eliminate the need for complex joins when querying data. In addition, the system allows for faster access to information because the engine does not have to combine various tables to generate results. This makes it simpler to derive business insights for downstream analysts. 

In this case, the **songplays** table acts as the facts table supported by the users, songs, artists, and time dimension tables. The songplays table is a record-based table and records singular instances of song-listening behaviour from users, hence its centrality in the star schema. 

Additionally, there are two staging tables to copy data from S3 and insert into the finalized tables. The **staging_songs** table stages data from the s3://udacity-dend/song_data directory and the **staging_events** table stages data from the s3://udacity-dend/log_data directory. Each directory is loaded into a separate table to be able to customize the format and processing of each unique directory as it is loaded into the Redshift environment. 

The schema of each table can be defined as the following:

#### Staging Events Table
    artist varchar
    auth varchar
    firstName varchar
    gender varchar
    itemInSession int
    lastName varchar
    length double precision
    level varchar
    location varchar
    method varchar
    page varchar
    registration float
    sessionId int
    song varchar
    status int
    ts bigint
    userAgent varchar
    userId int

#### Staging Songs Table

    num_songs int
    artist_id varchar
    artist_latitude double precision
    artist_longitude double precision
    artist_location varchar
    artist_name varchar
    song_id varchar
    title varchar
    duration double precision
    year int

#### Songplays Table
    songplay_id int PRIMARY KEY
    start_time timestamp
    user_id int
    level varchar
    song_id varchar
    artist_id varchar
    session_id int
    location varchar
    user_agent varchar

#### Users Table

    user_id int PRIMARY KEY
    first_name varchar
    last_name varchar
    gender varchar
    level varchar

#### Songs Table

    song_id varchar PRIMARY KEY
    title varchar
    artist_id int
    year int
    duration double precision

#### Artists Table

    artist_id varchar PRIMARY KEY
    name varchar
    location varchar
    latitude double precision
    longitude double precision

#### Time Table

    start_time timestamp
    hour int
    day int
    week int
    month int
    year int
    weekday int

## Sample Queries

> What are the top 10 most streamed songs? 

    SELECT songs.title, count(*) as streams
    FROM songplays 
    JOIN songs ON (songplays.song_id = songs.song_id)
    GROUP BY songs.title
    ORDER BY streams desc
    LIMIT 10

> Rank the highest usage time of day by hour for song streaming.

    SELECT time.hour, count(*) as streams
    FROM songplays 
    JOIN time ON (songplays.start_time = time.start_time)
    GROUP BY time.hour
    ORDER BY streams desc

> Which gender listens to the most music on Sparkify?

    SELECT users.gender, count(*) as streams
    FROM songplays 
    JOIN users ON (songplays.user_id = users.user_id)
    GROUP BY users.gender
    ORDER BY streams desc
    LIMIT 1





