## Project: Data Warehouse
## Data Engineering Nanodegree 

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

#### Model

In addition to the Udacity nanodegree, I found this links about how to model data in AWS Redshift really interesting:

    AWS Redshift Reference: CREATE TABLE
    AWS Redshift Reference: Distribution Styles
    AWS Big Data Blog: Distribution Styles and Distribution Keys

convert from csv files to staging_events and staging_songs tables in DB by ETL

### Dimension and fact tables

convert from staging_events and staging_songs tables to -Dimension and fact tables  

###### Dimension Tables
-->users - users in the app(user_id, first_name, last_name, gender, level)
-->songs - songs in music database(song_id, title, artist_id, year, duration)
-->artists - artists in music database(artist_id, name, location, lattitude, longitude)
-->time - timestamps of records in songplays broken down into specific units(time_id,tart_time, hour, day, week, month, year, weekday)

###### fact table
-->1songplays - records in event data associated with song plays i.e. records with page NextSong 
(songplay_id, time_id, user_id, level, song_id, artist_id, session_id, location, user_agent)
