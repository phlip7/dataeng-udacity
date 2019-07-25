## Project 4: Datalake  

### Project Description :

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.   
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Data Processes :

The repo contains an an ETL pipeline that  
- extracts their data from S3,  
- processes them using Spark,  
- and loads the data back into S3 as a set of dimensional tables.

### Input Data in S3:

There are two datasets that reside in S3.  
- Song Dataset
below is an example of what a song record
> > {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
- Log Dataset
below is an example of what a song record
>> {"artist":"Stephen Lynch","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Bell","length":182.85669,"level":"free","location":"Dallas-Fort Worth-Arlington, TX","method":"PUT","page":"NextSong","registration":1540991795796.0,"sessionId":829,"song":"Jim Henson's Dead","status":200,"ts":1543537327796,"userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)","userId":"91"}

### Output Data in S3 as DWH:

Using the song and log datasets, the processes create a star schema optimized for queries on song play analysis.  
Below the structure of the DWH created

Fact Table

1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

2. **users** - users in the app
        user_id, first_name, last_name, gender, level
3. **songs** - songs in music database
        song_id, title, artist_id, year, duration
4. **artists** - artists in music database
        artist_id, name, location, lattitude, longitude
5. **time** - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
