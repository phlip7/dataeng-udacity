# Song Play Analysis
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The code in this folder aims to create and load a dwh for song play analysis.

### Source data stands into json files

### Target dwh will have the following design 
Fact Table:  
songplays - records in log data associated with song plays i.e. records with page NextSong  
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables:  
  users - users in the app  
        user_id, first_name, last_name, gender, level 
        
  songs - songs in music database  
        song_id, title, artist_id, year, duration  
        
  artists - artists in music database  
        artist_id, name, location, lattitude, longitude  
        
  time - timestamps of records in songplays broken down into specific units  
        start_time, hour, day, week, month, year, weekday


### Code for dwh creation and loading : 
test.ipynb displays the first few rows of each table to let you check your database.  

create_tables.py drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.  

etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.  

etl.py reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.  

sql_queries.py contains all your sql queries, and is imported into the last three files above.  
