import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used them to populate the song and artist dim tables.

    Arguments:
    cur: the cursor object. 
    filepath: song data file path. 

    Returns:
    None
    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
    cur: the cursor object. 
    filepath: log data file path. 

    Returns:
    None
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_nxt = df['page'] == 'NextSong'
    df = df[df_nxt]

    # convert timestamp column to datetime
    df['dateTime'] = pd.to_datetime(df['ts'], unit='ms')
    t = df
    
    # insert time data records
    time_data = t
    time_data['hour'] = time_data['dateTime'].astype(str).str.slice(11, 23, 1)
    time_data['day'] = time_data['dateTime'].dt.day
    time_data['week'] = time_data['dateTime'].dt.week
    time_data['month'] = time_data['dateTime'].dt.month
    time_data['year'] = time_data['dateTime'].dt.year
    time_data['weekday'] = time_data['dateTime'].dt.weekday
    
    column_labels = ['dateTime','hour','day','week','month','year','weekday']
    time_df = time_data[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            
            # insert songplay record
            songplay_data = (row.dateTime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
        else:
            songid, artistid = None, None



def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
