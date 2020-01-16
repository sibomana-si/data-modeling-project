import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Load relevant data from the specified song file 
        into the song and artist tables.
        
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        filepath (:obj:`str`): path to file containing the song and artist data
    """
    
    try:
        df = pd.read_json(filepath, lines=True)
        
        song_data = list(df[['song_id','title', 'artist_id', 'year', 'duration']].loc[0].values.astype('unicode_'))
        cur.execute(song_table_insert, song_data)
           
        artist_data = list(df[['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].loc[0].values.astype('unicode_'))
        cur.execute(artist_table_insert, artist_data)
    except (psycopg2.Error, OSError) as e: 	    
	    print(e)


    
def process_log_file(cur, filepath):
    """Load relevant data from the specified log file 
        into the time and user tables, and populate the 
        songplay table.
        
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        filepath (:obj:`str`): path to file containing the time and user data.
    """
    
    try:
        df = pd.read_json(filepath, lines=True)
        df = df.loc[df['page'] == 'NextSong']        
        
        t = pd.to_datetime(df['ts'], unit='ms', origin='unix')        
        time_data = (df['ts'], pd.Series.dt(t).hour, pd.Series.dt(t).day, pd.Series.dt(t).weekofyear, pd.Series.dt(t).month, 
                 pd.Series.dt(t).year, pd.Series.dt(t).weekday)
        column_labels = ('ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
        time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
        
        for index, row in df.iterrows():            
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        
            if results:
                songid, artistid = results
            else:
                songid, artistid = 'NULL', 'NULL'

            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
    except (psycopg2.Error, OSError) as e: 	    
	    print(e)
    
    
def process_data(cur, conn, filepath, func):
    """Processes song data using the 'process_song_file' function,
        and log data using the 'process_log_file' function.
        
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        conn (:obj:`connection`): connection object to the sparkify database. 
        filepath (:obj:`str`): path to the data to be processed.
        func (:obj:`function`): the function to be applied in processing the data.            
    """
    
    try:
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))

        num_files = len(all_files)
        print('{} files found in {}'.format(num_files, filepath))

        for i, datafile in enumerate(all_files, 1):        
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
    except (psycopg2.Error, OSError) as e: 	    
	    print(e)


def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)

        conn.close()        
    except psycopg2.Error as e: 	    
	    print(e)


if __name__ == "__main__":
    main()
    