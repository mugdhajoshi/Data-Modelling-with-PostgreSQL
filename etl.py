import os
import pandas as pd
import psycopg2
from queries import *

# Function to process all the json files 
# :param cur: cursor to the database
# :param conn: database connection reference
# :param filepath: Absolute path to the file
# :param func: Function to process song and log data is passed
def process_data(cur, conn, filepath, func):
    list_of_files = []
    for (dirpath, dirnames, filenames) in os.walk(filepath):
        for filename in filenames:
            if filename.endswith('.json'): 
                list_of_files.append(os.path.abspath(os.path.join(dirpath, filename)))
                            
    # iterate over files and process
    for datafile in list_of_files:
        func(datafile, cur)
        conn.commit()
        
 
# Function to process song files
# :param filepath: Absolute path to the file
# :param cur: cursor to the database
def process_song_files(filepath , cur):
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])
    
    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        # insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)
        
         # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
        

# Function to process log files
# :param filepath: Absolute path to the file
# :param cur: cursor to the database
def process_log_files(filepath, cur):
    dfs = []
    data = pd.read_json(filepath, typ='series', convert_dates=False, lines = True)
    for i in range(len(data)):
        dfs.append(pd.DataFrame(data[i], index=[0]))
          
    df = pd.concat(dfs, ignore_index=True, sort=False)
    df = df[df['page'] == "NextSong"].reset_index(drop = True)  
    
    df["timestamp"] = pd.to_datetime(df.ts)
    df['date']= [d.date for d in df.timestamp]
    df['hour']= [d.hour for d in df.timestamp]
    df['day']= [d.day for d in df.timestamp]
    df['weekofyear']= [d.weekofyear for d in df.timestamp]
    df['month']= [d.month for d in df.timestamp]
    df['year']= [d.year for d in df.timestamp]
    df['day_name']= [d.day_name() for d in df.timestamp]
    
    
    for value in df.values:
        artist, auth, firstName, gender, itemInSession, lastName,\
        length, level, location, method, page, registration,\
        sessionId, song, status, ts, userAgent, userId, timestamp,\
        date, hour, day, weekofyear, month, year, day_name = value
        
        # insert time data records
        time_data = (timestamp, hour, day, weekofyear, month, year, day_name)
        cur.execute(time_table_insert, time_data)
        
        # insert user records
        user_data = (userId, firstName, lastName, gender, level)
        cur.execute(user_table_insert, user_data)
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (song, artist, length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # insert songplay record
        songplay_data = ( timestamp, userId, level, songid, artistid, sessionId, location, userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
    


# Driver function for loading songs and log data into Postgres database
def main():
   
    conn = psycopg2.connect("host=127.0.0.1 dbname=songsdb user=postgres password=password")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_files)
    process_data(cur, conn, filepath='data/log_data',  func=process_log_files)

    conn.close()