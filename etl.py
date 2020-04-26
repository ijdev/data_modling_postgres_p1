import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # looping through the songs.
    # extracting the values and storing them for readability
    for value in df.values:
        num_songs = value[0]
        artist_id = value[1]
        lat = value[2]
        long = value[3]
        location = value[4]
        artist_name = value[5]
        song_id = value[6]
        title = value[7]
        duration = value[8]
        year = value[9]
        # insert song record
        song_data = [song_id, title, artist_id, year, duration]
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = [artist_id, artist_name, location, lat, long]
        cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime

    # create new column as datetime
    df['date'] = pd.to_datetime(df.ts, unit='ms')

    # extracting information from the new column
    ts = df.ts.values.tolist()
    hour = df['date'].dt.hour.values.tolist()
    day = df['date'].dt.day.values.tolist()
    week = df['date'].dt.week.values.tolist()
    month = df['date'].dt.month.values.tolist()
    year = df['date'].dt.year.values.tolist()
    weekday = df['date'].dt.day_name(locale='English').values.tolist()

    time_data = [ts, hour, day, week, month, year, weekday]
    column_labels = ('Start_Time', 'Hour', 'Day', 'Week', 'Month', 'Year', 'Weekday')

    time_df = pd.DataFrame(time_data)
    time_df = time_df.transpose()
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame([df.userId.values.tolist(),
                            df.firstName.values.tolist(),
                            df.lastName.values.tolist(),
                            df.gender.values.tolist(),
                            df.level.values.tolist()])
    user_df = user_df.transpose()
    # naming columns
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (
        row.ts,
        row.userId,
        row.level, songid,
        artistid,
        row.sessionId,
        row.location,
        row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


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

