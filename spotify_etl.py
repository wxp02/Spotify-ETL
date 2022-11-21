import pandas as pd
import sqlalchemy
import sqlite3
import spotipy
import sys
import schedule
import time
from spotipy.oauth2 import SpotifyOAuth


# Connecting to Spotify API and retrieving the json file containing information of my 50 most recently played tracks
def api_to_json():
    client_id = ''
    client_secret = ''
    redirect_uri = 'http://localhost:8888/callback'
    scope = 'user-read-recently-played'
    auth_manager = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    recently_played_tracks = sp.current_user_recently_played(limit=50)
    if len(recently_played_tracks) == 0:
        sys.exit('Recent played songs unavailable')
    return recently_played_tracks


# Creating a DataFrame containing the album, track, and artists' names of each recently played songs
def recent_tracks():
    # Find album and track names
    track_name_list = []
    album_name_list = []
    played_at_list = []
    played_at_detailed_list = []
    for i in api_to_json()['items']:
        track_name = i['track']['name']
        album_name = i['track']['album']['name']
        played_at = i['played_at'][0:10]
        played_at_detailed = i['played_at']
        track_name_list.append(track_name)
        album_name_list.append(album_name)
        played_at_list.append(played_at)
        played_at_detailed_list.append(played_at_detailed)
    df1 = pd.DataFrame({'track_name':track_name_list, 'album_name':album_name_list})
    df3 = pd.DataFrame({'played_at':played_at_list, 'played_at_detailed':played_at_detailed_list})
    # Find artists' names
    artist_name_list = []
    for i in api_to_json()['items']:
        all_artists = i['track']['album']['artists'][0]['name']
        for artist in range(1, len(i['track']['album']['artists'])):
            artist_name = i['track']['album']['artists'][artist]['name']
            all_artists += ', ' + artist_name
        artist_name_list.append(all_artists)
    df2 = pd.DataFrame({'artists':artist_name_list})
    return pd.concat([df1, df2, df3], axis=1)


# Conducting checks to make sure DataFrame is valid to load into database
def valid_data():
    # Check if DataFrame is empty
    if recent_tracks().empty:
        print('No recent played songs.')
        return False
    # Primary Key Check
    if pd.Series(recent_tracks()['played_at_detailed']).is_unique:
        pass
    else:
        raise Exception('Failed primary key check.')
    # Check for nulls
    if recent_tracks().isnull().values.any():
        raise Exception('Null value(s) found.')


# Creating a SQLite database storing the DataFrame
def create_db():
    DATABASE_LOCATION = 'sqlite:///recent_played_tracks.sqlite' 
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    connect = sqlite3.connect('recent_played_tracks.sqlite')
    cursor = connect.cursor()
    sql_query = """
        CREATE TABLE IF NOT EXISTS recent_played_tracks(
            track_name VARCHAR(200),
            album_name VARCHAR(200),
            artists VARCHAR(200),
            played_at VARCHAR(200),
            played_at_detailed VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at_detailed)
        )
        """
    cursor.execute(sql_query)
    print("Opened database successfully.")
    recent_tracks().to_sql('recent_played_tracks', engine, index=False, if_exists='replace')
    connect.close()
    print("Closed database successfully.")


# The script is scheduled to run every Monday
schedule.every().monday.do(api_to_json)
schedule.every().monday.do(recent_tracks)
schedule.every().monday.do(valid_data)
schedule.every().monday.do(create_db)

while True:
    schedule.run_pending()
    time.sleep(1)
