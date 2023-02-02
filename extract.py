import requests
import os 
import yaml
import datetime
import pandas as pd

#fetching the token from the config file
config_file='spotify-airflow\config.yaml'
with open(config_file,'r') as f:
    r=yaml.safe_load(f)
    cfg=r['param']

token=cfg['token']

def create_dataframe():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=500) #no of Days u want the data for)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    input_variables={
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=token)
    }

    response = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp), headers = input_variables)

    songs=response.json()

    song_names=[]
    artist_names=[]
    played_at_list = []
    timestamps = []

    for song in songs['items']:
        song_names.append(song['track']['album']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    print(len(song_df))
    return song_df
        
