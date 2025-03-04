-- Carga de los datos
tracks = LOAD 'CAMBIAR/spotify_tracks.csv' 
USING PigStorage(',') 
AS (id:chararray, track_name:chararray, disc_number:int, 
duration:long, explicit:int, audio_feature_id:chararray,
preview_url:chararray, track_number:int, popularity:int,
is_playable:int, acousticness:double, danceability:double, 
energy:double, instrumentalness:double, key:int, liveness:double,
loudness:double, mode:int, speechiness:double, tempo:double,
time_signature:int, valence:double, album_name:chararray,
album_group:chararray, album_type:chararray, release_date:chararray,
album_popularity:int, artist_name:chararray, artist_popularity:int,
followers:int, genre_id:chararray);

