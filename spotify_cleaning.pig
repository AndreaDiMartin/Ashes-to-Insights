REGISTER 'pig-0.16.0/lib/piggybank.jar';

-- Carga de los datos
tracks = LOAD 'CAMBIAR/spotify_tracks.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (id:chararray, track_name:chararray, disc_number:int, 
duration:long, explicit:int, audio_feature_id:chararray,
preview_url:chararray, track_number:int, popularity:int,
is_playable:int, acousticness:double, danceability:double, 
energy:double, instrumentalness:double, key:int, liveness:double,
loudness:double, mode:int, speechiness:double, tempo:double,
time_signature:int, valence:double, album_name:chararray,
album_group:chararray, album_type:chararray, release_date:chararray,
album_popularity:int, artist_name:chararray, artist_popularity:int,
followers:int, genre_id:chararray);

-- Limpieza de las columnas
filtered_tracks = FOREACH tracks GENERATE   id, track_name, duration, explicit, popularity,
                                            acousticness, danceability, energy, instrumentalness,
                                            key, liveness,loudness, speechiness, tempo, time_signature,
                                            valence, album_name, album_type,
                                             release_date, album_popularity,
                                            artist_name, artist_popularity, followers, genre_id;
                                            
-- Quitar nulos
no_null = FILTER tracks BY release_date IS NOT NULL AND valence IS NOT NULL;

REGISTER get_release_date.py 

-- Guardado de los datos
STORE filtered_tracks INTO 'spotify_tracks_clean_new.csv'
USING PigStorage(',');