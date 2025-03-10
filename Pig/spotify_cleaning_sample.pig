REGISTER 'pig-0.16.0/lib/piggybank.jar';
REGISTER 'Ashes-to-Insights/get_release_date.py' USING jython AS date;

-- Carga de los datos
tracks = LOAD 'tracks-sample.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER') 
AS (
    id:                 chararray, 
    track_name:         chararray, 
    disc_number:        int, 
    duration:           long, 
    explicit:           int, 
    audio_feature_id:   chararray,
    preview_url:        chararray, 
    track_number:       int, 
    popularity:         int,
    is_playable:        int, 
    acousticness:       double, 
    danceability:       double, 
    energy:             double, 
    instrumentalness:   double, 
    key:                int, 
    liveness:           double,
    loudness:           double, 
    mode:               int, 
    speechiness:        double, 
    tempo:              double,
    time_signature:     int, 
    valence:            double, 
    album_name:         chararray,
    album_group:        chararray,
    album_type:         chararray, 
    release_date:       chararray,
    album_popularity:   int, 
    artist_name:        chararray, 
    artist_popularity:  int,
    followers:          int,
    genre_id:           chararray
    );

-- Quitar nulos
no_null = FILTER tracks BY release_date IS NOT NULL AND valence IS NOT NULL;

-- Limpieza de las columnas y obtención de los campos de fecha
filtered_tracks = FOREACH no_null GENERATE  CONCAT(CONCAT('"', REPLACE(id, '"', '\"')), '"') AS id,
                                            CONCAT(CONCAT('"', REPLACE(track_name, '"', '\"')), '"') AS track_name,
                                            duration,
                                            explicit,
                                            popularity,
                                            acousticness,
                                            danceability,
                                            energy,
                                            instrumentalness,
                                            key,
                                            liveness,
                                            loudness,
                                            speechiness,
                                            tempo,
                                            time_signature,
                                            valence,
                                            CONCAT(CONCAT('"', REPLACE(album_name, '"', '\"')), '"') AS album_name,
                                            CONCAT(CONCAT('"', REPLACE(album_type, '"', '\"')), '"') AS album_type,

                                            date.get_year(release_date) AS year,
                                            date.get_month(release_date) AS month,
                                            date.get_day(release_date) AS day,
                                            date.get_day_of_week(release_date) AS day_of_week,

                                            album_popularity,
                                            CONCAT(CONCAT('"', REPLACE(artist_name, '"', '\"')), '"') AS artist_name,
                                            artist_popularity,
                                            followers,
                                            CONCAT(CONCAT('"', REPLACE((genre_id != '' ? genre_id : 'Unknown'), '"', '\"')), '"') AS genre_id;
                                            

-- Guardado de los datos
STORE filtered_tracks INTO 'tracks-sample-new' USING PigStorage(',');