REGISTER 'pig-0.16.0/lib/piggybank.jar';
REGISTER 'Ashes-to-Insights/get_release_date.py' USING jython AS date;

-- Carga de los datos
tracks = LOAD 'Ashes-to-Insights/tracks-sample.csv' 
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
filtered_tracks = FOREACH no_null GENERATE  id,
                                            track_name,
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
                                            album_name, 
                                            album_type,

                                            date.get_year(release_date),
                                            date.get_month(release_date),
                                            date.get_day(release_date),
                                            date.get_day_of_week(release_date),

                                            release_date, 
                                            album_popularity,
                                            artist_name, 
                                            artist_popularity, 
                                            followers,
                                            genre_id;
                                            



-- Guardado de los datos
STORE filtered_tracks INTO 'Ashes-to-Insights/tracks-sample-new' USING PigStorage(',');