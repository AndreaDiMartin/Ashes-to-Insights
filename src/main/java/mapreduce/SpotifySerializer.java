/*
 * Este archivo tiene como finalidad serializar y deserializar usando la herramienta avro
 * donde el esquema define el dataset Spotify
 */

package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.opencsv.exceptions.CsvValidationException;

import mapreduce.SpotifyParser;

public class SpotifySerializer {

   
    public static void serializer() throws IOException, CsvValidationException {
        //Se establecen las rutas de los archivos 
        String CSV_FILE_PATH = "tracks-clean.csv";
        String AVRO_SCHEMA_PATH = "./src/avro/spotify.avsc";
        String PATH = "./outputSerializado/spotify.avro";

        //Se define el esquema de los datos de spotify
        Schema schema;
        try {
            schema = new Schema.Parser().parse(new File(AVRO_SCHEMA_PATH));
        } catch(IOException e){
            schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"spotify\",\"namespace\":\"classes.avro\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"track_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"duration\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"explicit\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"popularity\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"acousticness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"danceability\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"energy\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"instrumentalness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"key\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"liveness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"loudness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"speechiness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"tempo\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"time_signature\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"valence\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"album_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"album_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"year_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"month_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"day_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"weekday_of_release\",\"type\":[\"string\",\"null\"]},{\"name\":\"album_popularity\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"artist_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"artist_popularity\",\"type\":[\"int\",\"null\",\"string\"]},{\"name\":\"followers\",\"type\":[\"int\",\"null\",\"string\"]},{\"name\":\"genre_id\",\"type\":[\"string\",\"null\"]}]}");
        }

        //Se define la configuración para el archivo de salida avro
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(PATH);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        //Se crea el dataFileWriter para escribir los datos en el archivo avro
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));
        dataFileWriter.create(schema, fs.create(path));

        //Se llama a la clase SpotifyParser para obtener los datos del archivo csv
        SpotifyParser parser = new SpotifyParser();
        List<String[]> records = parser.parse(new Text(CSV_FILE_PATH));

        for (String[] nextRecord : records) {
            
            // Se extraen los datos de cada campo y se guardan en un objeto GenericRecord
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", nextRecord[0]);
            record.put("track_name", nextRecord[1].isEmpty() ? null :  nextRecord[1]);
            record.put("duration", nextRecord[2].isEmpty() ? null :  Integer.parseInt(nextRecord[2]));
            record.put("explicit", nextRecord[3].isEmpty() ? null :  Integer.parseInt(nextRecord[3]));
            record.put("popularity", nextRecord[4].isEmpty() ? null : Integer.parseInt(nextRecord[4]));
            record.put("acousticness", nextRecord[5].isEmpty() ? null :  Float.parseFloat(nextRecord[5]));
            record.put("danceability", nextRecord[6].isEmpty() ? null :  Float.parseFloat(nextRecord[6]));
            record.put("energy", nextRecord[7].isEmpty() ? null :  Float.parseFloat(nextRecord[7]));
            record.put("instrumentalness", nextRecord[8].isEmpty() ? null :  Float.parseFloat(nextRecord[8]));
            record.put("key", nextRecord[9].isEmpty() ? null :  Integer.parseInt(nextRecord[9]));
            record.put("liveness", nextRecord[10].isEmpty() ? null :  Float.parseFloat(nextRecord[10]));
            record.put("loudness", nextRecord[11].isEmpty() ? null :  Float.parseFloat(nextRecord[11]));
            record.put("speechiness", nextRecord[12].isEmpty() ? null :  Float.parseFloat(nextRecord[12]));
            record.put("tempo", nextRecord[13].isEmpty() ? null :  Float.parseFloat(nextRecord[13]));
            record.put("time_signature", nextRecord[14].isEmpty() ? null :  Integer.parseInt(nextRecord[14]));
            record.put("valence", nextRecord[15].isEmpty() ? null :  Float.parseFloat(nextRecord[15]));
            record.put("album_name", nextRecord[16].isEmpty() ? null :  nextRecord[16]);
            record.put("album_type", nextRecord[17].isEmpty() ? null :  nextRecord[17]);
            record.put("year_of_release",nextRecord[18].isEmpty() ? null : Integer.parseInt(nextRecord[18]));
            record.put("month_of_release",nextRecord[19].isEmpty() ? null : Integer.parseInt(nextRecord[19]));
            record.put("day_of_release",nextRecord[20].isEmpty() ? null : Integer.parseInt(nextRecord[20]));
            record.put("weekday_of_release",nextRecord[21].isEmpty() ? null : nextRecord[21]);
            record.put("album_popularity", nextRecord[22].isEmpty() ? null :  Integer.parseInt(nextRecord[22]));
            record.put("artist_name", nextRecord[23].isEmpty() ? null :  nextRecord[23]);
            record.put("artist_popularity", nextRecord[24].isEmpty() ? null :  Integer.parseInt(nextRecord[24]));
            record.put("followers", nextRecord[25].isEmpty() ? null :  Integer.parseInt(nextRecord[25]));
            record.put("genre_id", nextRecord[26].isEmpty() ? null :  nextRecord[26]);

            //Se escribe el registro en el archivo avro
            dataFileWriter.append(record);
            //Se imprime el registro 
            System.out.println(record);
        }
        
        //Se cierra el archivo avro
        dataFileWriter.close();
    }


    public static void deserializer() throws IOException {
        //Se establecen las rutas de los archivos necesarios para la deserialización
        String AVRO_SCHEMA_PATH = "src/avro/spotify.avsc";
        String INPUT_PATH = "./outputSerializado/spotify.avro";
        String OUTPUT_PATH = "./outputDeserializado/spotify_deserialized.txt";
    
        // Se carga el esquema de los datos de spotify que posee en archivo avro
        Schema schema;
        try {
            schema = new Schema.Parser().parse(new File(AVRO_SCHEMA_PATH));
        } catch(IOException e){
            schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"spotify\",\"namespace\":\"classes.avro\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"track_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"duration\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"explicit\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"popularity\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"acousticness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"danceability\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"energy\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"instrumentalness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"key\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"liveness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"loudness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"speechiness\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"tempo\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"time_signature\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"valence\",\"type\":[\"float\",\"string\",\"null\"]},{\"name\":\"album_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"album_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"year_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"month_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"day_of_release\",\"type\":[\"int\",\"null\"]},{\"name\":\"weekday_of_release\",\"type\":[\"string\",\"null\"]},{\"name\":\"album_popularity\",\"type\":[\"int\",\"string\",\"null\"]},{\"name\":\"artist_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"artist_popularity\",\"type\":[\"int\",\"null\",\"string\"]},{\"name\":\"followers\",\"type\":[\"int\",\"null\",\"string\"]},{\"name\":\"genre_id\",\"type\":[\"string\",\"null\"]}]}");
        }
    
        //Se abre el archivo avro para leer los registros
        Configuration conf = new Configuration();
        Path inputPath = new Path(INPUT_PATH);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        FsInput fsInput = new FsInput(inputPath, conf);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(fsInput, datumReader);
    
        //Se crea el archivo de salida y se borra si ya existe
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(OUTPUT_PATH);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FSDataOutputStream outputStream = fs.create(outputPath);

        //Se escriben los registros deserializados en el archivo de salida
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                writer.write(record.toString());
                writer.newLine();
            }
        }

        // Close the Avro file
        dataFileReader.close();
    
        System.out.println("Deserialized records written to " + OUTPUT_PATH);
    }

    public static void main(String[] args) throws IOException, CsvValidationException {
        //Verifica que el número de argumentos sea correcto
        if (args.length != 1) {
            System.out.println("Usage: GenerateSpotify <flag>");
            System.exit(1);
        }
        //Se obtiene el flag para determinar si se serializa o deserializa
        String flag = args[0];
        if (flag.equals("serializer")) { 
            serializer();
        } else if (flag.equals("deserializer")) { 
            deserializer();
        } else {
            System.out.println("Invalid flag. Use 'serializer' or 'deserializer'.");
        }
    }
}