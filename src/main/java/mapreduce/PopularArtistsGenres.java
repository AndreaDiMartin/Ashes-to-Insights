package mapreduce;

//Problema 3: Géneros de los artistas más populares

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularArtistsGenres extends Configured implements Tool {
    //Géneros principales más populares
    private static final List<String> GENRES = Arrays.asList(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "rap", "blues", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore", "trap"
        );

    private static Object[] CSVParser(String line){
        List<String> spotifyLine = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        // Crea un flag para tomar en cuenta si se está dentro de comillas
        boolean inQuotes = false;

        // Itera por los caracteres de una linea 
        for (char c : line.toCharArray()) {
            // Verifica si el caracter es una comilla
            if (c == '"') {
                inQuotes = !inQuotes; // Cambia el estado de inQuotes
            } else if (c == ',' && !inQuotes) {
                // Si el caracter es una coma y no está dentro de comillas, agrega el campo actual a la lista
                spotifyLine.add(currentField.toString());
                currentField.setLength(0); // Se limpia el campo actual
            } else {
                // Si no es una coma o una comilla, se agrega el caracter al campo actual
                currentField.append(c);
            }
        }
        // Add the last field
        spotifyLine.add(currentField.toString());

        return spotifyLine.toArray();      
    } 


    public static class PopularArtistsGenresMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            Object[] lineArray = CSVParser(line);
            // Ignora la primera línea del archivo (header)
            if(!lineArray[0].equals("id")){
                Integer artistPopularity = Integer.parseInt(lineArray[28].toString());
                if(artistPopularity > 70){ //Seleccionamos los artistas con un popularidad mayor a 70
                String genre_id = lineArray[30].toString();
                String[] genreSplit = genre_id.toString().split(" ");
                String mainGenre = genreSplit[genreSplit.length - 1];
                //Verificamos si es posible agrupar el subgenero
                if (GENRES.contains(mainGenre)) {
                    genre_id = mainGenre;
                }
                //Si el género está vacío entonces el género se pasa a Unknown
                if(genre_id.equals("")){
                    genre_id = "unknown";
                }
                context.write(new Text("{" + genre_id + "}"), new IntWritable(1));
                }
            }
            }
        }
            
        
    public static class PopularArtistsGenresReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            //Se suman los valores para obtener la cuenta de cuántos artistas pertenecen a cierto género
            Integer sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        //Se verifica la longitud de los argumentos
        if(args.length != 2){
            System.err.println("Usage: PopularArtistsGenre <input path> <output path>");
            System.exit(-1);
        }
        //Se obtiene la configuración de hadoop
        Configuration conf = getConf();
        
        //Se le da nombre al trabajo
        Job job = Job.getInstance(conf, "PopularArstistsGenre");

        //Se borran la carpeta del path de salida si ya existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //Se designan los paths de los archivos de entrada y salida
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Se designan las clases de los mappers y reducers
        job.setMapperClass(PopularArtistsGenresMapper.class);
        job.setReducerClass(PopularArtistsGenresReducer.class);

        //Se designan las clases de salida
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Se ejecuta el trabajos
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new PopularArtistsGenres(), args);
        System.exit(res);
    }
}
