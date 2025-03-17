package mapreduce;

//Parte 1 - Generos mas populares por año

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


import classes.avro.spotify;


public class GenresByYearMapRed extends Configured implements Tool {
    public static class GenresByYearMapper extends AvroMapper<spotify, Pair<Integer, CharSequence>> {
        //Géneros principales más comunes
        private static final List<String> GENRES = Arrays.asList(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "blues", "rap", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore", "trap"
        );
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
                throws IOException {
            //Se obtiene el año de lanzamiento de la canción
            Integer year = track.getYearOfRelease();
            //Se verifica que el año sea válido 
            if(year != null && year > 1){
                CharSequence genre = track.getGenreId();
                String[] genreSplit = genre.toString().split(" ");
                String mainGenre = genreSplit[genreSplit.length - 1];
                //Si el género se puede agrupar en uno de los géneros principales, se asigna
                if (GENRES.contains(mainGenre)) {
                    genre = mainGenre;
                }
                collector.collect(new Pair<Integer, CharSequence>(year, genre));
            }
        }
    }

public static class GenresByYearReducer extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence>>{
    @Override
    public void reduce(Integer key, Iterable<CharSequence> values, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
            throws IOException {
        //Se crea un string con los géneros separados por comas
        CharSequence genres = "";
        for (CharSequence value : values) {
            genres = genres.toString() + ", " + value.toString();
        }
        collector.collect(new Pair<Integer, CharSequence>(key, genres));
    }
}

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: GenresByYear <input path> <output path>");
            return -1;
        }
        //Se obtiene la configuración de hadoop 
        JobConf conf = new JobConf(getConf(), GenresByYearMapRed.class);
        conf.setJobName("PopularGenresByYear");

        //Se elimina el directorio de salida si ya existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //Se establecen los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Se establecen las clases del Mapper y Reducer
        AvroJob.setMapperClass(conf, GenresByYearMapper.class);
        AvroJob.setReducerClass(conf, GenresByYearReducer.class);

        //Se establecen los tipos de salida del Mapper y Reducer
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.STRING)));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new GenresByYearMapRed(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        if(res == 0){
            //En caso de que el trabajo haya sido exitoso, se crea un archivo de texto con los resultados
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "int", "string");
                File textFile = new File(outputFile.getParent(), textName);
                FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con exito - GenresByYearMapRed");
        } else {
            System.out.println("Trabajo falló - GenresByYearMapRed");
        }
    }

    
}
