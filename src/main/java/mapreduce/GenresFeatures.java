package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//Separar por decadas

import classes.avro.spotify;

public class GenresFeatures extends Configured implements Tool{
        //Función para separar por decadas el año de publicacion de una canción
        private static String DecadeParser(Integer year){
        if(year >= 1800 && year < 1900){
            return "1800s";
        } else if (year >= 1900 && year < 1910) {
            return "1900s";
        } else if (year >= 1910 && year < 1920) {
            return "1910s";
        } else if (year >= 1920 && year < 1930) {
            return "1920s";
        } else if (year >= 1930 && year < 1940) {
            return "1930s";
        } else if (year >= 1940 && year < 1950) {
            return "1940s";
        } else if (year >= 1950 && year < 1960) {
            return "1950s";
        } else if (year >= 1960 && year < 1970) {
            return "1960s";
        } else if (year >= 1970 && year < 1980) {
            return "1970s";
        } else if (year >= 1980 && year < 1990) {
            return "1980s";
        } else if (year >= 1990 && year < 2000) {
            return "1990s";
        } else if (year >= 2000 && year < 2010) {
            return "2000s";
        } else if (year >= 2010 && year < 2020) {
            return "2010s";
        } else {
            return "2020s";
        }
    }


     public static class GenresFeatureMapper extends AvroMapper<spotify, Pair<CharSequence, CharSequence>> {
        @Override
        public void map(spotify track, AvroCollector<Pair<CharSequence, CharSequence>> collector, Reporter reporter)
        throws IOException {
            //Género a seleccionar
            String genreToSelect = "pop";
            Integer year = track.getYearOfRelease();
            CharSequence genre_id = track.getGenreId(); 
            String[] genreSplit = genre_id.toString().split(" ");
            String mainGenre = genreSplit[genreSplit.length - 1];
            //Verificamos si es posible agrupar el subgenero
            if (mainGenre.equals(genreToSelect)) {
                genre_id = mainGenre;
            }      
            if(year != null && year > 1 && genre_id.equals(genreToSelect)){
                //Casting de los valores de las caracteristicas a seleccionar
                Integer explicit = (Integer) track.getExplicit();
                Float energy = (Float) track.getEnergy();
                Float loudness = (Float) track.getLoudness();
                Float valence = (Float) track.getValence(); 
                Float acousticness = (Float) track.getAcousticness();
                String decade = DecadeParser(year);
                collector.collect(new Pair<CharSequence, CharSequence>(decade, genre_id + "/" + explicit.toString() + "/" + energy.toString() + "/" + loudness.toString() + "/" + valence.toString() + "/" + acousticness.toString()));
                
            }
        }
    }

public static class GenresFeatureReducer extends AvroReducer<CharSequence, CharSequence, Pair<CharSequence, CharSequence>>{
    @Override
    public void reduce(CharSequence key, Iterable<CharSequence> values, AvroCollector<Pair<CharSequence, CharSequence>> collector, Reporter reporter)
            throws IOException {
        Float explicitMean = 0.0f;
        Float energyMean = 0.0f;
        Float loudnessMean = 0.0f;
        Float valenceMean = 0.0f; 
        Float acousticnessMean = 0.0f;
        CharSequence genre = "";
        Integer num = 0;        
        //Por cada decada, se encuentra el promedio de cada caracteristica
        for (CharSequence value : values) {
            String[] features = value.toString().split("/");
            explicitMean += Float.parseFloat(features[1]);
            energyMean += Float.parseFloat(features[2]);
            loudnessMean += Float.parseFloat(features[3]);
            valenceMean += Float.parseFloat(features[4]);
            acousticnessMean += Float.parseFloat(features[5]);
            num += 1;
            genre = features[0];
        }
        if (num > 0) {
            explicitMean /= num;
            energyMean /= num;
            loudnessMean /= num;
            valenceMean /= num;
            acousticnessMean /=num;
        }
        collector.collect(new Pair<CharSequence, CharSequence>(key, genre + "/" + explicitMean.toString() + "/" + energyMean.toString() + "/"  + loudnessMean.toString() + "/" + valenceMean.toString() + "/" + acousticnessMean.toString()));
    }
}

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: GenresByYear <input path> <output path>");
            return -1;
        }
        //Se obtiene la configuración de hadoop y se nombra el trabajo
        JobConf conf = new JobConf(getConf(), GenresByYearMapRed.class);
        conf.setJobName("FeaturesGenre");

        //Se borra la carpeta de salida si ya existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //Se designan los paths de los archivos de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Se designan las clases de los mappers y reducers
        AvroJob.setMapperClass(conf, GenresFeatureMapper.class);
        AvroJob.setReducerClass(conf, GenresFeatureReducer.class);

        //Se designan los tipos de salida del mapper y el reducer
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.STRING),Schema.create(Type.STRING)));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new GenresFeatures(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        if(res == 0){
            //Se crea un archivo de texto con los resultados de la salida usando el deserializador de avro
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string", "string");
                File textFile = new File(outputFile.getParent(), textName);
                FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con exito");
        } else {
            System.out.println("Trabajo falló");
        }
        System.exit(res);
    }
}
