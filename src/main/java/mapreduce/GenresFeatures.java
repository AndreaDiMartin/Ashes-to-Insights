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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


import classes.avro.spotify;

//Caracteristicas de los generos a traves de los años
//Caracteristicas: 
//Explicit, energy, loudness, valence
// Parte 1 - Seleccionar un género por input y agrupar las caracteristicas de ese genero por año
// Parte 2 - Promedio de caracteristicas por año
// Parte 3 - Porcentajes????? MAP POR FEATURE?????

public class GenresFeatures extends Configured implements Tool{
     public static class GenresFeatureMapper extends AvroMapper<spotify, Pair<Integer, CharSequence>> {
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
        throws IOException {
            String genreToSelect = "pop";
            Integer year = track.getYearOfRelease();
            CharSequence genre_id = track.getGenreId(); 
            String[] genreSplit = genre_id.toString().split(" ");
            String mainGenre = genreSplit[genreSplit.length - 1];
            if (mainGenre.equals(genreToSelect)) {
                genre_id = mainGenre;
            }      
            if(year != null && year > 1 && genre_id.equals(genreToSelect)){
                Integer explicit = (Integer) track.getExplicit();
                Float energy = (Float) track.getEnergy();
                Float loudness = (Float) track.getLoudness();
                Float valence = (Float) track.getValence(); 
                Float accousticness = (Float) track.getAcousticness();
                //POPULARIDAD????
                collector.collect(new Pair<Integer, CharSequence>(year, genre_id + "/" + explicit.toString() + "/" + energy.toString() + "/" + loudness.toString() + "/" + valence.toString()));
                
            }
        }
    }

public static class GenresFeatureReducer extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence>>{
    @Override
    public void reduce(Integer key, Iterable<CharSequence> values, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
            throws IOException {
        Float explicitMean = 0.0f;
        Float energyMean = 0.0f;
        Float loudnessMean = 0.0f;
        Float valenceMean = 0.0f; 
        CharSequence genre = "";
        Integer num = 0;        
        for (CharSequence value : values) {
            String[] features = value.toString().split("/");
            explicitMean += Float.parseFloat(features[1]);
            energyMean += Float.parseFloat(features[2]);
            loudnessMean += Float.parseFloat(features[3]);
            valenceMean += Float.parseFloat(features[4]);
            num += 1;
            genre = features[0];
        }
        if (num > 0) {
            explicitMean /= num;
            energyMean /= num;
            loudnessMean /= num;
            valenceMean /= num;
        }
        System.out.println(valenceMean);
        collector.collect(new Pair<Integer, CharSequence>(key, genre + "-" + explicitMean.toString() + "-" + energyMean.toString() + "-"  + loudnessMean.toString() + "-" + valenceMean.toString()));
    }
}

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: GenresByYear <input path> <output path>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), GenresByYearMapRed.class);
        conf.setJobName("FeaturesGenre");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, GenresFeatureMapper.class);
        AvroJob.setReducerClass(conf, GenresFeatureReducer.class);

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.STRING)));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new GenresFeatures(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        if(res == 0){
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
            System.out.println("Trabajo terminado con exito");
        } else {
            System.out.println("Trabajo falló");
        }
        System.exit(res);
    }
}
