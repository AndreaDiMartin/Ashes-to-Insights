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
        private static final List<String> GENRES = Arrays.asList(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "blues", "rap", "blues", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore", "trap"
        );
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
                throws IOException {
            
            Integer year = track.getYearOfRelease();
            if(year != null && year > 1){
                CharSequence genre = track.getGenreId();
                String[] genreSplit = genre.toString().split(" ");
                String mainGenre = genreSplit[genreSplit.length - 1];
                if (GENRES.contains(mainGenre)) {
                    genre = mainGenre;
                }
                collector.collect(new Pair<Integer, CharSequence>(year, genre));
            }
            }
    }
/* 
    public static class PopularGenresByYearMapper extends AvroMapper<Pair<Integer, CharSequence>, Pair<Integer, Pair<CharSequence, Integer>>> {
        @Override
        public void map(Pair<Integer, CharSequence> pair, AvroCollector<Pair<Integer, Pair<CharSequence, Integer>>> collector, Reporter reporter)
                throws IOException {
            
            collector.collect(new Pair<Integer, Pair<CharSequence, Integer>>(pair.key(), new Pair<CharSequence, Integer>(pair.value(), 1)));
        }
    }
*/
public static class GenresByYearReducer extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence>>{
    @Override
    public void reduce(Integer key, Iterable<CharSequence> values, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
            throws IOException {
        CharSequence genres = "";
        System.out.println("--------Año--------: " + key);
        for (CharSequence value : values) {
            genres = genres.toString() + ", " + value.toString();
            //System.out.println(value);
        }
        //for (CharSequence genre : genres) {
          //  System.out.println(genre);
        //}
        collector.collect(new Pair<Integer, CharSequence>(key, genres));
    }
}

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: GenresByYear <input path> <output path>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), GenresByYearMapRed.class);
        conf.setJobName("PopularGenresByYear");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, GenresByYearMapper.class);
        AvroJob.setReducerClass(conf, GenresByYearReducer.class);

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
