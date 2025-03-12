//Mapper para agrupar por genero y contar canciones de cada género
//Generos:
//Hip-hop
//Country
//Rock
//Jazz
//Pop
//Reggae
//Metal
//Blues
//Rap


package mapreduce;

import java.io.IOException;
import java.io.OutputStreamWriter;
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


public class GenreCounterMapRed extends Configured implements Tool{
    public static class GenreCounterMapper extends AvroMapper<spotify, Pair<CharSequence, Integer>> {
        private static final List<String> GENRES = Arrays.asList(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "blues", "rap", "blues", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore"
        );

        @Override
        public void map(spotify track, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            CharSequence genre = track.getGenreId();
            String[] genreSplit = genre.toString().split(" ");
            String mainGenre = genreSplit[genreSplit.length - 1];
            if (GENRES.contains(mainGenre)) {
                genre = mainGenre;
            }
            collector.collect(new Pair<CharSequence, Integer>(genre, 1));
        }

    }
    public static class GenreCounterReducer extends AvroReducer<CharSequence, Integer, Pair<CharSequence,Integer>>{
        @Override 
        public void reduce(CharSequence key, Iterable<Integer> values, AvroCollector<Pair<CharSequence,Integer>> collector, Reporter reporter) throws IOException{
            int sum = 0;
            for (Integer value: values){
                sum += value;
            }
            collector.collect(new Pair<CharSequence,Integer>(key,sum));
        }  
    
       }

     public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: GenreCounterMapRed <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), GenreCounterMapRed.class);
        conf.setJobName("GenreCounterMapRed");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, GenreCounterMapper.class);
        AvroJob.setReducerClass(conf, GenreCounterReducer.class);

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.STRING),Schema.create(Type.INT)));

        JobClient.runJob(conf);
        return 0;
   }

   public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new GenreCounterMapRed(), args);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);


    if(res == 0){
        File outputDir = new File(args[1]);
        File[] outputFiles = outputDir.listFiles();
        for (File outputFile : outputFiles) {
            if (outputFile.getName().endsWith(".avro")) {
            String textName = outputFile.getName().replace(".avro", ".txt");
            List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string");
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
