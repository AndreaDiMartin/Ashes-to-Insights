package mapreduce;


//Mapear tracks con key = Año y Value = Género
//Mapear Género y reducir al contar cuántos hay (por año)
//Sortear o seleccionar aquellos con las cuentas más altas (Top 5)
//Buscar que otra cosa se puede hacer con los géneros?????

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


public class PopularGenresByYearMapRed extends Configured implements Tool {
    public static class GenresByYearMapper extends AvroMapper<spotify, Pair<Integer, CharSequence>> {
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
                throws IOException {
            
            Integer year = track.getYearOfRelease();
            if(year != null){
                CharSequence genre = track.getGenreId();
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
    public static class PopularGenresByYearCounterReducer extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence[]>> {
        @Override
        public void reduce(Integer key, Iterable<CharSequence> values, AvroCollector<Pair<Integer, CharSequence[]>> collector, Reporter reporter)
                throws IOException {
            List<CharSequence> genres = new ArrayList<CharSequence>();
            System.out.println("--------Año--------: "+ key);
            for (CharSequence value : values) {
                genres.add(value);
                System.out.println(value);
            }
            collector.collect(new Pair<Integer, CharSequence[]>(key, genres.toArray()));
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: PopularGenresByYear <input path> <output path>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), PopularGenresByYearMapRed.class);
        conf.setJobName("PopularGenresByYear");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, GenresByYearMapper.class);
        //AvroJob.setMapperClass(conf, PopularGenresByYearMapper.class);
        AvroJob.setReducerClass(conf, PopularGenresByYearCounterReducer.class);

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        Schema arraySchema = Schema.create(Type.STRING);
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),arraySchema));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new PopularGenresByYearMapRed(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        if(res == 0){
            /** 
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string");
                File textFile = new File(outputFile.getParent(), textName);
                FileUtils.writeLines(textFile, records);
                }
            }*/
            System.out.println("Trabajo terminado con exito");
        } else {
            System.out.println("Trabajo falló");
        }
        System.exit(res);
    }

    
}
