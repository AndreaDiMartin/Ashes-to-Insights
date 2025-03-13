package mapreduce;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PopularGenresByYear extends Configured implements Tool {
    private Schema genreYear = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"genreYear\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}");
    public static class PopularGenresMapper extends AvroMapper<GenericRecord, Pair<CharSequence, Integer>> {
        @Override
        public void map(GenericRecord record,AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            String year = record.get("key").toString();
            String[] listOfGenres = record.get("value").toString().split(", ");
            for(String genre: listOfGenres){
                if(!genre.trim().isEmpty()){
                    collector.collect(new Pair<CharSequence, Integer>(year + " - " + genre, 1));
                }
            }
        
        }
    }

    public static class PopularGenresReducer extends AvroReducer<CharSequence, Integer, Pair<CharSequence, Integer>>{
        @Override
        public void reduce(CharSequence key, Iterable<Integer> values, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            int sum = 0;
            for (Integer value: values){
                sum += value;
            }
            collector.collect(new Pair<>(key, sum));
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: PopularGenresByYear <input path> <output path>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), PopularGenresByYear.class);
        conf.setJobName("PopularGenresByYearMapRed");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, PopularGenresMapper.class);
        AvroJob.setReducerClass(conf, PopularGenresReducer.class);

        AvroJob.setInputSchema(conf, genreYear);
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.STRING),Schema.create(Type.INT)));
        
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new PopularGenresByYear(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        if(res == 0){
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string", "int");
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
