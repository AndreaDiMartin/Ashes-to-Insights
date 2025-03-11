package mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import classes.avro.spotify;


public class YearCounterMapRed extends Configured implements Tool{
   public static class YearCounterMapper extends AvroMapper<spotify,Pair<Integer,Integer>>{
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer,Integer>> collector, Reporter reporter)
        throws IOException{
                Integer year = track.getYearOfRelease();
                if (year == null) {
                year = 0;
                }
                collector.collect(new Pair<Integer,Integer>(year,1));

        }
   } 
   
   public static class YearCounterReducer extends AvroReducer<Integer, Integer, Pair<Integer,Integer>>{
    @Override 
    public void reduce(Integer key, Iterable<Integer> values, AvroCollector<Pair<Integer,Integer>> collector, Reporter reporter) throws IOException{
        int sum = 0;
        for (Integer value: values){
            sum += value;
        }
        collector.collect(new Pair<Integer,Integer>(key,sum));
    }  

   }

   public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: YearCounterMapRed <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), YearCounterMapRed.class);
        conf.setJobName("YearCounterMapRed");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, YearCounterMapper.class);
        AvroJob.setReducerClass(conf, YearCounterReducer.class);

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.INT)));

        JobClient.runJob(conf);
        return 0;
   }

   public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new YearCounterMapRed(), args);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);


    if(res == 0){
        File outputDir = new File(args[1]);
        File[] outputFiles = outputDir.listFiles();
        for (File outputFile : outputFiles) {
            if (outputFile.getName().endsWith(".avro")) {
            String textName = outputFile.getName().replace(".avro", ".txt");
            List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath());
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
