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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import javax.xml.validation.Schema;

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

import classes.avro.Spotify;


public class YearCounterMapRed extends Configured implements Tool{
   public static class YearCounterMapper extends AvroMapper<Spotify,Pair<Integer,Integer>{
        @Override
        public void map(Spotify track, AvroCollector<Pair<Integer,Integer>> collector, Reporter reporter)
        throws IOException{
                Integer year = track.getYear();
                collector.collect(new Pair<Integer,Integer>(year,1));

        }
   } 
   
   public static class YearCounterReducer extends AvroReducer<Integer, Integer, Pair<Integer,Integer>>{
    @Override 
    public void reduce(Integer key, Iterable<Integer> values, AvroCollector<Pair<Integer,Integer>> collector, Reporter reporter){
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

        java.nio.file.Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, YearCounterMapper.class);
        AvroJob.setReducerClass(conf, YearCounterReducer.class);

        AvroJob.setInputSchema(conf, Spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.INT)));

        JobClient.runJob(conf);
        return 0;
   }

   public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new YearCounterMapRed(), args);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if(res == 0){
        Path outputdir = new Path(args[1]);
        FileStatus[] outputfiles = fs.listStatus(outputDir);
        for(FileStatus outputFile: outputfiles){
            if(outputFile.getPath().getName().endsWith("avro")){
                String route = outputFile.getPath().toString().replace("file://", "").replace(".avro",".txt");
                List<String> records = DeserializationData.getRecords(route);
                System.out.println("Aqui no funciona");
          Path outputPath = new Path(route);
          if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
          }
          FSDataOutputStream outputStream = fs.create(outputPath);
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) {
            for (String line : records) {
              writer.write(line.toString());
              writer.newLine();
            }
          }

            }
        }
        System.out.println("Trabajo terminado con exito");
    } else {
        System.out.println("Trabajo falló");
    }
    System.exit(res);
}
}
