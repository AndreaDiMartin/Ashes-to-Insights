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

public class SortGenresByYear extends Configured implements Tool {
    private Schema genreYearCount = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"genreYear\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}");
    public static class SortPopularGenresMapper extends AvroMapper<GenericRecord, Pair<Integer, CharSequence>> {
        @Override
        public void map(GenericRecord record,AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
                throws IOException {
            String yearGenre = record.get("key").toString();
            String[] yearGenreSplit = yearGenre.split(" - ");
            Integer year = Integer.parseInt(yearGenreSplit[0]);
            CharSequence genre = yearGenreSplit[1];
            Integer count = Integer.parseInt(record.get("value").toString());
            Integer genreCount = year + count;
            collector.collect(new Pair<Integer, CharSequence>(genreCount, genre));
        }
    }

    public static class SortPopularGenresReducer extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence>>{
        @Override
        public void reduce(Integer key, Iterable<CharSequence> values, AvroCollector<Pair<Integer, CharSequence>> collector, Reporter reporter)
                throws IOException {
            String genre = "";
            for (CharSequence value: values){
                genre += value.toString() + ", ";
            }
            collector.collect(new Pair<>(key, genre));
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: SortPopularGenresByYear <input path> <output path>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), SortGenresByYear.class);
        conf.setJobName("SortPopularGenresByYearMapRed");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, SortPopularGenresMapper.class);
        AvroJob.setReducerClass(conf, SortPopularGenresReducer.class);

        AvroJob.setInputSchema(conf, genreYearCount);
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.STRING)));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new SortGenresByYear(), args);
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
