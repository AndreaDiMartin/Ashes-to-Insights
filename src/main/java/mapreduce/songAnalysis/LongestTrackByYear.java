package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import classes.avro.spotify;


public class LongestTrackByYear extends Configured implements Tool {

    public static class TrackMapper extends AvroMapper<spotify, Pair<Integer, Integer>> {
        
        @Override
        public void map(spotify spotifyRecord, AvroCollector<Pair<Integer, Integer>> collector, org.apache.hadoop.mapred.Reporter reporter) throws IOException {
            Integer yearOfRelease = spotifyRecord.getYearOfRelease();
            CharSequence trackNameUtf8 = spotifyRecord.getTrackName();

            if (yearOfRelease == null) {
                yearOfRelease = 0;
            }

            String trackName = trackNameUtf8 != null ? trackNameUtf8.toString() : "";
            trackName = trackName.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase();
            
            collector.collect(new Pair<>(yearOfRelease, trackName.length()));
        }
    }

    public static class LongestTrackReducer extends AvroReducer<Integer, Integer, Pair<Integer, Integer>> {
        @Override
        public void reduce(Integer key, Iterable<Integer> values, AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter) throws IOException {
            int maxLength = 0;
            for (Integer length : values) {
                maxLength = Math.max(maxLength, length);
            }
            collector.collect(new Pair<>(key, maxLength));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LongestTrackByYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), LongestTrackByYear.class);
        conf.setJobName("LongestTrackByYear");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
    
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
        AvroJob.setMapperClass(conf, TrackMapper.class);
        AvroJob.setReducerClass(conf, LongestTrackReducer.class);

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), Schema.create(Type.INT)));
    
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LongestTrackByYear(), args);

        if (res == 0) {
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "int", "int");
                File textFile = new File(outputFile.getParent(), textName);
                FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Job executed successfully");
        } else {
            System.out.println("Job failed");
        }

        System.exit(res);
    }
}

