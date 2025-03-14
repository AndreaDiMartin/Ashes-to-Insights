package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import classes.avro.spotify;

public class TrackWordCountAvro {

    public static class TrackMapper extends Mapper<AvroKey<spotify>, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "i", "you", "your", "she", "her", "he", "his", "they", "their", "we", "our", "it", "is", "are", "the", "a"
        ));

        @Override
        public void map(AvroKey<spotify> key, Text value, Context context) throws IOException, InterruptedException {

            String trackName = key.datum().getTrackName().toString().toLowerCase();


            String cleanedName = trackName.replaceAll("[^a-zA-Z ]", "");

            StringTokenizer tokenizer = new StringTokenizer(cleanedName);

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (!STOP_WORDS.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: TrackWordCountAvro <input avro> <output>");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "TrackWordCountAvro");
        job.setJarByClass(TrackWordCountAvro.class);
        job.setMapperClass(TrackMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        AvroJob.setInputKeySchema(job, spotify.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.createRecord("Pair", null, null, false, Arrays.asList(
            new Schema.Field("key", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)
        )));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


