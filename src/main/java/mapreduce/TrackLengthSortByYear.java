package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TrackLengthSortByYear {

    public static class TrackMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text yearTrackKey = new Text();
        private IntWritable lengthValue = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); 

            String track = fields[1].trim().replaceAll("^\"|\"$", "");
            String year = fields[18].trim();

            try {
                int length = track.length();
                yearTrackKey.set(year + "," + track);
                lengthValue.set(length);
                context.write(yearTrackKey, lengthValue);
            } catch (Exception e) {

            }
        }
    }

    public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, List<TrackEntry>> yearTrackMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            String[] parts = key.toString().split(",", 2);

            String year = parts[0];
            String trackName = parts[1];

            for (IntWritable val : values) {
                yearTrackMap.computeIfAbsent(year, k -> new ArrayList<>())
                            .add(new TrackEntry(trackName, val.get()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, List<TrackEntry>> entry : yearTrackMap.entrySet()) {
                String year = entry.getKey();
                List<TrackEntry> trackList = entry.getValue();

                trackList.sort(Comparator.comparingInt(TrackEntry::getLength).reversed());

                for (TrackEntry track : trackList) {
                    context.write(new Text(year + " - " + track.getName()), new IntWritable(track.getLength()));
                }
            }
        }

        private static class TrackEntry {
            private final String name;
            private final int length;

            public TrackEntry(String name, int length) {
                this.name = name;
                this.length = length;
            }

            public String getName() {
                return name;
            }

            public int getLength() {
                return length;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TrackLengthSortByYear <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "track length sort by year");
        job.setJarByClass(TrackLengthSortByYear.class);
        job.setMapperClass(TrackMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

