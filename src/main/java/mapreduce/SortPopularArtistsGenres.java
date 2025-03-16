package mapreduce;

//Parte 2 - Popular Artists Genres (Sorter)

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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.IntPair;
import mapreduce.SortPopularGenresByYearMapRed.PopularGenresByYearMapper;
import mapreduce.SortPopularGenresByYearMapRed.PopularGenresByYearReducer;

public class SortPopularArtistsGenres extends Configured implements Tool{
    static class PopularGenresByArtistMapper extends Mapper<LongWritable, Text, TextPair, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            // Split the line by one or more spaces
            String[] parts = line.split("\\s+");
            if (parts.length >= 2) {
                String genre = parts[0].substring(1, parts[0].length() - 1).trim(); // Remove the curly braces
                try {
                    int count = Integer.parseInt(parts[parts.length - 1]);
                    context.write(new TextPair(genre, String.valueOf(count)), NullWritable.get());
                } catch (NumberFormatException e) {
                    // Log and skip the line if the count is not a valid integer
                    System.err.println("Skipping line due to invalid count: " + line);
                }
            }
        }
    }


    static class PopularGenresByArtistReducer extends Reducer<TextPair, NullWritable, TextPair, NullWritable>{
        @Override
        protected void reduce(TextPair key,Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
            context.write(key,NullWritable.get());
        }
    }

    public static class FirstPartitioner extends Partitioner<TextPair, NullWritable>{
        @Override
        public int getPartition(TextPair key, NullWritable value, int numPartitions){
            return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator{
        protected KeyComparator(){
            super(TextPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;
            int cmp = TextPair.compare(ip1.getFirst(), ip2.getFirst());
            if(cmp != 0){
                return cmp;
            }
            return TextPair.compare(ip1.getSecond(), ip2.getSecond());
        }
    }

    public static class GroupCopmparator extends WritableComparator{
        protected GroupCopmparator(){
            super(TextPair.class,true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;
            return TextPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    public int run(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("Usage: SortPopularGenresByArtists <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "PopularGenresByArtistSSMapRed");
        //job.setJarByClass(PopularGenresByYearMapRed.class);
        
    //    job.setJobName("PopularGenresByYearMapRed");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PopularGenresByArtistMapper.class);
        job.setReducerClass(PopularGenresByArtistReducer.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(NullWritable.class);

        job.setSortComparatorClass(KeyComparator.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupCopmparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SortPopularArtistsGenres(), args);
        System.exit(exitCode);
    }

}

