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

public class PopularGenresByYearMapRed extends Configured implements Tool{
    static class PopularGenresByYearMapper extends Mapper<LongWritable, Text, IntPair, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            line = line.substring(1, line.length() - 1);
            String[] parts = line.split(", ");
            // Extract the key and value
            System.out.println(parts[0]);
            String[] yearGenre = parts[0].split(": ")[1].replace("\"", "").split(" - ");
            int count = Integer.parseInt(parts[1].split(": ")[1]);
            int year = Integer.parseInt(yearGenre[0]);
            String genre = yearGenre[1];
            //Parser
            context.write(new IntPair(year,count), new Text(genre));
        }
    }


    static class PopularGenresByYearReducer extends Reducer<IntPair, Text, IntPair, Text>{
        @Override
        protected void reduce(IntPair key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String genreList = "";
            for(Text genre: values){
                genreList = genre + "-";
            }
            context.write(key,new Text(genreList));
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, Text>{
        @Override
        public int getPartition(IntPair key, Text value, int numPartitions){
            return Math.abs(key.getFirst().get() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator{
        protected KeyComparator(){
            super(IntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if(cmp != 0){
                return cmp;
            }
            return -IntPair.compare(ip1.getSecond(), ip2.getFirst());
        }
    }

    public static class GroupCopmparator extends WritableComparator{
        protected GroupCopmparator(){
            super(IntPair.class,true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    public int run(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("Usage: PopularGenresByYear <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "PopularGenresByYearMapRed");
        //job.setJarByClass(PopularGenresByYearMapRed.class);
        
    //    job.setJobName("PopularGenresByYearMapRed");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PopularGenresByYearMapper.class);
        job.setReducerClass(PopularGenresByYearReducer.class);

        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(KeyComparator.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupCopmparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new PopularGenresByYearMapRed(), args);
        System.exit(exitCode);
    }

}


