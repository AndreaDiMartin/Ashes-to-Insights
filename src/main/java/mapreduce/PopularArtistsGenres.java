package mapreduce;

//Artistas con popularidad mayor a 60 y sus generos

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

import mapreduce.SortPopularGenresByYearMapRed.FirstPartitioner;
import mapreduce.SortPopularGenresByYearMapRed.GroupCopmparator;
import mapreduce.SortPopularGenresByYearMapRed.KeyComparator;
import mapreduce.SortPopularGenresByYearMapRed.PopularGenresByYearMapper;
import mapreduce.SortPopularGenresByYearMapRed.PopularGenresByYearReducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularArtistsGenres extends Configured implements Tool {
    private static Object[] CSVParser(String line){
        List<String> spotifyLine = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        // Create a flag to keep track of whether we're inside quotes
        boolean inQuotes = false;

        // Iterate over the characters in the line
        for (char c : line.toCharArray()) {
            // Check if the character is a quote
            if (c == '"') {
                // If it is, toggle the inQuotes flag
                inQuotes = !inQuotes; // Toggle the inQuotes flag
            } else if (c == ',' && !inQuotes) {
                // If we encounter a comma and we're not inside quotes, it's the end of a field
                spotifyLine.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
            } else {
                // Otherwise, just add the character to the current field
                currentField.append(c);
            }
        }
        // Add the last field
        spotifyLine.add(currentField.toString());

        return spotifyLine.toArray();      
    } 


    public static class PopularArtistsGenresMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            Object[] lineArray = CSVParser(line);
            if(!lineArray[0].equals("id")){
                Integer artistPopularity = Integer.parseInt(lineArray[28].toString());
                if(artistPopularity > 65){
                String genre_id = lineArray[30].toString();
                System.out.println(lineArray[28] +  "," + lineArray[30]);
                context.write(new Text(genre_id), new IntWritable(1));
                }
            }
            }
        }
            
        
    public static class PopularArtistsGenresReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Integer sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: PopularArtistsGenre <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "PopularArstistsGenre");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PopularArtistsGenresMapper.class);
        job.setReducerClass(PopularArtistsGenresReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new PopularArtistsGenres(), args);
        System.exit(res);
    }
}
