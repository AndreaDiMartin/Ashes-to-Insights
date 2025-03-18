package mapreduce.genreanalysis;

//Parte 3 -  - Generos mas populares por año

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

public class SortPopularGenresByYearMapRed extends Configured implements Tool{
    static class PopularGenresByYearMapper extends Mapper<LongWritable, Text, IntPair, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            //Se eliminan las comillas de la linea y se guarda en un arreglo separando cada campo por comas
            line = line.substring(1, line.length() - 1);
            String[] parts = line.split(", ");
            //Se verifica que la linea no esté vacía 
            if(parts.length > 1){
                //Se extrae el año, el genero y el conteo de la linea
                String[] yearGenreSplit = parts[0].split(": ");
                String[] countSplit = parts[1].split(": ");
            //Se verifica que el año y el conteo no estén vacíos
            if(yearGenreSplit.length>1 && countSplit.length > 1){
                //Se separan el año y el genero
                String[] yearGenre = yearGenreSplit[1].replace("\"", "").split(" - ");
                int year = Integer.parseInt(yearGenre[0]);
                String genre = yearGenre[1];
                //Se extrae el conteo y se convierte a entero
                int count = Integer.parseInt(countSplit[1]);
                //El año y la cantidad se envían como clave y el genero como valor 
                context.write(new IntPair(year,count), new Text(genre));
            }
            }
            
        }
    }

    static class PopularGenresByYearReducer extends Reducer<IntPair, Text, IntPair, Text>{
        @Override
        protected void reduce(IntPair key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String genreList = "";
            //Se concatenan dos géneros en caso de que se repitan la misma cantidad de veces en un mismo año
            for(Text genre: values){
                genreList = genre + "-";
            }
            context.write(key,new Text(genreList));
        }
    }

    //Se crea una clase para particionar las claves por año
    public static class FirstPartitioner extends Partitioner<IntPair, Text>{
        @Override
        public int getPartition(IntPair key, Text value, int numPartitions){
            return Math.abs(key.getFirst().get() * 127) % numPartitions;
        }
    }

    //Se crea una clase para comparar las claves
    public static class KeyComparator extends WritableComparator{
        protected KeyComparator(){
            super(IntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            //Se ordenan los años de menor a mayor
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if(cmp != 0){
                return cmp;
            }
            //Se ordenan el conteo de mayor a menor
            return IntPair.compare(ip1.getSecond(), ip2.getSecond());
        }
    }

    //Se crea una clase para comparar las claves por año
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
        //Se obtiene la configuración de hadoop
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "PopularGenresByYearSSMapRed");
        
        //Se borran la carpeta del path de salida si ya existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //Se designan los paths de los archivos de entrada y salida
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Se designan las clases del mapper y el reducer
        job.setMapperClass(PopularGenresByYearMapper.class);
        job.setReducerClass(PopularGenresByYearReducer.class);

        //Se designan las clases de salida
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(Text.class);

        //Se designan las clases de comparación y partición de las claves
        job.setSortComparatorClass(KeyComparator.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupCopmparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        //Se ejecuta el trabajo
        int exitCode = ToolRunner.run(new SortPopularGenresByYearMapRed(), args);
        System.exit(exitCode);
    }

}


