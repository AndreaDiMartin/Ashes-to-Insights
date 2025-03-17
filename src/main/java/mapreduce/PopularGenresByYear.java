package mapreduce;

//Parte 2 - Generos mas populares por año


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


public class PopularGenresByYear extends Configured implements Tool {
    //Se define el esquema para el archivo de entrada
    private Schema genreYear = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"genreYear\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}");
    public static class PopularGenresMapper extends AvroMapper<GenericRecord, Pair<CharSequence, Integer>> {
        @Override
        public void map(GenericRecord record,AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            //Se extrae el año 
            String year = record.get("key").toString();
            //Se divide el string que contiene todos los generos separados por coma y se guardan en un arreglo
            String[] listOfGenres = record.get("value").toString().split(", ");
            //Por cada genero se crea un par con el año y el genero como llave y un 1 como valor 
            for(String genre: listOfGenres){
                if(!genre.trim().isEmpty() && !genre.trim().equals("Unknown")){
                    collector.collect(new Pair<CharSequence, Integer>(year + " - " + genre, 1));
                }
            }
        
        }
    }

    public static class PopularGenresReducer extends AvroReducer<CharSequence, Integer, Pair<CharSequence, Integer>>{
        @Override
        public void reduce(CharSequence key, Iterable<Integer> values, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            //Se suman los valores de cada genero por año
            int sum = 0;
            for (Integer value: values){
                sum += value;
            }
            collector.collect(new Pair<>(key, sum));
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: PopularGenresByYear <input path> <output path>");
            return -1;
        }
        //Se obtiene la configuración de hadoop
        JobConf conf = new JobConf(getConf(), PopularGenresByYear.class);
        conf.setJobName("PopularGenresByYearMapRed");

        //Se borra la carpeta de salida si ya existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //Se establecen los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Se establecen las clases del mapper y el reducer
        AvroJob.setMapperClass(conf, PopularGenresMapper.class);
        AvroJob.setReducerClass(conf, PopularGenresReducer.class);

        //Se establecen los tipos de salida del mapper y el reducer
        AvroJob.setInputSchema(conf, genreYear);
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.STRING),Schema.create(Type.INT)));

        //Se ejecuta el trabajo 
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new PopularGenresByYear(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //Si el trabajo fue exitoso, se crea un archivo de texto con los resultados
        if(res == 0){
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();
            //Se recorren los archivos de salida y se crea un archivo de texto con los resultados
            for (File outputFile : outputFiles) {
                if (outputFile.getName().endsWith(".avro")) {
                String textName = outputFile.getName().replace(".avro", ".txt");
                List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string", "int");
                File textFile = new File(outputFile.getParent(), textName);
                FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con exito - PopularGenresByYear");
        } else {
            System.out.println("Trabajo falló - PopularGenresByYear");
        }
        //System.exit(res);
    }
    
}
