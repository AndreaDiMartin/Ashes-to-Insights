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

public class LessPopularYear extends Configured implements Tool { 
    // Clase Mapper para procesar cada registro y emitir el año y el conteo de canciones impopulares
    public static class ImpopularityMapper extends AvroMapper<spotify, Pair<Integer, Integer>> {

        @Override
        public void map(spotify spotifyRecord, AvroCollector<Pair<Integer, Integer>> collector, org.apache.hadoop.mapred.Reporter reporter) throws IOException {
            Integer year =  spotifyRecord.getYearOfRelease();
            Integer popularity = (Integer) spotifyRecord.getPopularity();
            
            // Manejar año nulo o invalido
            if (year == null || year == 1) {
                year = 0;
            }

            // Recoger registros con popularidad < 50
            if (popularity < 50) {
                collector.collect(new Pair<>(year, 1));
            }
        }
    }

    // Clase Reducer para agregar los conteos de canciones impopulares por año
    public static class ImpopularityReducer extends AvroReducer<Integer, Integer, Pair<Integer, Integer>> {
        
        @Override
        public void reduce(Integer key, Iterable<Integer> values, AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter) throws IOException {
            int count = 0;
            for (Integer value : values) {
                count += value;
            }
            collector.collect(new Pair<>(key, count));
        }
    }

    // Metodo principal para configurar y ejecutar el trabajo MapReduce
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: LessPopularYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), LessPopularYear.class);
        conf.setJobName("LessPopularYearMapRed");

        // Eliminar la ruta de salida si existe
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Establecer rutas de entrada y salida
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Establecer clases Mapper y Reducer
        AvroJob.setMapperClass(conf, ImpopularityMapper.class);
        AvroJob.setReducerClass(conf, ImpopularityReducer.class);

        // Establecer esquemas de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf ,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.INT)));

        // Ejecutar el trabajo
        JobClient.runJob(conf);
        return 0;
    }

    // Metodo principal para ejecutar el trabajo y manejar la conversión de salida
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new LessPopularYear(), args);
    
        if(res == 0){
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
            System.out.println("Trabajo terminado con exito");
        } else {
            System.out.println("Trabajo falló");
        }
        
        System.exit(res);
    }
}