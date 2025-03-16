package mapreduce;

import java.io.IOException;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


// Esquemas usados
import classes.avro.spotify;
import classes.avro.PopularityAnalysis;


public class AlbumPopularityAnalysis extends Configured implements Tool {

    public static class AlbumPopularityAnalysisMapper 
    extends AvroMapper<spotify, Pair<Integer, PopularityAnalysis>> 
    {

        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, PopularityAnalysis>> collector, 
                        Reporter reporter)
        throws IOException 
        {

            Integer year = track.getYearOfRelease();

            // Dificultades tecnicas con la union en los tipos de Avro
            Integer p = (track.get("album_popularity") instanceof Integer) ? 
                        (Integer) track.get("album_popularity") : null;
 
            if (year != null && year != 1 && p != null) 
            {
                PopularityAnalysis PA = new PopularityAnalysis(1, p, p, p, 0, 0.0, 0.0, 0.0, 0.0);
                collector.collect(new Pair<Integer, PopularityAnalysis>(year, PA));
            } 

        }
    }

    public static class AlbumPopularityAnalysisReducer 
        extends AvroReducer<Integer, 
                            PopularityAnalysis, 
                            Pair<Integer, PopularityAnalysis>> 
    {

        public static double calculateMedian(Integer[] data) {

            int n = data.length;
            
            if (n % 2 == 0) {
                return (data[n / 2 - 1] + data[n / 2]) / 2.0;
            } else {
                return data[n / 2];
            }
        }

        @Override
        public void reduce( Integer key, 
                            Iterable<PopularityAnalysis> values, 
                            AvroCollector<Pair<Integer,PopularityAnalysis>> collector,
                            Reporter reporter) 
        throws IOException 
        {

            // Cantidad de items
            int n = 0;

            // Max
            Integer max = Integer.MIN_VALUE;

            // Min
            Integer min = Integer.MAX_VALUE;

            // Dict para determinar moda
            Map<Integer, Integer> popularityValuesMap = new HashMap<>();

            // Lista para calcular cuartiles 
            List<Integer> popularityValuesList = new ArrayList<>();

            // El rango, la media y el IQR se calculan al final

            for (PopularityAnalysis PA : values) 
            {
                // Sabemos que la mayoria de los campos tienen la 
                // popularidad asociada al album
                Integer p = PA.getMode();

                if (p > max){
                    max = Integer.valueOf(p);
                }

                if (p < min){
                    min = Integer.valueOf(p);
                }

                popularityValuesMap.putIfAbsent(Integer.valueOf(p), 0);
                Integer count = popularityValuesMap.get(Integer.valueOf(p));
                popularityValuesMap.put(Integer.valueOf(p), count + 1);

                popularityValuesList.add(Integer.valueOf(p));
                n++;
                
            }

            // Calculo de cuartiles
            double q1 = 0.0, q2 = 0.0, q3 = 0.0;

            if (n == 1){
                q1 = popularityValuesList.get(0);
                q2 = popularityValuesList.get(0);
                q3 = popularityValuesList.get(0);
            } else if (n > 1)
            {
                Integer[] popularityValuesArray = popularityValuesList.toArray(new Integer[0]);

                // Hay que ordenar el arreglo y calcular los cuartiles
                Arrays.sort(popularityValuesArray);

                q2 = calculateMedian(popularityValuesArray);

                // Dividir el arreglo para Q1 y Q3
                Integer[] lowerHalf = Arrays.copyOfRange(popularityValuesArray, 0, n / 2);

                int startIndex;
                if (n % 2 == 0) {
                    startIndex = n / 2;
                } else {
                    startIndex = n / 2 + 1;
                }
            
                Integer[] upperHalf = Arrays.copyOfRange(popularityValuesArray, startIndex, n);

                // Q1 (mediana de la mitad inferior)
                q1 = calculateMedian(lowerHalf);

                // Q3 (mediana de la mitad superior)
                q3 = calculateMedian(upperHalf);
            }

    
            // A partir de las llaves del diccionario hay que determinar cual
            // popularidad se repite mas 
            Integer modeKey = -1;
            Integer mode = Integer.MIN_VALUE;
            Set<Integer> keys = popularityValuesMap.keySet();
            for (Integer popkey : keys) 
            {
                if (popularityValuesMap.get(popkey) > mode) 
                {
                    mode = Integer.valueOf(popularityValuesMap.get(popkey));
                    modeKey = Integer.valueOf(popkey);
                }
            }

                
            PopularityAnalysis PA = new PopularityAnalysis(n, min, max, mode,
                                                            max - min, q1, q2, q3, q3 - q1 );
            collector.collect(new Pair<Integer, PopularityAnalysis>(key , PA));
        }
    }

    // Configura y ejecuta el trabajo de MapReduce.
    // args:  Argumentos de la línea de comandos: [ruta de entrada] [ruta de salida]
    // retorna  0 si el trabajo se completa con éxito, un valor distinto de cero si falla.
    public int run(String[] args) throws Exception 
    {
        // Indicaciones de uso
        if (args.length != 2) 
        {
            System.err.println("Uso: AlbumPopularityAnalysis <input path> <output path>");
            return -1;
        }

        // Configuración del trabajo
        JobConf conf = new JobConf(getConf(), AlbumPopularityAnalysis.class);
        conf.setJobName("Mostrar un análisis por año de la popularidad de los albumes");

        // Si el path de salida provisto ya existe, se elimina
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Establecemos los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Establecemos el mappper y reducer
        AvroJob.setMapperClass(conf, AlbumPopularityAnalysisMapper.class);
        AvroJob.setReducerClass(conf, AlbumPopularityAnalysisReducer.class);

        // Especificamos el esquema Avro de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        Schema PASchema = PopularityAnalysis.getClassSchema();
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT), PASchema));

        // Ejecuta el trabajo de MapReduce
        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        // Ejecuta el trabajo de MapReduce utilizando ToolRunner
        int res = ToolRunner.run(new Configuration(), new AlbumPopularityAnalysis(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        // Procesa la salida si el trabajo fue exitoso
        if (res == 0) 
        {

            System.out.println("Trabajo terminado con éxito");

            //Comienzo del proceso de deserialización
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();

            for (File outputFile : outputFiles) 
            {
                if (outputFile.getName().endsWith(".avro")) 
                {
                    String textName = outputFile.getName().replace(".avro", ".txt");

                    List<String> records = DeserializationData.getPairIntPopularityAnalysisRecords(outputFile.getAbsolutePath());
                    
                    File textFile = new File(outputFile.getParent(), textName);

                    FileUtils.writeLines(textFile, records);
                }
            }

        } else 
        {
            System.out.println("El trabajo falló");
        }
        
        System.exit(res);
    }

}




