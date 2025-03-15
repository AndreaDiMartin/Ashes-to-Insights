package mapreduce;

import java.io.IOException;
import java.util.List;
import java.io.File;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// Esquemas usados
import classes.avro.spotify;
import classes.avro.MonthValue;


// MapReduce para contar el número de álbumes publicados por mes para cada año.
public class AlbumsPerMonth extends Configured implements Tool {


    // Mapper: - Extrae el año, el mes de lanzamiento y el nombre del álbum 
    //         de cada registro de Spotify.
    //          - La salida del mapper es un par (Año, MonthValue), donde MonthValue 
    //          contiene el mes y el nombre del álbum.
    public static class AlbumsPerMonthMapper 
    extends AvroMapper<spotify, Pair<Integer, MonthValue>> 
    {

        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, MonthValue>> collector, 
                        Reporter reporter)
        throws IOException {

            // Accedemos al nombre del album y su año y mes de salida
            Integer year = track.getYearOfRelease();
            Integer month = track.getMonthOfRelease();
            CharSequence albumName = track.getAlbumName();

            // Si todos los valores son distintos a null (y el año no es uno)
            // Agregamos el par donde la key es el año y el valor es un nuevo
            // esquema Avro que representa los valores de un mes (Un entero para el mes
            // y un string para el nombre del album)
            if (year != null && year != 1 && month != null &&  albumName != null) {
                MonthValue monthValue = new MonthValue(month, albumName);
                collector.collect(new Pair<Integer, MonthValue>(year, monthValue));
            } 

            // Si el año es null o 1, agregamos los valores al par con key especial 0
            if ((year == null || year == 1) && month != null &&  albumName != null) {
                MonthValue monthValue = new MonthValue(month, albumName);
                collector.collect(new Pair<Integer, MonthValue>(0, monthValue));
            } 

        }
    }

    // Reducer:  - Cuenta la cantidad de álbumes únicos por mes para cada año.
    //           - La entrada  es un año y una lista de MonthValue 
    //              (mes y nombre del álbum) asociados a ese año.
    //           - La salida  es un par (Año, MonthValue), donde MonthValue contiene el mes 
    //              y la cantidad de álbumes únicos para ese mes.
    public static class AlbumsPerMonthReducer 
    extends AvroReducer<Integer, MonthValue, Pair<Integer, MonthValue>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<MonthValue> values, 
                            AvroCollector<Pair<Integer, MonthValue>> collector,
                            Reporter reporter) 
        throws IOException {

            // Arreglo para llevar un set de albumes unicos por cada mes del año
            Set<CharSequence>[] uniqueAlbumsPerMonth = new HashSet[12];

            // Inicializamos los sets
            for (int i = 0; i < uniqueAlbumsPerMonth.length; i++) {
                uniqueAlbumsPerMonth[i] = new HashSet<>();
            }


            // Aprovechamos que los meses estan representados como enteros
            // para acceder al set del mes correspondiente
            for (MonthValue albumValue : values) {
                Integer month = albumValue.getMonth();
                CharSequence album = albumValue.getAlbums(); 
                uniqueAlbumsPerMonth[month - 1].add(album);
            }

            // Con el set de cada mes listo, vamos a crear un par por cada uno 
            // donde el key es el año y MonthValue ahora tiene el tam del set que 
            // le correspondia al mes  (Volviendo a hacer el truco de pasarlo a string)
            Integer numAlbums;
            for (int i = 0; i < uniqueAlbumsPerMonth.length; i++) {

                numAlbums = uniqueAlbumsPerMonth[i].size();
                MonthValue monthValue = new MonthValue(i + 1, String.valueOf(numAlbums));

                collector.collect(new Pair<>( key , monthValue));
            }          
        }
    }

    // Configura y ejecuta el trabajo de MapReduce.
    // args:  Argumentos de la línea de comandos: [ruta de entrada] [ruta de salida]
    // retorna  0 si el trabajo se completa con éxito, un valor distinto de cero si falla
    public int run(String[] args) throws Exception 
    {

        
        if (args.length != 2) 
        {
            System.err.println("Uso: AlbumsPerMonthOfEachYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), AlbumsPerMonth.class);
        conf.setJobName("Contar el numero de albums publicados por mes de cada año");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, AlbumsPerMonthMapper.class);
        AvroJob.setReducerClass(conf, AlbumsPerMonthReducer.class);

        // Esquemas a usar de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT), MonthValue.getClassSchema()));


        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        int res = ToolRunner.run(new Configuration(), new AlbumsPerMonth(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (res == 0) 
        {
            // Accedemos al directorio donde se encuentra el output del MapReduce
            File outputDir = new File(args[1]);
            // Enlistamos los archivos de este directorio
            File[] outputFiles = outputDir.listFiles();

            // Vamos a interar sobre el arreglo
            for (File outputFile : outputFiles) {
                // Nos interesa el archivo .avro con nuestros datos serializados
                if (outputFile.getName().endsWith(".avro")) {

                    // Creamos el nombre para nuestro archivo txt 
                    String textName = outputFile.getName().replace(".avro", ".txt");

                    // Llamamos a la funcion espeficia para deserializar el esquema usado
                    List<String> records = DeserializationData.getPairIntAlbumValueRecords(outputFile.getAbsolutePath());
                    // Creamos el archivo con los registros obtenidos 
                    File textFile = new File(outputFile.getParent(), textName);
                    FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con éxito");
        } else 
        {
            System.out.println("Trabajo falló");
        }
        System.exit(res);
    }
}